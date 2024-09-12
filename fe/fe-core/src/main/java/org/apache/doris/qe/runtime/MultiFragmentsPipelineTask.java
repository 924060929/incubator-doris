package org.apache.doris.qe.runtime;

import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.proto.InternalService.PExecPlanFragmentStartRequest;
import org.apache.doris.proto.Types;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MultiFragmentsPipelineTask extends AbstractRuntimeTask<Integer, SingleFragmentPipelineTask> {
    private static final Logger LOG = LogManager.getLogger(SqlExecutionPipelineTask.class);

    // immutable parameters
    private final TUniqueId queryId;
    private final Backend backend;
    private final BackendServiceProxy backendClientProxy = BackendServiceProxy.getInstance();

    // mutable states

    // we will set fragmentsParams and serializeFragments to null after send rpc, to save memory
    private TPipelineFragmentParamsList fragmentParamsList;
    private ByteString serializeFragments;
    private final AtomicBoolean hasCancelled = new AtomicBoolean();
    private final AtomicBoolean cancelInProcess = new AtomicBoolean();

    public MultiFragmentsPipelineTask(
            TUniqueId queryId, Backend backend, TPipelineFragmentParamsList fragmentsParams,
            ByteString serializeFragments, Map<Integer, SingleFragmentPipelineTask> fragmentTasks) {
        super(new ChildrenRuntimeTasks<>(fragmentTasks));
        this.queryId = Objects.requireNonNull(queryId, "queryId can not be null");
        this.backend = Objects.requireNonNull(backend, "backend can not be null");
        this.fragmentParamsList = Objects.requireNonNull(fragmentsParams, "fragmentParamsList can not be null");
        this.serializeFragments = Objects.requireNonNull(
                serializeFragments, "serializeFragments can not be null"
        );
    }

    public Future<PExecPlanFragmentResult> sendPhaseOneRpc(boolean twoPhaseExecution) {
        return execRemoteFragmentsAsync(
                backendClientProxy, serializeFragments, backend.getBrpcAddress(), twoPhaseExecution
        );
    }

    public Future<PExecPlanFragmentResult> sendPhaseTwoRpc() {
        return execPlanFragmentStartAsync(backendClientProxy, backend.getBrpcAddress());
    }

    @Override
    public String toString() {
        TNetworkAddress brpcAddress = backend.getBrpcAddress();
        return "MultiFragmentsPipelineTask(Backend " + backend.getId()
                + "(" + brpcAddress.getHostname() + ":" + brpcAddress.getPort() + "): ["
                + childrenTasks.allTasks()
                    .stream()
                    .map(singleFragment -> "F" + singleFragment.getFragmentId())
                    .collect(Collectors.joining(", ")) + "])";
    }

    public synchronized void cancelExecute(Status cancelReason) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("cancelRemoteFragments backend: {}, query={}, reason: {}",
                    backend, DebugUtil.printId(queryId), cancelReason.toString());
        }

        if (this.hasCancelled.get() || this.cancelInProcess.get()) {
            LOG.info("Frangment has already been cancelled. Query {} backend: {}",
                    DebugUtil.printId(queryId), backend);
            return;
        }
        try {
            TNetworkAddress brpcAddress = backend.getBrpcAddress();
            try {
                ListenableFuture<PCancelPlanFragmentResult> cancelResult =
                        BackendServiceProxy.getInstance().cancelPipelineXPlanFragmentAsync(
                                brpcAddress, queryId, cancelReason);
                Futures.addCallback(cancelResult, new FutureCallback<PCancelPlanFragmentResult>() {
                    public void onSuccess(InternalService.PCancelPlanFragmentResult result) {
                        cancelInProcess.set(false);
                        if (result.hasStatus()) {
                            Status status = new Status(result.getStatus());
                            if (status.getErrorCode() == TStatusCode.OK) {
                                hasCancelled.set(true);
                            } else {
                                LOG.warn("Failed to cancel query {} backend: {}, reason: {}",
                                        DebugUtil.printId(queryId), backend, status.toString());
                            }
                        }
                        LOG.warn("Failed to cancel query {} backend: {} reason: {}",
                                DebugUtil.printId(queryId), backend, "without status");
                    }

                    public void onFailure(Throwable t) {
                        cancelInProcess.set(false);;
                        LOG.warn("Failed to cancel query {} backend: {}, reason: {}",
                                DebugUtil.printId(queryId), backend,  cancelReason.toString(), t);
                    }
                }, Coordinator.backendRpcCallbackExecutor);
                cancelInProcess.set(true);
            } catch (RpcException e) {
                LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                        brpcAddress.getPort());
                SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("catch a exception", e);
            return;
        }
    }

    public Backend getBackend() {
        return backend;
    }

    private Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentsAsync(
            BackendServiceProxy proxy, ByteString serializedFragments, TNetworkAddress brpcAddr,
            boolean twoPhaseExecution) {
        Preconditions.checkNotNull(serializedFragments);
        try {
            return proxy.execPlanFragmentsAsync(brpcAddr, serializedFragments, twoPhaseExecution);
        } catch (RpcException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return futureWithException(e);
        } finally {
            // save memory
            this.fragmentParamsList = null;
            this.serializeFragments = null;
        }
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync(
            BackendServiceProxy proxy, TNetworkAddress brpcAddr) {
        try {
            PExecPlanFragmentStartRequest.Builder builder = PExecPlanFragmentStartRequest.newBuilder();
            PUniqueId qid = PUniqueId.newBuilder().setHi(queryId.hi).setLo(queryId.lo).build();
            builder.setQueryId(qid);
            return proxy.execPlanFragmentStartAsync(brpcAddr, builder.build());
        } catch (RpcException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return futureWithException(e);
        }
    }

    private Future<PExecPlanFragmentResult> futureWithException(RpcException e) {
        return new Future<PExecPlanFragmentResult>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public PExecPlanFragmentResult get() {
                PExecPlanFragmentResult result = PExecPlanFragmentResult.newBuilder().setStatus(
                        Types.PStatus.newBuilder().addErrorMsgs(e.getMessage())
                                .setStatusCode(TStatusCode.THRIFT_RPC_ERROR.getValue()).build()).build();
                return result;
            }

            @Override
            public PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                return get();
            }
        };
    }
}
