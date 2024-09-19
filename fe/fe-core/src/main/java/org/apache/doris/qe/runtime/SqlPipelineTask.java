package org.apache.doris.qe.runtime;

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.ExecContext;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * SqlPipelineTask.
 *
 * This class is used to describe which backend process which fragments
 */
public class SqlPipelineTask extends AbstractRuntimeTask<Long, MultiFragmentsPipelineTask> {
    private static final Logger LOG = LogManager.getLogger(SqlPipelineTask.class);

    // immutable parameters
    private final long timeoutDeadline;
    private final ExecContext execContext;
    private final CoordinatorContext coordinatorContext;

    // mutable states
    public SqlPipelineTask(
            ExecContext execContext,
            CoordinatorContext coordinatorContext,
            Map<Long, MultiFragmentsPipelineTask> fragmentTasks) {
        super(new ChildrenRuntimeTasks<>(fragmentTasks));
        this.execContext = Objects.requireNonNull(execContext, "execContext can not be null");
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.timeoutDeadline = execContext.timeoutDeadline;
    }

    @Override
    public void execute() throws UserException, RpcException {
        sendAndWaitPhaseOneRpc();
        if (execContext.twoPhaseExecution) {
            sendAndWaitPhaseTwoRpc();
        }
    }

    public void cancelSchedule(Status cancelReason) {
        for (MultiFragmentsPipelineTask fragmentsTask : childrenTasks.allTasks()) {
            fragmentsTask.cancelExecute(cancelReason);
        }
    }

    @Override
    public String toString() {
        return "SqlPipelineTask(\n"
                + childrenTasks.allTasks()
                    .stream()
                    .map(multiFragmentsPipelineTask -> "  " + multiFragmentsPipelineTask)
                    .collect(Collectors.joining(",\n"))
                + "\n)";
    }

    private void sendAndWaitPhaseOneRpc() throws UserException, RpcException {
        List<Pair<MultiFragmentsPipelineTask, Future<PExecPlanFragmentResult>>> futures = Lists.newArrayList();
        try {
            for (MultiFragmentsPipelineTask fragmentsTask : childrenTasks.allTasks()) {
                futures.add(Pair.of(fragmentsTask, fragmentsTask.sendPhaseOneRpc(execContext.twoPhaseExecution)));
            }
        } catch (Throwable t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
        waitPipelineRpc(futures, timeoutDeadline - System.currentTimeMillis(), "send fragments");
    }

    private void sendAndWaitPhaseTwoRpc() {
        List<Pair<MultiFragmentsPipelineTask, Future<PExecPlanFragmentResult>>> futures = Lists.newArrayList();
        try {
            for (MultiFragmentsPipelineTask fragmentTask : childrenTasks.allTasks()) {
                futures.add(Pair.of(fragmentTask, fragmentTask.sendPhaseTwoRpc()));
            }
            waitPipelineRpc(futures, timeoutDeadline - System.currentTimeMillis(), "send execution start");
        } catch (Throwable t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
    }

    private void waitPipelineRpc(
            List<Pair<MultiFragmentsPipelineTask, Future<PExecPlanFragmentResult>>> taskAndResults,
            long leftTimeMs, String operation) throws UserException, RpcException {
        TQueryOptions queryOptions = execContext.queryOptions;
        TUniqueId queryId = execContext.queryId;

        if (leftTimeMs <= 0) {
            long currentTimeMillis = System.currentTimeMillis();
            long elapsed = (currentTimeMillis - timeoutDeadline) / 1000 + queryOptions.getExecutionTimeout();
            String msg = String.format(
                    "timeout before waiting %s rpc, query timeout:%d, already elapsed:%d, left for this:%d",
                    operation, queryOptions.getExecutionTimeout(), elapsed, leftTimeMs);
            LOG.warn("Query {} {}", DebugUtil.printId(queryId), msg);
            if (!queryOptions.isSetExecutionTimeout() || !queryOptions.isSetQueryTimeout()) {
                LOG.warn("Query {} does not set timeout info, execution timeout: is_set:{}, value:{}"
                                + ", query timeout: is_set:{}, value: {}, "
                                + "coordinator timeout deadline {}, cur time millis: {}",
                        DebugUtil.printId(queryId),
                        queryOptions.isSetExecutionTimeout(), queryOptions.getExecutionTimeout(),
                        queryOptions.isSetQueryTimeout(), queryOptions.getQueryTimeout(),
                        timeoutDeadline, currentTimeMillis);
            }
            throw new UserException(msg);
        }

        long timeoutMs = Math.min(leftTimeMs, Config.remote_fragment_exec_timeout_ms);
        for (Pair<MultiFragmentsPipelineTask, Future<PExecPlanFragmentResult>> taskAndResult : taskAndResults) {
            TStatusCode code;
            String errMsg = null;
            Exception exception = null;

            try {
                PExecPlanFragmentResult result = taskAndResult.second.get(timeoutMs, TimeUnit.MILLISECONDS);
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code == null) {
                    code = TStatusCode.INTERNAL_ERROR;
                }

                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    }
                }
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
            } catch (TimeoutException e) {
                exception = e;
                code = TStatusCode.TIMEOUT;
            }

            if (code != TStatusCode.OK) {
                MultiFragmentsPipelineTask fragmentTask = taskAndResult.first;
                Backend backend = fragmentTask.getBackend();
                TNetworkAddress brpcAddr = backend.getBrpcAddress();

                if (exception != null) {
                    errMsg = operation + " failed. " + exception.getMessage();
                }

                Status errorStatus = new Status(TStatusCode.INTERNAL_ERROR, errMsg);
                coordinatorContext.updateStatusIfOk(errorStatus);
                cancelSchedule(errorStatus);
                switch (code) {
                    case TIMEOUT:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(brpcAddr.hostname)
                                .increase(1L);
                        throw new RpcException(brpcAddr.hostname, errMsg, exception);
                    case THRIFT_RPC_ERROR:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(brpcAddr.hostname)
                                .increase(1L);
                        SimpleScheduler.addToBlacklist(backend.getId(), errMsg);
                        throw new RpcException(brpcAddr.hostname, errMsg, exception);
                    default:
                        throw new UserException(errMsg, exception);
                }
            }
        }
    }
}
