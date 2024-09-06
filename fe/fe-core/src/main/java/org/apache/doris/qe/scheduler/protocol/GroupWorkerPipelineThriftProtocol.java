// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe.scheduler.protocol;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendDistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.proto.InternalService.PExecPlanFragmentStartRequest;
import org.apache.doris.proto.Types;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.Coordinator.PipelineExecContext;
import org.apache.doris.qe.Coordinator.PipelineExecContexts;
import org.apache.doris.qe.ExecContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TPlanFragmentDestination;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** GroupWorkerPipelineThriftProtocol */
public class GroupWorkerPipelineThriftProtocol implements WorkerProtocol {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    private final ExecContext execContext;
    private final BackendServiceProxy backendClientProxy = BackendServiceProxy.getInstance();
    private BackendDistributedPlanWorkerManager workerManager;

    public GroupWorkerPipelineThriftProtocol(NereidsPlanner planner) {
        this.workerManager = new BackendDistributedPlanWorkerManager();
        this.execContext = toThrift(planner);
    }

    public Map<DistributedPlanWorker, TPipelineFragmentParamsList> getFragmentParams() {
        return execContext.workerToFragmentsParam;
    }

    public ExecContext getExecContext() {
        return execContext;
    }

    @Override
    public List<PipelineExecContexts> serialize(ExecutionProfile executionProfile) {
        execContext.serializedRpcData = execContext.workerToFragmentsParam.entrySet()
                .parallelStream()
                .map(kv -> {
                    ByteString serializedString = null;
                    try {
                        // zero copy
                        serializedString = UnsafeByteOperations.unsafeWrap(
                                new TSerializer(new Factory()).serialize(kv.getValue())
                        );
                    } catch (Throwable t) {
                        throw new IllegalStateException(t.getMessage(), t);
                    }
                    return Pair.of(kv.getKey(), serializedString);
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));

        List<PipelineExecContexts> pipelineExecContextsList = Lists.newArrayList();
        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv
                : execContext.workerToFragmentsParam.entrySet()) {
            BackendWorker worker = (BackendWorker) kv.getKey();
            TPipelineFragmentParamsList fragments = kv.getValue();
            Backend backend = worker.getBackend();

            List<PipelineExecContext> contexts = Lists.newArrayList();
            for (TPipelineFragmentParams fragmentParams : fragments.getParamsList()) {
                PipelineExecContext pipelineExecContext = new PipelineExecContext(
                        new PlanFragmentId(fragmentParams.getFragmentId()),
                        fragmentParams, backend, executionProfile, 0);
                // pipelineExecContext.unsetFields();
                contexts.add(pipelineExecContext);
            }
            TNetworkAddress brpcAddress = backend.getBrpcAddress();
            PipelineExecContexts pipelineExecContexts = new PipelineExecContexts(
                    execContext.queryId, backend, brpcAddress, false,
                    fragments.getParamsList().get(0).getFragmentNumOnHost()
            );
            for (PipelineExecContext context : contexts) {
                pipelineExecContexts.addContext(context);
            }
            pipelineExecContextsList.add(pipelineExecContexts);
        }
        return pipelineExecContextsList;
    }

    @Override
    public void send() {
        sendAndWaitPhaseOneRpc();

        // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
        // else use exec_plan_fragments directly.
        // we choose #fragments > 1 because in some cases
        // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
        // For example: select * from numbers("number"="10") will generate ExchangeNode and
        // TableValuedFunctionScanNode, we should ensure TableValuedFunctionScanNode does not
        // send data until ExchangeNode is ready to receive.
        if (execContext.twoPhaseExecution) {
            sendAndWaitPhaseTwoRpc();
        }
    }

    @Override
    public void cancel(Status cancelReason) {
        // backendClientProxy.cancelPlanFragmentAsync();
    }

    private void sendAndWaitPhaseOneRpc() {
        boolean twoPhaseExecution = execContext.twoPhaseExecution;
        List<Future<PExecPlanFragmentResult>> futures = Lists.newArrayList();
        try {
            for (Entry<DistributedPlanWorker, ByteString> kv : execContext.serializedRpcData.entrySet()) {
                BackendWorker backendWorker = (BackendWorker) kv.getKey();
                TNetworkAddress brpcAddress = backendWorker.getBackend().getBrpcAddress();
                Future<PExecPlanFragmentResult> future = execRemoteFragmentsAsync(
                        backendClientProxy, kv.getValue(), brpcAddress, twoPhaseExecution);
                futures.add(future);
            }
        } catch (Throwable t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
        waitPipelineRpc(futures, 10000, "abc");
    }

    private void sendAndWaitPhaseTwoRpc() {
        List<Future<PExecPlanFragmentResult>> futures = Lists.newArrayList();
        try {
            for (Entry<DistributedPlanWorker, ByteString> kv : execContext.serializedRpcData.entrySet()) {
                BackendWorker backendWorker = (BackendWorker) kv.getKey();
                TNetworkAddress brpcAddress = backendWorker.getBackend().getBrpcAddress();
                Future<PExecPlanFragmentResult> future = execPlanFragmentStartAsync(
                        backendClientProxy, brpcAddress);
                futures.add(future);
            }
            waitPipelineRpc(futures, 10000, "abc2");
        } catch (Throwable t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
    }

    protected void waitPipelineRpc(
            List<Future<PExecPlanFragmentResult>> futures, long leftTimeMs, String operation) {

        for (Future<PExecPlanFragmentResult> future : futures) {
            try {
                future.get();
            } catch (Throwable t) {
                throw new IllegalStateException(t.getMessage(), t);
            }
        }
        // if (leftTimeMs <= 0) {
        //     long currentTimeMillis = System.currentTimeMillis();
        //     long elapsed = (currentTimeMillis - timeoutDeadline) / 1000 + queryOptions.getExecutionTimeout();
        //     String msg = String.format(
        //             "timeout before waiting %s rpc, query timeout:%d, already elapsed:%d, left for this:%d",
        //             operation, queryOptions.getExecutionTimeout(), elapsed, leftTimeMs);
        //     LOG.warn("Query {} {}", DebugUtil.printId(queryId), msg);
        //     if (!queryOptions.isSetExecutionTimeout() || !queryOptions.isSetQueryTimeout()) {
        //         LOG.warn("Query {} does not set timeout info, execution timeout: is_set:{}, value:{}"
        //                         + ", query timeout: is_set:{}, value: {}, "
        //                         + "coordinator timeout deadline {}, cur time millis: {}",
        //                 DebugUtil.printId(queryId),
        //                 queryOptions.isSetExecutionTimeout(), queryOptions.getExecutionTimeout(),
        //                 queryOptions.isSetQueryTimeout(), queryOptions.getQueryTimeout(),
        //                 timeoutDeadline, currentTimeMillis);
        //     }
        //     throw new UserException(msg);
        // }
        //
        // long timeoutMs = Math.min(leftTimeMs, Config.remote_fragment_exec_timeout_ms);
        // for (Future<PExecPlanFragmentResult> future : futures) {
        //     TStatusCode code;
        //     String errMsg = null;
        //     Exception exception = null;
        //
        //     try {
        //         PExecPlanFragmentResult result = future.get(timeoutMs, TimeUnit.MILLISECONDS);
        //         code = TStatusCode.findByValue(result.getStatus().getStatusCode());
        //         if (code == null) {
        //             code = TStatusCode.INTERNAL_ERROR;
        //         }
        //
        //         if (code != TStatusCode.OK) {
        //             if (!result.getStatus().getErrorMsgsList().isEmpty()) {
        //                 errMsg = result.getStatus().getErrorMsgsList().get(0);
        //             } else {
        //                 // errMsg = operation + " failed. backend id: " + triple.getLeft().beId;
        //             }
        //         }
        //     } catch (ExecutionException e) {
        //         exception = e;
        //         code = TStatusCode.THRIFT_RPC_ERROR;
        //         // triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
        //     } catch (InterruptedException e) {
        //         exception = e;
        //         code = TStatusCode.INTERNAL_ERROR;
        //         // triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
        //     } catch (TimeoutException e) {
        //         exception = e;
        //         // errMsg = String.format(
        //         //         "timeout when waiting for %s rpc, query timeout:%d, left timeout for this operation:%d",
        //         //         operation, queryOptions.getExecutionTimeout(), timeoutMs / 1000);
        //         // LOG.warn("Query {} {}", DebugUtil.printId(queryId), errMsg);
        //         code = TStatusCode.TIMEOUT;
        //         // triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
        //     }
        //
        //     if (code != TStatusCode.OK) {
        //         if (exception != null && errMsg == null) {
        //             errMsg = operation + " failed. " + exception.getMessage();
        //         }
        //         queryStatus.updateStatus(TStatusCode.INTERNAL_ERROR, errMsg);
        //         cancelInternal(queryStatus);
        //         switch (code) {
        //             case TIMEOUT:
        //                 MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
        //                         .increase(1L);
        //                 throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
        //             case THRIFT_RPC_ERROR:
        //                 MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
        //                         .increase(1L);
        //                 SimpleScheduler.addToBlacklist(triple.getLeft().beId, errMsg);
        //                 throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
        //             default:
        //                 throw new UserException(errMsg, exception);
        //         }
        //     }
        // }
    }

    public Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentsAsync(
            BackendServiceProxy proxy, ByteString serializedFragments, TNetworkAddress brpcAddr,
            boolean twoPhaseExecution)
            throws TException {
        Preconditions.checkNotNull(serializedFragments);
        try {
            return proxy.execPlanFragmentsAsync(brpcAddr, serializedFragments, twoPhaseExecution);
        } catch (RpcException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return futureWithException(e);
        }
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync(
            BackendServiceProxy proxy, TNetworkAddress brpcAddr) throws TException {
        TUniqueId queryId = execContext.queryId;
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

    private ExecContext toThrift(NereidsPlanner planner) {
        ConnectContext connectContext = planner.getCascadesContext().getConnectContext();
        TQueryOptions queryOptions = initQueryOptions(connectContext);
        TQueryGlobals queryGlobals = initQueryGlobals(connectContext);
        TDescriptorTable descriptorTable = planner.getDescTable().toThrift();
        List<TPipelineWorkloadGroup> workloadGroup = computeWorkloadGroups(connectContext);
        TNetworkAddress coordinatorAddress = new TNetworkAddress(Coordinator.localIP, Config.rpc_port);
        String currentConnectedFEIp = connectContext.getCurrentConnectedFEIp();
        TNetworkAddress directConnectFrontendAddress =
                connectContext.isProxy() && !StringUtils.isBlank(currentConnectedFEIp)
                                ? new TNetworkAddress(currentConnectedFEIp, Config.rpc_port)
                                : coordinatorAddress;

        ExecContext execContext = new ExecContext(
                connectContext, planner, queryGlobals, queryOptions, descriptorTable, workloadGroup,
                coordinatorAddress, directConnectFrontendAddress
        );

        List<PipelineDistributedPlan> distributedPlans = planner.getDistributedPlans().valueList();
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam
                = plansToThrift(distributedPlans, execContext);
        execContext.workerToFragmentsParam = workerToFragmentsParam;
        return execContext;
    }

    private List<TPipelineWorkloadGroup> computeWorkloadGroups(ConnectContext connectContext) {
        List<TPipelineWorkloadGroup> workloadGroup = ImmutableList.of();
        if (Config.enable_workload_group) {
            try {
                workloadGroup = connectContext.getEnv().getWorkloadGroupMgr().getWorkloadGroup(connectContext);
            } catch (UserException e) {
                throw new NereidsException(e.getMessage(), e);
            }
        }
        return workloadGroup;
    }

    private TQueryOptions initQueryOptions(ConnectContext context) {
        TQueryOptions queryOptions = context.getSessionVariable().toThrift();
        queryOptions.setBeExecVersion(Config.be_exec_version);
        queryOptions.setQueryTimeout(context.getExecTimeout());
        queryOptions.setExecutionTimeout(context.getExecTimeout());
        if (queryOptions.getExecutionTimeout() < 1) {
            LOG.info("try set timeout less than 1", new RuntimeException(""));
        }
        queryOptions.setEnableScanNodeRunSerial(context.getSessionVariable().isEnableScanRunSerial());
        queryOptions.setFeProcessUuid(ExecuteEnv.getInstance().getProcessUUID());
        queryOptions.setWaitFullBlockScheduleTimes(context.getSessionVariable().getWaitFullBlockScheduleTimes());
        queryOptions.setMysqlRowBinaryFormat(context.getCommand() == MysqlCommand.COM_STMT_EXECUTE);

        setOptionsFromUserProperty(context, queryOptions);
        return queryOptions;
    }

    private TQueryGlobals initQueryGlobals(ConnectContext context) {
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.now()));
        queryGlobals.setTimestampMs(System.currentTimeMillis());
        queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());
        queryGlobals.setLoadZeroTolerance(false);
        if (context.getSessionVariable().getTimeZone().equals("CST")) {
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
        }
        return queryGlobals;
    }

    private void setOptionsFromUserProperty(ConnectContext connectContext, TQueryOptions queryOptions) {
        String qualifiedUser = connectContext.getQualifiedUser();
        // set cpu resource limit
        int cpuLimit = Env.getCurrentEnv().getAuth().getCpuResourceLimit(qualifiedUser);
        if (cpuLimit > 0) {
            // overwrite the cpu resource limit from session variable;
            TResourceLimit resourceLimit = new TResourceLimit();
            resourceLimit.setCpuLimit(cpuLimit);
            queryOptions.setResourceLimit(resourceLimit);
        }
        // set exec mem limit
        long maxExecMemByte = connectContext.getSessionVariable().getMaxExecMemByte();
        long memLimit = maxExecMemByte > 0 ? maxExecMemByte :
                Env.getCurrentEnv().getAuth().getExecMemLimit(qualifiedUser);
        if (memLimit > 0) {
            // overwrite the exec_mem_limit from session variable;
            queryOptions.setMemLimit(memLimit);
            queryOptions.setMaxReservation(memLimit);
            queryOptions.setInitialReservationTotalClaims(memLimit);
            queryOptions.setBufferPoolLimit(memLimit);
        }
    }

    private Map<DistributedPlanWorker, TPipelineFragmentParamsList> plansToThrift(
            List<PipelineDistributedPlan> distributedPlans, ExecContext execContext) {
        Multiset<DistributedPlanWorker> workerProcessInstanceNum = computeInstanceNumPerWorker(distributedPlans);
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> fragmentsGroupByWorker = Maps.newLinkedHashMap();
        int currentInstanceIndex = 0;
        for (PipelineDistributedPlan currentFragmentPlan : distributedPlans) {
            TPlanFragment currentFragmentThrift = currentFragmentPlan.getFragmentJob().getFragment().toThrift();
            Map<Integer, TFileScanRangeParams> fileScanRangeParams = computeFileScanRangeParams(currentFragmentPlan);
            Map<Integer, Integer> exchangeSenderNum = computeExchangeSenderNum(currentFragmentPlan);
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToCurrentFragment = Maps.newLinkedHashMap();

            List<TPlanFragmentDestination> destinations = destinationToThrift(currentFragmentPlan);

            for (int recvrId = 0; recvrId < currentFragmentPlan.getInstanceJobs().size(); recvrId++) {
                AssignedJob instanceJob = currentFragmentPlan.getInstanceJobs().get(recvrId);
                // Suggestion: Do not modify currentFragmentParam out of the `fragmentToThriftIfAbsent` method,
                //             except add instanceParam into local_params
                TPipelineFragmentParams currentFragmentParam = fragmentToThriftIfAbsent(
                        currentFragmentPlan, instanceJob, workerToCurrentFragment,
                        exchangeSenderNum, currentFragmentThrift, fileScanRangeParams,
                        workerProcessInstanceNum, destinations, execContext);
                TPipelineInstanceParams instanceParam
                        = instanceToThrift(currentFragmentPlan, instanceJob, currentInstanceIndex++);
                List<TPipelineInstanceParams> instancesParams = currentFragmentParam.getLocalParams();
                currentFragmentParam.getShuffleIdxToInstanceIdx().put(recvrId, instancesParams.size());
                currentFragmentParam.getPerNodeSharedScans().putAll(instanceParam.getPerNodeSharedScans());
                currentFragmentParam.setNumBuckets(0);

                instancesParams.add(instanceParam);
            }

            // arrange fragments by the same worker,
            // so we can merge and send multiple fragment to a backend use one rpc
            for (Entry<DistributedPlanWorker, TPipelineFragmentParams> kv : workerToCurrentFragment.entrySet()) {
                TPipelineFragmentParamsList fragments = fragmentsGroupByWorker.computeIfAbsent(
                        kv.getKey(), w -> new TPipelineFragmentParamsList());
                fragments.addToParamsList(kv.getValue());
            }
        }

        // we should init fragment from target to source in backend
        for (DistributedPlanWorker worker : fragmentsGroupByWorker.keySet()) {
            Collections.reverse(fragmentsGroupByWorker.get(worker).getParamsList());
        }

        // remove redundant params to reduce rpc message size
        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv : fragmentsGroupByWorker.entrySet()) {
            boolean isFirstFragmentInCurrentBackend = true;
            for (TPipelineFragmentParams fragmentParams : kv.getValue().getParamsList()) {
                if (!isFirstFragmentInCurrentBackend) {
                    fragmentParams.unsetDescTbl();
                    fragmentParams.unsetFileScanParams();
                    fragmentParams.unsetCoord();
                    fragmentParams.unsetQueryGlobals();
                    fragmentParams.unsetResourceInfo();
                    fragmentParams.setIsSimplifiedParam(true);
                }
                isFirstFragmentInCurrentBackend = false;
            }
        }
        return fragmentsGroupByWorker;
    }

    private Multiset<DistributedPlanWorker> computeInstanceNumPerWorker(
            List<PipelineDistributedPlan> distributedPlans) {
        Multiset<DistributedPlanWorker> workerCounter = LinkedHashMultiset.create();
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            for (AssignedJob instanceJob : distributedPlan.getInstanceJobs()) {
                workerCounter.add(instanceJob.getAssignedWorker());
            }
        }
        return workerCounter;
    }

    private Map<Integer, Integer> computeExchangeSenderNum(PipelineDistributedPlan distributedPlan) {
        Map<Integer, Integer> senderNum = Maps.newLinkedHashMap();
        for (Entry<ExchangeNode, DistributedPlan> kv : distributedPlan.getInputs().entries()) {
            ExchangeNode exchangeNode = kv.getKey();
            PipelineDistributedPlan childPlan = (PipelineDistributedPlan) kv.getValue();
            senderNum.merge(exchangeNode.getId().asInt(), childPlan.getInstanceJobs().size(), Integer::sum);
        }
        return senderNum;
    }

    private List<TPlanFragmentDestination> destinationToThrift(PipelineDistributedPlan plan) {
        List<AssignedJob> destinationJobs = plan.getDestinations();
        List<TPlanFragmentDestination> destinations = Lists.newArrayListWithCapacity(destinationJobs.size());
        for (int receiverId = 0; receiverId < destinationJobs.size(); receiverId++) {
            AssignedJob destinationJob = destinationJobs.get(receiverId);
            DistributedPlanWorker worker = destinationJob.getAssignedWorker();
            String host = worker.host();
            int port = worker.port();
            int brpcPort = worker.brpcPort();

            TPlanFragmentDestination destination = new TPlanFragmentDestination();
            destination.setServer(new TNetworkAddress(host, port));
            destination.setBrpcServer(new TNetworkAddress(host, brpcPort));
            destination.setFragmentInstanceId(destinationJob.instanceId());
            destinations.add(destination);
        }
        return destinations;
    }

    private TPipelineFragmentParams fragmentToThriftIfAbsent(
            PipelineDistributedPlan fragmentPlan, AssignedJob assignedJob,
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToFragmentParams,
            Map<Integer, Integer> exchangeSenderNum, TPlanFragment fragmentThrift,
            Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap,
            Multiset<DistributedPlanWorker> workerProcessInstanceNum,
            List<TPlanFragmentDestination> destinations, ExecContext execContext) {
        return workerToFragmentParams.computeIfAbsent(assignedJob.getAssignedWorker(), worker -> {
            PlanFragment fragment = fragmentPlan.getFragmentJob().getFragment();
            ConnectContext connectContext = execContext.connectContext;

            TPipelineFragmentParams params = new TPipelineFragmentParams();
            params.setIsNereids(true);
            params.setBackendId(worker.id());
            params.setProtocolVersion(PaloInternalServiceVersion.V1);
            params.setDescTbl(execContext.descriptorTable);
            params.setQueryId(execContext.queryId);
            params.setFragmentId(fragment.getFragmentId().asInt());

            // Each tParam will set the total number of Fragments that need to be executed on the same BE,
            // and the BE will determine whether all Fragments have been executed based on this information.
            // Notice. load fragment has a small probability that FragmentNumOnHost is 0, for unknown reasons.
            params.setFragmentNumOnHost(workerProcessInstanceNum.count(worker));

            params.setNeedWaitExecutionTrigger(execContext.twoPhaseExecution);
            params.setPerExchNumSenders(exchangeSenderNum);
            params.setDestinations(destinations);

            int instanceNumInThisFragment = fragmentPlan.getInstanceJobs().size();
            params.setNumSenders(instanceNumInThisFragment);
            params.setTotalInstances(instanceNumInThisFragment);

            params.setCoord(execContext.coordinatorAddress);
            params.setCurrentConnectFe(execContext.directConnectFrontendAddress);
            params.setQueryGlobals(execContext.queryGlobals);
            params.setQueryOptions(new TQueryOptions(execContext.queryOptions));
            long memLimit = execContext.queryOptions.getMemLimit();
            if (!connectContext.getSessionVariable().isDisableColocatePlan() && fragment.hasColocatePlanNode()) {
                int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceNumInThisFragment);
                memLimit = execContext.queryOptions.getMemLimit() / rate;
            }
            params.getQueryOptions().setMemLimit(memLimit);

            params.setSendQueryStatisticsWithEveryBatch(fragment.isTransferQueryStatisticsWithEveryBatch());
            params.setFragment(fragmentThrift);
            params.setLocalParams(Lists.newArrayList());
            params.setWorkloadGroups(execContext.workloadGroups);

            params.setFileScanParams(fileScanRangeParamsMap);
            // params.setNumBuckets(fragment.getBucketNum());
            params.setPerNodeSharedScans(new LinkedHashMap<>());
            // if (ignoreDataDistribution) {
            //     params.setParallelInstances(parallelTasksNum);
            // }
            params.setBucketSeqToInstanceIdx(new LinkedHashMap<>());
            params.setShuffleIdxToInstanceIdx(new LinkedHashMap<>());
            return params;
        });
    }

    private Map<Integer, TFileScanRangeParams> computeFileScanRangeParams(PipelineDistributedPlan distributedPlan) {
        // scan node id -> TFileScanRangeParams
        Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap = Maps.newLinkedHashMap();
        for (ScanNode scanNode : distributedPlan.getFragmentJob().getScanNodes()) {
            if (scanNode instanceof FileQueryScanNode) {
                TFileScanRangeParams fileScanRangeParams = ((FileQueryScanNode) scanNode).getFileScanRangeParams();
                fileScanRangeParamsMap.put(scanNode.getId().asInt(), fileScanRangeParams);
            }
        }

        return fileScanRangeParamsMap;
    }

    private TPipelineInstanceParams instanceToThrift(
            PipelineDistributedPlan distributedPlan, AssignedJob instance, int currentInstanceNum) {
        TPipelineInstanceParams instanceParam = new TPipelineInstanceParams();
        instanceParam.setFragmentInstanceId(instance.instanceId());
        setScanSourceParam(instance.getScanSource(), instanceParam);
        instanceParam.setSenderId(instance.indexInUnassignedJob());
        instanceParam.setBackendNum(currentInstanceNum);
        instanceParam.setRuntimeFilterParams(new TRuntimeFilterParams());
        // instanceParam.runtime_filter_params.setRuntimeFilterMergeAddr(runtimeFilterMergeAddr);
        // topn filter
        return instanceParam;
    }

    private void setScanSourceParam(ScanSource scanSource, TPipelineInstanceParams params) {
        if (scanSource instanceof BucketScanSource) {
            setBucketScanSourceParam((BucketScanSource) scanSource, params);
        } else {
            setDefaultScanSourceParam((DefaultScanSource) scanSource, params);
        }
    }

    private void setDefaultScanSourceParam(DefaultScanSource defaultScanSource, TPipelineInstanceParams params) {
        Map<Integer, List<TScanRangeParams>> scanNodeIdToScanRanges = Maps.newLinkedHashMap();
        Map<Integer, Boolean> perNodeSharedScans = Maps.newLinkedHashMap();
        for (Entry<ScanNode, ScanRanges> kv : defaultScanSource.scanNodeToScanRanges.entrySet()) {
            int scanNodeId = kv.getKey().getId().asInt();
            scanNodeIdToScanRanges.put(scanNodeId, kv.getValue().params);
            // ???
            perNodeSharedScans.put(scanNodeId, false);
        }
        params.setPerNodeScanRanges(scanNodeIdToScanRanges);
        params.setPerNodeSharedScans(perNodeSharedScans);
    }

    private void setBucketScanSourceParam(BucketScanSource bucketScanSource, TPipelineInstanceParams params) {
        Map<Integer, List<TScanRangeParams>> scanNodeIdToScanRanges = Maps.newLinkedHashMap();
        for (Map<ScanNode, ScanRanges> scanNodeToRanges : bucketScanSource.bucketIndexToScanNodeToTablets.values()) {
            for (Entry<ScanNode, ScanRanges> kv2 : scanNodeToRanges.entrySet()) {
                int scanNodeId = kv2.getKey().getId().asInt();
                List<TScanRangeParams> scanRanges = scanNodeIdToScanRanges.computeIfAbsent(scanNodeId, ArrayList::new);
                List<TScanRangeParams> currentScanRanges = kv2.getValue().params;
                scanRanges.addAll(currentScanRanges);
            }
        }
        params.setPerNodeScanRanges(scanNodeIdToScanRanges);
    }

    // record this worker should process which fragment and instances
    private static class FragmentToInstances {
        public final DistributedPlanWorker worker;
        public final Multimap<PipelineDistributedPlan, AssignedJob> fragmentToInstances;

        public FragmentToInstances(DistributedPlanWorker worker) {
            this.worker = worker;
            this.fragmentToInstances = ArrayListMultimap.create();
        }

        public int totalInstances() {
            return fragmentToInstances.values().size();
        }
    }

    @NotNull
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
