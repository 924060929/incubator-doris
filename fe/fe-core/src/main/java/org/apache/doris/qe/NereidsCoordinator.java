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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.qe.scheduler.protocol.GroupWorkerPipelineThriftProtocol;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** NereidsCoordinator */
public class NereidsCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(NereidsCoordinator.class);

    private NereidsPlanner nereidsPlanner;
    private FragmentIdMapping<DistributedPlan> distributedPlans;
    private GroupWorkerPipelineThriftProtocol workersClient;

    public NereidsCoordinator(ConnectContext context, Analyzer analyzer,
            Planner planner, StatsErrorEstimator statsErrorEstimator, NereidsPlanner nereidsPlanner) {
        super(context, analyzer, planner, statsErrorEstimator);
        this.nereidsPlanner = Objects.requireNonNull(nereidsPlanner, "nereidsPlanner can not be null");
        this.distributedPlans = Objects.requireNonNull(
                nereidsPlanner.getDistributedPlans(), "distributedPlans can not be null"
        );
    }

    @Override
    protected void execInternal() throws Exception {
        workersClient = new GroupWorkerPipelineThriftProtocol(nereidsPlanner);
        ExecContext execContext = workersClient.getExecContext();

        List<DistributedPlan> distributedPlans = this.distributedPlans.valueList();
        PipelineDistributedPlan topFragment = (PipelineDistributedPlan) distributedPlans.get(
                distributedPlans.size() - 1);

        Boolean enableParallelResultSink = execContext.queryOptions.isEnableParallelResultSink()
                && topFragment.getFragmentJob().getFragment().getSink() instanceof ResultSink;

        AssignedJob topInstance = topFragment.getInstanceJobs().get(0);

        DistributedPlanWorker topWorker = topInstance.getAssignedWorker();
        TNetworkAddress execBeAddr = new TNetworkAddress(topWorker.host(), topWorker.brpcPort());
        this.timeoutDeadline = System.currentTimeMillis() + execContext.queryOptions.getExecutionTimeout() * 1000L;

        receivers.add(new ResultReceiver(queryId, topInstance.instanceId(), topWorker.id(),
                execBeAddr, this.timeoutDeadline,
                nereidsPlanner.getCascadesContext().getConnectContext()
                        .getSessionVariable().getMaxMsgSizeOfResultReceiver(), enableParallelResultSink));

        sendPipelineCtx();
        // super.execInternal();
    }

    @Override
    protected void sendPipelineCtx() throws TException, RpcException, UserException {
        // Init the mark done in order to track the finished state of the query
        fragmentsDoneLatch = new MarkedCountDownLatch<>(distributedPlans.size());

        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentParams
                = workersClient.getFragmentParams();

        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv : workerToFragmentParams.entrySet()) {
            DistributedPlanWorker worker = kv.getKey();
            for (TPipelineFragmentParams fragments : kv.getValue().getParamsList()) {
                fragmentsDoneLatch.addMark(fragments.getFragmentId(), worker.id());
            }
        }

        // if (topDataSink instanceof ResultSink || topDataSink instanceof ResultFileSink) {
        //     Boolean enableParallelResultSink = queryOptions.isEnableParallelResultSink()
        //             && topDataSink instanceof ResultSink;
        // Set<TNetworkAddress> addrs = new HashSet<>();
        // for (FInstanceExecParam param : topParams.instanceExecParams) {
        //     if (addrs.contains(param.host)) {
        //         continue;
        //     }
        //     addrs.add(param.host);
        //     receivers.add(new ResultReceiver(queryId, param.instanceId, addressToBackendID.get(param.host),
        //             toBrpcHost(param.host), this.timeoutDeadline,
        //             context.getSessionVariable().getMaxMsgSizeOfResultReceiver(), enableParallelResultSink));
        // }

        List<PipelineExecContexts> contexts = workersClient.serialize(getExecutionProfile());
        for (PipelineExecContexts context : contexts) {
            for (PipelineExecContext ctx : context.getCtxs()) {
                pipelineExecContexts.put(Pair.of(ctx.fragmentId.asInt(), ctx.backend.getId()), ctx);
            }
        }

        workersClient.send();

        // serializeFragments() can be called in parallel.
        // final AtomicLong compressedSize = new AtomicLong(0);
        // beToPipelineExecCtxs.values().parallelStream().forEach(ctxs -> {
        //     try {
        //         compressedSize.addAndGet(ctxs.serializeFragments());
        //     } catch (TException e) {
        //         throw new RuntimeException(e);
        //     }
        // });
        //
        // updateProfileIfPresent(profile -> profile.updateFragmentCompressedSize(compressedSize.get()));
        // updateProfileIfPresent(profile -> profile.setFragmentSerializeTime());

        // 4.2 send fragments rpc
        // List<Triple<PipelineExecContexts, BackendServiceProxy, Future<PExecPlanFragmentResult>>>
        //         futures = Lists.newArrayList();
        // BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        // for (PipelineExecContexts ctxs : beToPipelineExecCtxs.values()) {
        //     futures.add(ImmutableTriple.of(ctxs, proxy, ctxs.execRemoteFragmentsAsync(proxy)));
        // }
        // waitPipelineRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send fragments");
        //
        // updateProfileIfPresent(profile -> profile.updateFragmentRpcCount(futures.size()));
        // updateProfileIfPresent(profile -> profile.setFragmentSendPhase1Time());
        //
        // if (true) {
        //     // 5. send and wait execution start rpc
        //     futures.clear();
        //     for (PipelineExecContexts ctxs : beToPipelineExecCtxs.values()) {
        //         futures.add(ImmutableTriple.of(ctxs, proxy, ctxs.execPlanFragmentStartAsync(proxy)));
        //     }
        //     waitPipelineRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send execution start");
        //     updateProfileIfPresent(profile -> profile.updateFragmentRpcCount(futures.size()));
        //     updateProfileIfPresent(profile -> profile.setFragmentSendPhase2Time());
        // }
    }

    @Override
    protected void processFragmentAssignmentAndParams() throws Exception {
        // prepare information
        prepare();

        computeFragmentExecParams();
    }

    @Override
    protected void computeFragmentHosts() {
        // translate distributed plan to params
        for (DistributedPlan distributedPlan : distributedPlans.values()) {
            UnassignedJob fragmentJob = distributedPlan.getFragmentJob();
            PlanFragment fragment = fragmentJob.getFragment();

            bucketShuffleJoinController
                    .isBucketShuffleJoin(fragment.getFragmentId().asInt(), fragment.getPlanRoot());

            setFileScanParams(distributedPlan);

            FragmentExecParams fragmentExecParams = fragmentExecParamsMap.computeIfAbsent(
                    fragment.getFragmentId(), id -> new FragmentExecParams(fragment)
            );
            List<AssignedJob> instanceJobs = ((PipelineDistributedPlan) distributedPlan).getInstanceJobs();
            boolean useLocalShuffle = useLocalShuffle(distributedPlan);
            if (useLocalShuffle) {
                fragmentExecParams.ignoreDataDistribution = true;
                fragmentExecParams.parallelTasksNum = 1;
            } else {
                fragmentExecParams.parallelTasksNum = instanceJobs.size();
            }

            for (AssignedJob instanceJob : instanceJobs) {
                DistributedPlanWorker worker = instanceJob.getAssignedWorker();
                TNetworkAddress address = new TNetworkAddress(worker.host(), worker.port());
                FInstanceExecParam instanceExecParam = new FInstanceExecParam(
                        null, address, 0, fragmentExecParams);
                instanceExecParam.instanceId = instanceJob.instanceId();
                fragmentExecParams.instanceExecParams.add(instanceExecParam);
                addressToBackendID.put(address, worker.id());
                ScanSource scanSource = instanceJob.getScanSource();
                if (scanSource instanceof BucketScanSource) {
                    setForBucketScanSource(instanceExecParam, (BucketScanSource) scanSource, useLocalShuffle);
                } else {
                    setForDefaultScanSource(instanceExecParam, (DefaultScanSource) scanSource, useLocalShuffle);
                }
            }
        }
    }

    private void setFileScanParams(DistributedPlan distributedPlan) {
        for (ScanNode scanNode : distributedPlan.getFragmentJob().getScanNodes()) {
            if (scanNode instanceof FileQueryScanNode) {
                fileScanRangeParamsMap.put(
                        scanNode.getId().asInt(),
                        ((FileQueryScanNode) scanNode).getFileScanRangeParams()
                );
            }
        }
    }

    private boolean useLocalShuffle(DistributedPlan distributedPlan) {
        List<AssignedJob> instanceJobs = ((PipelineDistributedPlan) distributedPlan).getInstanceJobs();
        for (AssignedJob instanceJob : instanceJobs) {
            if (instanceJob instanceof LocalShuffleAssignedJob) {
                return true;
            }
        }
        return false;
    }

    private void setForDefaultScanSource(
            FInstanceExecParam instanceExecParam, DefaultScanSource scanSource, boolean isShareScan) {
        for (Entry<ScanNode, ScanRanges> scanNodeIdToReplicaIds : scanSource.scanNodeToScanRanges.entrySet()) {
            ScanNode scanNode = scanNodeIdToReplicaIds.getKey();
            ScanRanges scanReplicas = scanNodeIdToReplicaIds.getValue();
            instanceExecParam.perNodeScanRanges.put(scanNode.getId().asInt(), scanReplicas.params);
            instanceExecParam.perNodeSharedScans.put(scanNode.getId().asInt(), isShareScan);
        }
    }

    private void setForBucketScanSource(FInstanceExecParam instanceExecParam,
            BucketScanSource bucketScanSource, boolean isShareScan) {
        for (Entry<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanTablets :
                bucketScanSource.bucketIndexToScanNodeToTablets.entrySet()) {
            Integer bucketIndex = bucketIndexToScanTablets.getKey();
            instanceExecParam.addBucketSeq(bucketIndex);
            Map<ScanNode, ScanRanges> scanNodeToRangeMap = bucketIndexToScanTablets.getValue();
            for (Entry<ScanNode, ScanRanges> scanNodeToRange : scanNodeToRangeMap.entrySet()) {
                ScanNode scanNode = scanNodeToRange.getKey();
                ScanRanges scanRanges = scanNodeToRange.getValue();
                List<TScanRangeParams> scanBucketTablets = instanceExecParam.perNodeScanRanges.computeIfAbsent(
                        scanNode.getId().asInt(), id -> Lists.newArrayList());
                scanBucketTablets.addAll(scanRanges.params);
                instanceExecParam.perNodeSharedScans.put(scanNode.getId().asInt(), isShareScan);

                if (scanNode instanceof OlapScanNode) {
                    OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                    if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
                        int bucketNum = olapScanNode.getBucketNum();
                        fragmentIdToSeqToAddressMap.put(olapScanNode.getFragmentId(), new HashMap<>());
                        bucketShuffleJoinController.fragmentIdBucketSeqToScanRangeMap
                                .put(scanNode.getFragmentId(), new BucketSeqToScanRange());
                        bucketShuffleJoinController.fragmentIdToBucketNumMap
                                .put(scanNode.getFragmentId(), bucketNum);
                        olapScanNode.getFragment().setBucketNum(bucketNum);
                    }
                } else if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
                    int bucketNum = 1;
                    fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashMap<>());
                    bucketShuffleJoinController.fragmentIdBucketSeqToScanRangeMap
                            .put(scanNode.getFragmentId(), new BucketSeqToScanRange());
                    bucketShuffleJoinController.fragmentIdToBucketNumMap
                            .put(scanNode.getFragmentId(), bucketNum);
                    scanNode.getFragment().setBucketNum(bucketNum);
                }

                BucketSeqToScanRange bucketSeqToScanRange = bucketShuffleJoinController
                        .fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

                Map<Integer, List<TScanRangeParams>> scanNodeIdToReplicas
                        = bucketSeqToScanRange.computeIfAbsent(bucketIndex, set -> Maps.newLinkedHashMap());
                List<TScanRangeParams> tablets = scanNodeIdToReplicas.computeIfAbsent(
                        scanNode.getId().asInt(), id -> new ArrayList<>());
                tablets.addAll(scanRanges.params);
            }
        }
    }

    protected void waitPipelineRpc(TQueryOptions queryOptions,
            List<Triple<PipelineExecContexts, BackendServiceProxy, Future<PExecPlanFragmentResult>>> futures,
            long leftTimeMs, String operation) throws RpcException, UserException {
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
        for (Triple<PipelineExecContexts, BackendServiceProxy, Future<PExecPlanFragmentResult>> triple : futures) {
            TStatusCode code;
            String errMsg = null;
            Exception exception = null;

            try {
                PExecPlanFragmentResult result = triple.getRight().get(timeoutMs, TimeUnit.MILLISECONDS);
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code == null) {
                    code = TStatusCode.INTERNAL_ERROR;
                }

                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    } else {
                        errMsg = operation + " failed. backend id: " + triple.getLeft().beId;
                    }
                }
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
                triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
                triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
            } catch (TimeoutException e) {
                exception = e;
                errMsg = String.format(
                        "timeout when waiting for %s rpc, query timeout:%d, left timeout for this operation:%d",
                        operation, queryOptions.getExecutionTimeout(), timeoutMs / 1000);
                LOG.warn("Query {} {}", DebugUtil.printId(queryId), errMsg);
                code = TStatusCode.TIMEOUT;
                triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
            }

            if (code != TStatusCode.OK) {
                if (exception != null && errMsg == null) {
                    errMsg = operation + " failed. " + exception.getMessage();
                }
                queryStatus.updateStatus(TStatusCode.INTERNAL_ERROR, errMsg);
                cancelInternal(queryStatus);
                switch (code) {
                    case TIMEOUT:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
                                .increase(1L);
                        throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
                    case THRIFT_RPC_ERROR:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
                                .increase(1L);
                        SimpleScheduler.addToBlacklist(triple.getLeft().beId, errMsg);
                        throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
                    default:
                        throw new UserException(errMsg, exception);
                }
            }
        }
    }
}
