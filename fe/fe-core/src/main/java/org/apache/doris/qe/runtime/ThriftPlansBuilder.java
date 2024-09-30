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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedScanBucketOlapTableJob;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPlanFragmentDestination;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class ThriftPlansBuilder {
    private static final Logger LOG = LogManager.getLogger(ThriftPlansBuilder.class);

    public static Map<DistributedPlanWorker, TPipelineFragmentParamsList> plansToThrift(
            CoordinatorContext coordinatorContext) {
        List<PipelineDistributedPlan> distributedPlans = coordinatorContext.planner.getDistributedPlans().valueList();
        return plansToThrift(distributedPlans, coordinatorContext);
    }

    private static Map<DistributedPlanWorker, TPipelineFragmentParamsList> plansToThrift(
            List<PipelineDistributedPlan> distributedPlans, CoordinatorContext coordinatorContext) {
        Multiset<DistributedPlanWorker> workerProcessInstanceNum = computeInstanceNumPerWorker(distributedPlans);
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> fragmentsGroupByWorker = Maps.newLinkedHashMap();
        int currentInstanceIndex = 0;
        for (PipelineDistributedPlan currentFragmentPlan : distributedPlans) {
            Map<Integer, TFileScanRangeParams> fileScanRangeParams = computeFileScanRangeParams(currentFragmentPlan);
            Map<Integer, Integer> exchangeSenderNum = computeExchangeSenderNum(currentFragmentPlan);
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToCurrentFragment = Maps.newLinkedHashMap();
            List<TPlanFragmentDestination> destinations = destinationToThrift(currentFragmentPlan);

            for (int instanceNumInCurrentFragment = 0;
                    instanceNumInCurrentFragment < currentFragmentPlan.getInstanceJobs().size();
                    instanceNumInCurrentFragment++) {
                AssignedJob instanceJob = currentFragmentPlan.getInstanceJobs().get(instanceNumInCurrentFragment);
                TPipelineFragmentParams currentFragmentParam = fragmentToThriftIfAbsent(
                        currentFragmentPlan, instanceJob, workerToCurrentFragment,
                        exchangeSenderNum, fileScanRangeParams,
                        workerProcessInstanceNum, destinations, coordinatorContext);

                TPipelineInstanceParams instanceParam = instanceToThrift(
                        currentFragmentParam, instanceJob, currentInstanceIndex++);
                currentFragmentParam.getLocalParams().add(instanceParam);
            }

            // arrange fragments by the same worker,
            // so we can merge and send multiple fragment to a backend use one rpc
            for (Entry<DistributedPlanWorker, TPipelineFragmentParams> kv : workerToCurrentFragment.entrySet()) {
                TPipelineFragmentParamsList fragments = fragmentsGroupByWorker.computeIfAbsent(
                        kv.getKey(), w -> new TPipelineFragmentParamsList());
                fragments.addToParamsList(kv.getValue());
            }
        }

        // backend should initialize fragment from target to source in backend,
        for (DistributedPlanWorker worker : fragmentsGroupByWorker.keySet()) {
            Collections.reverse(fragmentsGroupByWorker.get(worker).getParamsList());
        }

        setParamsForOlapTableSink(distributedPlans, fragmentsGroupByWorker);

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

    private static void setParamsForOlapTableSink(List<PipelineDistributedPlan> distributedPlans,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> fragmentsGroupByWorker) {
        int numBackendsWithSink = 0;
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            PlanFragment fragment = distributedPlan.getFragmentJob().getFragment();
            if (fragment.getSink() instanceof OlapTableSink) {
                numBackendsWithSink += (int) distributedPlan.getInstanceJobs()
                        .stream()
                        .map(AssignedJob::getAssignedWorker)
                        .distinct()
                        .count();
            }
        }

        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv : fragmentsGroupByWorker.entrySet()) {
            TPipelineFragmentParamsList fragments = kv.getValue();
            for (TPipelineFragmentParams fragmentParams : fragments.getParamsList()) {
                if (fragmentParams.getFragment().getOutputSink().getType() == TDataSinkType.OLAP_TABLE_SINK) {
                    int loadStreamPerNode = 1;
                    if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
                        loadStreamPerNode = ConnectContext.get().getSessionVariable().getLoadStreamPerNode();
                    }
                    fragmentParams.setLoadStreamPerNode(loadStreamPerNode);
                    fragmentParams.setTotalLoadStreams(numBackendsWithSink * loadStreamPerNode);
                    fragmentParams.setNumLocalSink(fragmentParams.getLocalParams().size());
                    LOG.info("num local sink for backend {} is {}", fragmentParams.getBackendId(),
                            fragmentParams.getNumLocalSink());
                }
            }
        }
    }

    private static Multiset<DistributedPlanWorker> computeInstanceNumPerWorker(
            List<PipelineDistributedPlan> distributedPlans) {
        Multiset<DistributedPlanWorker> workerCounter = LinkedHashMultiset.create();
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            for (AssignedJob instanceJob : distributedPlan.getInstanceJobs()) {
                workerCounter.add(instanceJob.getAssignedWorker());
            }
        }
        return workerCounter;
    }

    private static Map<Integer, Integer> computeExchangeSenderNum(PipelineDistributedPlan distributedPlan) {
        Map<Integer, Integer> senderNum = Maps.newLinkedHashMap();
        for (Entry<ExchangeNode, DistributedPlan> kv : distributedPlan.getInputs().entries()) {
            ExchangeNode exchangeNode = kv.getKey();
            PipelineDistributedPlan childPlan = (PipelineDistributedPlan) kv.getValue();
            senderNum.merge(exchangeNode.getId().asInt(), childPlan.getInstanceJobs().size(), Integer::sum);
        }
        return senderNum;
    }

    private static List<TPlanFragmentDestination> destinationToThrift(PipelineDistributedPlan plan) {
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

    private static TPipelineFragmentParams fragmentToThriftIfAbsent(
            PipelineDistributedPlan fragmentPlan, AssignedJob assignedJob,
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToFragmentParams,
            Map<Integer, Integer> exchangeSenderNum,
            Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap,
            Multiset<DistributedPlanWorker> workerProcessInstanceNum,
            List<TPlanFragmentDestination> destinations, CoordinatorContext coordinatorContext) {
        DistributedPlanWorker worker = assignedJob.getAssignedWorker();
        return workerToFragmentParams.computeIfAbsent(worker, w -> {
            PlanFragment fragment = fragmentPlan.getFragmentJob().getFragment();
            ConnectContext connectContext = coordinatorContext.connectContext;

            TPipelineFragmentParams params = new TPipelineFragmentParams();
            params.setIsNereids(true);
            params.setBackendId(worker.id());
            params.setProtocolVersion(PaloInternalServiceVersion.V1);
            params.setDescTbl(coordinatorContext.descriptorTable);
            params.setQueryId(coordinatorContext.queryId);
            params.setFragmentId(fragment.getFragmentId().asInt());

            // Each tParam will set the total number of Fragments that need to be executed on the same BE,
            // and the BE will determine whether all Fragments have been executed based on this information.
            // Notice. load fragment has a small probability that FragmentNumOnHost is 0, for unknown reasons.
            params.setFragmentNumOnHost(workerProcessInstanceNum.count(worker));

            params.setNeedWaitExecutionTrigger(coordinatorContext.twoPhaseExecution);
            params.setPerExchNumSenders(exchangeSenderNum);
            params.setDestinations(destinations);

            int instanceNumInThisFragment = fragmentPlan.getInstanceJobs().size();
            params.setNumSenders(instanceNumInThisFragment);
            params.setTotalInstances(instanceNumInThisFragment);

            params.setCoord(coordinatorContext.coordinatorAddress);
            params.setCurrentConnectFe(coordinatorContext.directConnectFrontendAddress);
            params.setQueryGlobals(coordinatorContext.queryGlobals);
            params.setQueryOptions(new TQueryOptions(coordinatorContext.queryOptions));
            long memLimit = coordinatorContext.queryOptions.getMemLimit();
            if (!connectContext.getSessionVariable().isDisableColocatePlan() && fragment.hasColocatePlanNode()) {
                int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceNumInThisFragment);
                memLimit = coordinatorContext.queryOptions.getMemLimit() / rate;
            }
            params.getQueryOptions().setMemLimit(memLimit);

            params.setSendQueryStatisticsWithEveryBatch(fragment.isTransferQueryStatisticsWithEveryBatch());

            params.setFragment(fragment.toThrift());
            params.setLocalParams(Lists.newArrayList());
            params.setWorkloadGroups(coordinatorContext.getWorkloadGroups());

            params.setFileScanParams(fileScanRangeParamsMap);

            if (fragmentPlan.getFragmentJob() instanceof UnassignedScanBucketOlapTableJob) {
                int bucketNum = ((UnassignedScanBucketOlapTableJob) fragmentPlan.getFragmentJob())
                        .getOlapScanNodes()
                        .get(0)
                        .getBucketNum();
                params.setNumBuckets(bucketNum);
            }

            params.setBucketSeqToInstanceIdx(computeBucketIdToInstanceId(fragmentPlan));
            params.setShuffleIdxToInstanceIdx(computeDestIdToInstanceId(fragmentPlan));
            return params;
        });
    }

    private static Map<Integer, TFileScanRangeParams> computeFileScanRangeParams(
            PipelineDistributedPlan distributedPlan) {
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

    private static TPipelineInstanceParams instanceToThrift(
            TPipelineFragmentParams currentFragmentParam,
            AssignedJob instance, int currentInstanceNum) {
        TPipelineInstanceParams instanceParam = new TPipelineInstanceParams();
        instanceParam.setFragmentInstanceId(instance.instanceId());
        setScanSourceParam(currentFragmentParam, instance, instanceParam);

        instanceParam.setSenderId(instance.indexInUnassignedJob());
        instanceParam.setBackendNum(currentInstanceNum);
        instanceParam.setRuntimeFilterParams(new TRuntimeFilterParams());
        // instanceParam.runtime_filter_params.setRuntimeFilterMergeAddr(runtimeFilterMergeAddr);
        // topn filter
        return instanceParam;
    }

    private static void setScanSourceParam(
            TPipelineFragmentParams currentFragmentParam, AssignedJob instance,
            TPipelineInstanceParams instanceParams) {

        boolean isLocalShuffle = instance instanceof LocalShuffleAssignedJob;
        if (isLocalShuffle && ((LocalShuffleAssignedJob) instance).receiveDataFromLocal) {
            // save thrift rpc message size, don't need perNodeScanRanges and perNodeSharedScans,
            // but the perNodeScanRanges is required rpc field
            instanceParams.setPerNodeScanRanges(Maps.newLinkedHashMap());
            return;
        }

        ScanSource scanSource = instance.getScanSource();
        ScanParams scanParams;
        if (scanSource instanceof BucketScanSource) {
            scanParams = computeBucketScanSourceParam((BucketScanSource) scanSource);
        } else {
            scanParams = computeDefaultScanSourceParam((DefaultScanSource) scanSource);
        }
        // perNodeScanRanges is required
        instanceParams.setPerNodeScanRanges(scanParams.perNodeScanRanges);

        if (isLocalShuffle) {
            // a fragment in a backend only enable local shuffle once for the first local shuffle instance,
            // because we just skip set scan params for LocalShuffleAssignedJob.receiveDataFromLocal == true
            enableLocalShuffle(currentFragmentParam, scanParams);
        } else {
            disableLocalShuffle(currentFragmentParam);
        }
    }

    private static void enableLocalShuffle(TPipelineFragmentParams currentFragmentParam, ScanParams scanParams) {
        // enable local shuffle.
        // only set fragment.per_node_shared_scans by first instance.per_node_shared_scans.
        // parallel_instances == 1 && fragments.instances.size > 1 is the switch of local shuffle.
        currentFragmentParam.setParallelInstances(1);
        currentFragmentParam.setPerNodeSharedScans(scanParams.perNodeSharedScans);
    }

    private static void disableLocalShuffle(TPipelineFragmentParams currentFragmentParam) {
        // disable local shuffle, because the parallel_instances == fragments.instances.size,
        // if a fragment use local shuffle, all instanceJob would be LocalShuffleAssignedJob.
        // if not, all instanceJob would be StaticAssignedJob.
        // so we can cumulative parallel_instances as the fragments.instances.size.
        currentFragmentParam.setParallelInstances(currentFragmentParam.getParallelInstances() + 1);
    }

    private static ScanParams computeDefaultScanSourceParam(DefaultScanSource defaultScanSource) {
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newLinkedHashMap();
        Map<Integer, Boolean> perNodeSharedScans = Maps.newLinkedHashMap();
        for (Entry<ScanNode, ScanRanges> kv : defaultScanSource.scanNodeToScanRanges.entrySet()) {
            int scanNodeId = kv.getKey().getId().asInt();
            perNodeScanRanges.put(scanNodeId, kv.getValue().params);
            perNodeSharedScans.put(scanNodeId, true);
        }

        return new ScanParams(perNodeScanRanges, perNodeSharedScans);
    }

    private static ScanParams computeBucketScanSourceParam(BucketScanSource bucketScanSource) {
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newLinkedHashMap();
        Map<Integer, Boolean> perNodeSharedScans = Maps.newLinkedHashMap();
        for (Entry<Integer, Map<ScanNode, ScanRanges>> kv :
                bucketScanSource.bucketIndexToScanNodeToTablets.entrySet()) {
            Map<ScanNode, ScanRanges> scanNodeToRanges = kv.getValue();
            for (Entry<ScanNode, ScanRanges> kv2 : scanNodeToRanges.entrySet()) {
                int scanNodeId = kv2.getKey().getId().asInt();
                List<TScanRangeParams> scanRanges = perNodeScanRanges.computeIfAbsent(scanNodeId, ArrayList::new);
                scanRanges.addAll(kv2.getValue().params);
                perNodeSharedScans.put(scanNodeId, true);
            }
        }
        return new ScanParams(perNodeScanRanges, perNodeSharedScans);
    }

    private static Map<Integer, Integer> computeBucketIdToInstanceId(PipelineDistributedPlan receivePlan) {
        List<AssignedJob> instanceJobs = receivePlan.getInstanceJobs();
        if (instanceJobs.isEmpty() || !(instanceJobs.get(0).getScanSource() instanceof BucketScanSource)) {
            // bucket_seq_to_instance_id is optional, so we can return null to save memory
            return null;
        }

        Map<Integer, Integer> bucketIdToInstanceId = Maps.newLinkedHashMap();
        filterInstancesWhichReceiveDataFromRemote(receivePlan, (instanceJob, instanceIdInThisBackend) -> {
            BucketScanSource scanSource = (BucketScanSource) instanceJob.getScanSource();
            for (Integer bucketIndex : scanSource.bucketIndexToScanNodeToTablets.keySet()) {
                bucketIdToInstanceId.put(bucketIndex, instanceIdInThisBackend);
            }
        });
        return bucketIdToInstanceId;
    }

    private static Map<Integer, Integer> computeDestIdToInstanceId(PipelineDistributedPlan receivePlan) {
        if (receivePlan.getInputs().isEmpty()) {
            // shuffle_idx_to_index_id is required
            return Maps.newLinkedHashMap();
        }

        AtomicInteger receiveFromRemoteInstanceNum = new AtomicInteger();
        Map<Integer, Integer> destIdToInstanceId = Maps.newLinkedHashMap();
        filterInstancesWhichReceiveDataFromRemote(receivePlan, (instanceJob, instanceIdInThisBackend) ->
                destIdToInstanceId.put(receiveFromRemoteInstanceNum.getAndIncrement(), instanceIdInThisBackend)
        );
        return destIdToInstanceId;
    }

    private static void filterInstancesWhichReceiveDataFromRemote(
            PipelineDistributedPlan receivePlan, BiConsumer<AssignedJob, Integer> computeFn) {
        Map<Long, AtomicInteger> backendIdToInstanceCount = Maps.newLinkedHashMap();

        List<AssignedJob> instanceJobs = receivePlan.getInstanceJobs();
        for (int i = 0; i < instanceJobs.size(); i++) {
            AssignedJob instanceJob = instanceJobs.get(i);
            AtomicInteger instanceCount = backendIdToInstanceCount.computeIfAbsent(
                    instanceJob.getAssignedWorker().id(),
                    (backendId) -> new AtomicInteger()
            );
            int instanceIdInThisBackend = instanceCount.getAndIncrement();
            if (instanceJob instanceof LocalShuffleAssignedJob
                    && ((LocalShuffleAssignedJob) instanceJob).receiveDataFromLocal) {
                // not receive data from remote fragment
                continue;
            }
            computeFn.accept(instanceJob, instanceIdInThisBackend);
        }
    }

    private static class ScanParams {
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges;
        Map<Integer, Boolean> perNodeSharedScans;

        public ScanParams(Map<Integer, List<TScanRangeParams>> perNodeScanRanges,
                Map<Integer, Boolean> perNodeSharedScans) {
            this.perNodeScanRanges = perNodeScanRanges;
            this.perNodeSharedScans = perNodeSharedScans;
        }
    }
}
