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
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.ExecContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** GroupWorkerPipelineThriftProtocol */
public class GroupWorkerPipelineThriftProtocol implements WorkerProtocol {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    private final ExecContext execContext;
    private final BackendServiceProxy backendClientProxy = BackendServiceProxy.getInstance();

    // private Map<DistributedPlanWorker, TPipelineFragmentParams> workerToFragmentParams;

    public GroupWorkerPipelineThriftProtocol(NereidsPlanner planner) {
        this.execContext = toThrift(planner);
    }

    @Override
    public void serialize() {
        // group by worker, so that we can use one RPC to send all fragment instances of a worker.
        // Map<DistributedPlanWorker, FragmentToInstances> workerToFragmentAndInstances
        //         = groupByWorker((List) distributedPlans);
        //
        // workerToFragmentParams = Maps.newLinkedHashMapWithExpectedSize(workerToFragmentAndInstances.size());
        // for (Entry<DistributedPlanWorker, FragmentToInstances> kv : workerToFragmentAndInstances.entrySet()) {
        //     DistributedPlanWorker worker = kv.getKey();
        //     FragmentToInstances fragmentToInstances = kv.getValue();
        //
        //     // generate thrift parameters for this worker,
        //     // merge all fragments parameters into one TPipelineFragmentParams
        //     TPipelineFragmentParams mergedFragmentParams = fragmentsToThrift(fragmentToInstances);
        //     workerToFragmentParams.put(worker, mergedFragmentParams);
        // }
    }

    @Override
    public void send() {

    }

    @Override
    public void cancel(Status cancelReason) {
        // backendClientProxy.cancelPlanFragmentAsync();
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
        ListMultimap<DistributedPlanWorker, TPipelineFragmentParams> workerToFragmentsParam
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

    private ListMultimap<DistributedPlanWorker, TPipelineFragmentParams> plansToThrift(
            List<PipelineDistributedPlan> distributedPlans, ExecContext execContext) {
        Multiset<DistributedPlanWorker> workerProcessInstanceNum = computeInstanceNumPerWorker(distributedPlans);
        ListMultimap<DistributedPlanWorker, TPipelineFragmentParams> fragmentsGroupByWorker
                = ArrayListMultimap.create();
        int currentInstanceIndex = 0;
        for (PipelineDistributedPlan currentFragmentPlan : distributedPlans) {
            TPlanFragment currentFragmentThrift = currentFragmentPlan.getFragmentJob().getFragment().toThrift();
            Map<Integer, TFileScanRangeParams> fileScanRangeParams = computeFileScanRangeParams(currentFragmentPlan);
            Map<Integer, Integer> exchangeSenderNum = computeExchangeSenderNum(currentFragmentPlan);
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToCurrentFragment = Maps.newLinkedHashMap();
            for (AssignedJob instanceJob : currentFragmentPlan.getInstanceJobs()) {
                // Suggestion: Do not modify currentFragmentParam out of the `fragmentToThriftIfAbsent` method,
                //             except add instanceParam into local_params
                TPipelineFragmentParams currentFragmentParam = fragmentToThriftIfAbsent(
                        currentFragmentPlan, instanceJob, workerToCurrentFragment,
                        exchangeSenderNum, currentFragmentThrift, fileScanRangeParams,
                        workerProcessInstanceNum, execContext);
                TPipelineInstanceParams instanceParam
                        = instanceToThrift(currentFragmentPlan, instanceJob, currentInstanceIndex++);
                List<TPipelineInstanceParams> instancesParams = currentFragmentParam.getLocalParams();
                // currentFragmentParam.getShuffleIdxToInstanceIdx().put(instanceParam.recvrId, instancesParams.size());
                instancesParams.add(instanceParam);
            }

            // arrange fragments by the same worker,
            // so we can merge and send multiple fragment to a backend use one rpc
            for (Entry<DistributedPlanWorker, TPipelineFragmentParams> kv : workerToCurrentFragment.entrySet()) {
                fragmentsGroupByWorker.put(kv.getKey(), kv.getValue());
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

    private TPipelineFragmentParams fragmentToThriftIfAbsent(
            PipelineDistributedPlan fragmentPlan, AssignedJob assignedJob,
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToFragmentParams,
            Map<Integer, Integer> exchangeSenderNum, TPlanFragment fragmentThrift,
            Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap,
            Multiset<DistributedPlanWorker> workerProcessInstanceNum, ExecContext execContext) {
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
            // params.setDestinations(destinations);

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
            // params.setPerNodeSharedScans(perNodeSharedScans);
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

    private Map<DistributedPlanWorker, FragmentToInstances> groupByWorker(
            List<PipelineDistributedPlan> distributedPlans) {
        Map<DistributedPlanWorker, FragmentToInstances> workerToFragmentAndInstances = Maps.newLinkedHashMap();
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            List<AssignedJob> instanceJobs = distributedPlan.getInstanceJobs();
            Preconditions.checkState(!instanceJobs.isEmpty());
            for (AssignedJob instanceJob : instanceJobs) {
                DistributedPlanWorker worker = instanceJob.getAssignedWorker();
                FragmentToInstances fragmentToInstances =
                        workerToFragmentAndInstances.computeIfAbsent(worker, FragmentToInstances::new);
                fragmentToInstances.fragmentToInstances.put(distributedPlan, instanceJob);
            }
        }
        return workerToFragmentAndInstances;
    }

    private TPipelineInstanceParams instanceToThrift(
            PipelineDistributedPlan distributedPlan, AssignedJob instance, int currentInstanceNum) {
        TPipelineInstanceParams instanceParam = new TPipelineInstanceParams();
        instanceParam.setFragmentInstanceId(instance.instanceId());
        setScanSourceParam(instance.getScanSource(), instanceParam);
        // instanceParam.setPerNodeSharedScans(perNodeSharedScans);
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
        for (Entry<ScanNode, ScanRanges> kv : defaultScanSource.scanNodeToScanRanges.entrySet()) {
            scanNodeIdToScanRanges.put(kv.getKey().getId().asInt(), kv.getValue().params);
        }
        params.setPerNodeScanRanges(scanNodeIdToScanRanges);
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
}
