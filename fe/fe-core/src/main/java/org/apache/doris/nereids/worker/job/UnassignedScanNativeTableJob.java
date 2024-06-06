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

package org.apache.doris.nereids.worker.job;

import org.apache.doris.nereids.worker.ScanWorkerSelector;
import org.apache.doris.nereids.worker.Worker;
import org.apache.doris.nereids.worker.WorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * UnassignedScanNativeTableJob.
 * scan native olap table, we can assign a worker near the storage
 */
public class UnassignedScanNativeTableJob extends AbstractUnassignedJob {
    private final ScanWorkerSelector scanWorkerSelector;
    private final List<OlapScanNode> olapScanNodes;

    /** UnassignedScanNativeTableJob */
    public UnassignedScanNativeTableJob(
            PlanFragment fragment, List<ScanNode> allScanNodes,
            Map<ExchangeNode, UnassignedJob> exchangeToChildJob,
            ScanWorkerSelector scanWorkerSelector) {
        super(fragment, allScanNodes, exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(
                scanWorkerSelector, "scanWorkerSelector cat not be null");

        Preconditions.checkArgument(!allScanNodes.isEmpty(), "OlapScanNode is empty");

        for (ScanNode scanNode : allScanNodes) {
            if (!(scanNode instanceof OlapScanNode)) {
                throw new IllegalStateException(
                        "UnassignedScanNativeTableJob only support process OlapScanNode, but meet: "
                                + scanNode.getClass().getSimpleName());
            }
        }
        this.olapScanNodes = (List) allScanNodes;
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            WorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        if (shouldAssignByBucket()) {
            return assignWithBucket();
        } else {
            Preconditions.checkState(
                    olapScanNodes.size() == 1,
                    "One fragment contains multiple OlapScanNodes but not contains colocate join or bucket shuffle join"
            );
            return assignWithoutBucket(olapScanNodes.get(0));
        }
    }

    private boolean shouldAssignByBucket() {
        if (fragment.hasColocatePlanNode()) {
            return true;
        }
        if (enableBucketShuffleJoin() && fragment.isBucketShuffleJoinInput()) {
            return true;
        }
        return false;
    }

    private boolean enableBucketShuffleJoin() {
        if (ConnectContext.get() != null) {
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (!sessionVariable.isEnableBucketShuffleJoin() && !sessionVariable.isEnableNereidsPlanner()) {
                return false;
            }
        }
        return true;
    }

    private List<AssignedJob> assignWithBucket() {
        Map<Worker, Map<Integer, Map<ScanNode, ScanRanges>>> workerToReplicas
                = scanWorkerSelector.selectReplicaAndWorkerWithBucket(this);

        List<AssignedJob> assignments = Lists.newArrayListWithCapacity(workerToReplicas.size());
        int instanceIndexInFragment = 0;
        for (Entry<Worker, Map<Integer, Map<ScanNode, ScanRanges>>> entry : workerToReplicas.entrySet()) {
            Worker selectedWorker = entry.getKey();
            Map<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToToReplicas = entry.getValue();

            AssignedJob instanceJob = assignWorkerAndDataSources(
                    instanceIndexInFragment++, selectedWorker,
                    new BucketScanSource(bucketIndexToScanNodeToToReplicas)
            );
            assignments.add(instanceJob);
        }
        return assignments;
    }

    private List<AssignedJob> assignWithoutBucket(OlapScanNode olapScanNode) {
        // for every tablet, select its replica and worker.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"):
        //          ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //    BackendWorker("172.0.0.2"):
        //          ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008, tablet_10009])
        // }
        Map<Worker, ScanRanges> assignedScanRanges = multipleMachinesParallelization(olapScanNode);

        // for each worker, compute how many instances should be generated, and which data should be scanned.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"): [
        //        instance 1: ScanRanges([tablet_10001, tablet_10003])
        //        instance 2: ScanRanges([tablet_10002, tablet_10004])
        //    ],
        //    BackendWorker("172.0.0.2"): [
        //        instance 3: ScanRanges([tablet_10005, tablet_10008])
        //        instance 4: ScanRanges([tablet_10006, tablet_10009])
        //        instance 5: ScanRanges([tablet_10007])
        //    ],
        // }
        Map<Worker, List<ScanRanges>> workerToPerInstanceScanRanges
                = insideMachineParallelization(olapScanNode, assignedScanRanges);

        // flatten to instances.
        // for example:
        // [
        //   instance 1: AssignedJob(BackendWorker("172.0.0.1"), ScanRanges([tablet_10001, tablet_10003])),
        //   instance 2: AssignedJob(BackendWorker("172.0.0.1"), ScanRanges([tablet_10002, tablet_10004])),
        //   instance 3: AssignedJob(BackendWorker("172.0.0.2"), ScanRanges([tablet_10005, tablet_10008])),
        //   instance 4: AssignedJob(BackendWorker("172.0.0.2"), ScanRanges([tablet_10006, tablet_10009])),
        //   instance 5: AssignedJob(BackendWorker("172.0.0.2"), ScanRanges([tablet_10007])),
        // ]
        return buildInstances(olapScanNode, workerToPerInstanceScanRanges);
    }

    protected Map<Worker, ScanRanges> multipleMachinesParallelization(OlapScanNode olapScanNode) {
        return scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(olapScanNode);
    }

    protected <S extends Splittable<S>> Map<Worker, List<S>> insideMachineParallelization(
            OlapScanNode olapScanNode, Map<Worker, S> workerToScanRanges) {

        Map<Worker, List<S>> workerToInstances = Maps.newLinkedHashMap();

        for (Entry<Worker, S> entry : workerToScanRanges.entrySet()) {
            Worker worker = entry.getKey();

            // the scanRanges which this worker should scan,
            // for example: scan [tablet_10001, tablet_10002, tablet_10003, tablet_10004]
            S allScanRanges = entry.getValue();

            // now we should compute how many instances to process the data,
            // for example: two instances
            int instanceNum = degreeOfParallelism(olapScanNode, allScanRanges.itemSize());

            // split the scanRanges to some partitions, one partition for one instance
            // for example:
            //  [
            //     instance 1: [tablet_10001, tablet_10003]
            //     instance 2: [tablet_10002, tablet_10004]
            //  ]
            List<S> instanceToScanRanges = allScanRanges.split(instanceNum);

            workerToInstances.put(worker, instanceToScanRanges);
        }

        return workerToInstances;
    }

    protected <S extends Splittable<S>> List<AssignedJob> buildInstances(
            OlapScanNode olapScanNode,
            Map<Worker, List<ScanRanges>> workerToPerInstanceScanRanges) {
        List<AssignedJob> assignments = Lists.newArrayList();
        int instanceIndexInFragment = 0;
        for (Entry<Worker, List<ScanRanges>> entry : workerToPerInstanceScanRanges.entrySet()) {
            Worker selectedWorker = entry.getKey();
            List<ScanRanges> scanRangesPerInstance = entry.getValue();
            for (ScanRanges oneInstanceScanRanges : scanRangesPerInstance) {
                AssignedJob instanceJob = assignWorkerAndDataSources(
                        instanceIndexInFragment++, selectedWorker,
                        new DefaultScanSource(ImmutableMap.of(olapScanNode, oneInstanceScanRanges))
                );
                assignments.add(instanceJob);
            }
        }
        return assignments;
    }

    protected int degreeOfParallelism(ScanNode olapScanNode, int scanRangesSize) {
        // if the scan node have limit and no conjuncts, only need 1 instance to save cpu and mem resource
        if (ConnectContext.get() != null && olapScanNode.shouldUseOneInstance(ConnectContext.get())) {
            return 1;
        }

        // the scan instance num should not larger than the tablets num
        return Math.min(scanRangesSize, Math.max(fragment.getParallelExecNum(), 1));
    }
}
