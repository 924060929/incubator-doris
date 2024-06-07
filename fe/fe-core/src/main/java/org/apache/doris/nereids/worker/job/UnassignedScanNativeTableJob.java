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
import com.google.common.collect.ListMultimap;
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

    public List<OlapScanNode> getOlapScanNodes() {
        return olapScanNodes;
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
            return assignWithoutBucket();
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
        // for every bucket tablet, select its replica and worker.
        // for example, colocate join:
        // {
        //    BackendWorker("172.0.0.1"): {
        //       bucket 0: {
        //         olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //         olapScanNode2: ScanRanges([tablet_10009, tablet_10010, tablet_10011, tablet_10012])
        //       },
        //       bucket 1: {
        //         olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008])
        //         olapScanNode2: ScanRanges([tablet_10013, tablet_10014, tablet_10015, tablet_10016])
        //       },
        //       ...
        //    },
        //    BackendWorker("172.0.0.2"): {
        //       ...
        //    }
        // }
        Map<Worker, UninstancedScanSource> assignedBucketScanRanges = multipleMachinesParallelizationWithBuckets();

        // separate buckets to instanceNum groups, let one instance process some buckets.
        // for example, colocate join:
        // {
        //    // 172.0.0.1 has two instances
        //    BackendWorker("172.0.0.1"): [
        //       // instance 1 process two buckets
        //       {
        //         bucket 0: {
        //           olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //           olapScanNode2: ScanRanges([tablet_10009, tablet_10010, tablet_10011, tablet_10012])
        //         },
        //         bucket 1: {
        //           olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008])
        //           olapScanNode2: ScanRanges([tablet_10013, tablet_10014, tablet_10015, tablet_10016])
        //         }
        //       },
        //       // instance 1 process one bucket
        //       {
        //         bucket 3: ...
        //       }
        //    ]
        //    // instance 4... in "172.0.0.1"
        //    BackendWorker("172.0.0.2"): [
        //       ...
        //    ],
        //    ...
        // }
        Map<Worker, List<ScanSource>> parallelizedBuckets = insideMachineParallelization(assignedBucketScanRanges);

        // flatten to instances.
        // for example:
        // [
        //   instance 1: AssignedJob(BackendWorker("172.0.0.1"), BucketScanSource(...)),
        //   instance 2: AssignedJob(BackendWorker("172.0.0.1"), BucketScanSource(...)),
        //   instance 3: AssignedJob(BackendWorker("172.0.0.2"), BucketScanSource(...)),
        //   instance 4: AssignedJob(BackendWorker("172.0.0.2"), BucketScanSource(...)),
        // ]
        return buildInstances(parallelizedBuckets);
    }

    private List<AssignedJob> assignWithoutBucket() {
        // for every tablet, select its replica and worker.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"):
        //          olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //    BackendWorker("172.0.0.2"):
        //          olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008, tablet_10009])
        // }
        Map<Worker, UninstancedScanSource> assignedScanRanges = multipleMachinesParallelization();

        // for each worker, compute how many instances should be generated, and which data should be scanned.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"): [
        //        instance 1: olapScanNode1: ScanRanges([tablet_10001, tablet_10003])
        //        instance 2: olapScanNode1: ScanRanges([tablet_10002, tablet_10004])
        //    ],
        //    BackendWorker("172.0.0.2"): [
        //        instance 3: olapScanNode1: ScanRanges([tablet_10005, tablet_10008])
        //        instance 4: olapScanNode1: ScanRanges([tablet_10006, tablet_10009])
        //        instance 5: olapScanNode1: ScanRanges([tablet_10007])
        //    ],
        // }
        Map<Worker, List<ScanSource>> workerToPerInstanceScanRanges
                = insideMachineParallelization(assignedScanRanges);

        // flatten to instances.
        // for example:
        // [
        //   instance 1: AssignedJob(BackendWorker("172.0.0.1"), DefaultScanRange(
        //     olapScanNode1: [tablet_10001, tablet_10003])
        //   ),
        //   instance 2: AssignedJob(BackendWorker("172.0.0.1"), DefaultScanRange(
        //      olapScanNode1: [tablet_10002, tablet_10004])
        //   ),
        //   instance 3: AssignedJob(BackendWorker("172.0.0.2"), DefaultScanRange(
        //     olapScanNode1: [tablet_10005, tablet_10008])
        //   ),
        //   instance 4: AssignedJob(BackendWorker("172.0.0.2"), DefaultScanRange(
        //     olapScanNode1[tablet_10006, tablet_10009])
        //   ),
        //   instance 5: AssignedJob(BackendWorker("172.0.0.2"), DefaultScanRange(
        //     olapScanNode1: [tablet_10007])
        //   ),
        // ]
        return buildInstances(workerToPerInstanceScanRanges);
    }

    protected Map<Worker, UninstancedScanSource> multipleMachinesParallelization() {
        return scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(olapScanNodes.get(0));
    }

    protected Map<Worker, UninstancedScanSource> multipleMachinesParallelizationWithBuckets() {
        return scanWorkerSelector.selectReplicaAndWorkerWithBucket(this);
    }

    protected Map<Worker, List<ScanSource>> insideMachineParallelization(
            Map<Worker, UninstancedScanSource> workerToScanRanges) {

        Map<Worker, List<ScanSource>> workerToInstances = Maps.newLinkedHashMap();
        for (Entry<Worker, UninstancedScanSource> entry : workerToScanRanges.entrySet()) {
            Worker worker = entry.getKey();

            // the scanRanges which this worker should scan,
            // for example:
            // {
            //   scan tbl1: [tablet_10001, tablet_10002, tablet_10003, tablet_10004] // no instances
            // }
            ScanSource scanSource = entry.getValue().scanSource;

            // usually, its tablets num, or buckets num
            int maxParallel = scanSource.maxParallel(scanNodes);

            // now we should compute how many instances to process the data,
            // for example: two instances
            int instanceNum = degreeOfParallelism(maxParallel);

            // split the scanRanges to some partitions, one partition for one instance
            // for example:
            //  [
            //     scan tbl1: [tablet_10001, tablet_10003], // instance 1
            //     scan tbl1: [tablet_10002, tablet_10004]  // instance 2
            //  ]
            List<ScanSource> instanceToScanRanges = scanSource.parallelize(
                    scanNodes, instanceNum
            );

            workerToInstances.put(worker, instanceToScanRanges);
        }

        return workerToInstances;
    }
}
