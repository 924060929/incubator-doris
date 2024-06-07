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

import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * UnassignedScanBucketOlapTableJob.
 * bucket shuffle join olap table, or colocate join olap table
 */
public class UnassignedScanBucketOlapTableJob extends AbstractUnassignedScanJob {
    private final ScanWorkerSelector scanWorkerSelector;
    private final List<OlapScanNode> olapScanNodes;

    /** UnassignedScanNativeTableJob */
    public UnassignedScanBucketOlapTableJob(
            PlanFragment fragment, List<OlapScanNode> olapScanNodes,
            Map<ExchangeNode, UnassignedJob> exchangeToChildJob,
            ScanWorkerSelector scanWorkerSelector) {
        super(fragment, (List) olapScanNodes, exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(
                scanWorkerSelector, "scanWorkerSelector cat not be null");

        Preconditions.checkArgument(!olapScanNodes.isEmpty(), "OlapScanNode is empty");
        this.olapScanNodes = olapScanNodes;
    }

    public List<OlapScanNode> getOlapScanNodes() {
        return olapScanNodes;
    }

    @Override
    protected Map<Worker, UninstancedScanSource> multipleMachinesParallelization(
            WorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
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
        return scanWorkerSelector.selectReplicaAndWorkerWithBucket(this);
    }

    @Override
    protected Map<Worker, List<ScanSource>> insideMachineParallelization(
            Map<Worker, UninstancedScanSource> workerToScanRanges) {
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
        return super.insideMachineParallelization(workerToScanRanges);
    }
}
