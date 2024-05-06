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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * UnassignedScanNativeTableJob.
 * scan native olap table, we can assign a worker near the storage
 */
public class UnassignedScanNativeTableJob extends AbstractUnassignedJob implements UnassignedNearStorageJob {
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

        // filter scan nodes
        ImmutableList.Builder<OlapScanNode> olapScanNodes = ImmutableList.builderWithExpectedSize(allScanNodes.size());
        for (ScanNode allScanNode : allScanNodes) {
            if (allScanNode instanceof OlapScanNode) {
                olapScanNodes.add((OlapScanNode) allScanNode);
            }
        }
        this.olapScanNodes = olapScanNodes.build();
    }

    @Override
    public List<ScanNode> nearStorageScanNodes() {
        return (List) olapScanNodes;
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            WorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        if (shouldAssignByBucket()) {
            return assignWithBucket();
        } else {
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

    private List<AssignedJob> assignWithoutBucket() {
        Map<Worker, Map<ScanNode, ScanRanges>> workerToReplicas
                = scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(this);

        List<AssignedJob> assignments = Lists.newArrayListWithCapacity(workerToReplicas.size());
        int instanceIndexInFragment = 0;
        for (Entry<Worker, Map<ScanNode, ScanRanges>> entry : workerToReplicas.entrySet()) {
            Worker selectedWorker = entry.getKey();
            Map<ScanNode, ScanRanges> scanNodeToRanges = entry.getValue();
            AssignedJob instanceJob = assignWorkerAndDataSources(
                    instanceIndexInFragment++, selectedWorker, new DefaultScanSource(scanNodeToRanges)
            );
            assignments.add(instanceJob);
        }
        return assignments;
    }
}
