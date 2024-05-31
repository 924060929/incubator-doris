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
import org.apache.doris.nereids.worker.WorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;

import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * UnassignedScanRemoteTableJob
 * it should be a leaf job which not contains scan native olap table node,
 * for example, select literal without table, or scan an external table
 */
public class UnassignedScanRemoteTableJob extends AbstractUnassignedJob {
    private final ScanWorkerSelector scanWorkerSelector;
    private int assignedJobNum;

    public UnassignedScanRemoteTableJob(
            PlanFragment fragment, List<ScanNode> scanNodes, Map<ExchangeNode, UnassignedJob> exchangeToChildJob,
            ScanWorkerSelector scanWorkerSelector) {
        super(fragment, scanNodes, exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(scanWorkerSelector, "scanWorkerSelector is not null");
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(WorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        for (ScanNode scanNode : scanNodes) {

        }
        return super.computeAssignedJobs(workerManager, inputJobs);
    }

    // public static Map<> splitRanges(ScanNode scanNode) {
    //     List<TScanRangeLocations> scanLocations = scanNode.getScanRangeLocations(0);
    //     for (TScanRangeLocations scanLocation : scanLocations) {
    //         List<TScanRangeLocation> locations = scanLocation.getLocations();
    //     }
    // }
}
