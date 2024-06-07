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

import org.apache.doris.nereids.worker.Worker;
import org.apache.doris.nereids.worker.WorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** AbstractUnassignedScanJob */
public abstract class AbstractUnassignedScanJob extends AbstractUnassignedJob {
    public AbstractUnassignedScanJob(PlanFragment fragment,
            List<ScanNode> scanNodes, Map<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(fragment, scanNodes, exchangeToChildJob);
    }


    @Override
    public List<AssignedJob> computeAssignedJobs(WorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {

        Map<Worker, UninstancedScanSource> workerToScanSource = multipleMachinesParallelization(
                workerManager, inputJobs);

        Map<Worker, List<ScanSource>> workerToInstanceScanSource = insideMachineParallelization(workerToScanSource);

        return buildInstances(workerToInstanceScanSource);
    }

    protected abstract Map<Worker, UninstancedScanSource> multipleMachinesParallelization(
            WorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs);

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

    protected int degreeOfParallelism(int maxParallel) {
        Preconditions.checkArgument(maxParallel > 0, "maxParallel must be positive");
        if (!fragment.getDataPartition().isPartitioned()) {
            return 1;
        }
        if (scanNodes.size() == 1 && scanNodes.get(0) instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) scanNodes.get(0);
            // if the scan node have limit and no conjuncts, only need 1 instance to save cpu and mem resource,
            // e.g. select * from tbl limit 10
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null && olapScanNode.shouldUseOneInstance(connectContext)) {
                return 1;
            }
        }

        // the scan instance num should not larger than the tablets num
        return Math.min(maxParallel, Math.max(fragment.getParallelExecNum(), 1));
    }

    protected List<AssignedJob> buildInstances(Map<Worker, List<ScanSource>> workerToPerInstanceScanSource) {
        // flatten to instances.
        // for example:
        // [
        //   instance 1: AssignedJob(BackendWorker("172.0.0.1"), ScanSource(...)),
        //   instance 2: AssignedJob(BackendWorker("172.0.0.1"), ScanSource(...)),
        //   instance 3: AssignedJob(BackendWorker("172.0.0.2"), ScanSource(...)),
        //   instance 4: AssignedJob(BackendWorker("172.0.0.2"), ScanSource(...)),
        // ]
        List<AssignedJob> assignments = Lists.newArrayList();
        int instanceIndexInFragment = 0;
        for (Entry<Worker, List<ScanSource>> entry : workerToPerInstanceScanSource.entrySet()) {
            Worker selectedWorker = entry.getKey();
            List<ScanSource> scanSourcePerInstance = entry.getValue();
            for (ScanSource oneInstanceScanSource : scanSourcePerInstance) {
                AssignedJob instanceJob = assignWorkerAndDataSources(
                        instanceIndexInFragment++, selectedWorker, oneInstanceScanSource);
                assignments.add(instanceJob);
            }
        }
        return assignments;
    }
}
