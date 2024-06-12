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

        return insideMachineParallelization(workerToScanSource);
    }

    protected abstract Map<Worker, UninstancedScanSource> multipleMachinesParallelization(
            WorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs);

    protected List<AssignedJob> insideMachineParallelization(
            Map<Worker, UninstancedScanSource> workerToScanRanges) {

        boolean useLocalShuffle = useShareScan(workerToScanRanges);
        int instanceIndexInFragment = 0;
        int shareScanIndex = 0;
        List<AssignedJob> instances = Lists.newArrayList();
        for (Entry<Worker, UninstancedScanSource> entry : workerToScanRanges.entrySet()) {
            Worker worker = entry.getKey();

            // the scanRanges which this worker should scan,
            // for example:
            // {
            //   scan tbl1: [tablet_10001, tablet_10002, tablet_10003, tablet_10004] // no instances
            // }
            ScanSource scanSource = entry.getValue().scanSource;

            // usually, its tablets num, or buckets num
            int scanSourceMaxParallel = scanSource.maxParallel(scanNodes);

            // now we should compute how many instances to process the data,
            // for example: two instances
            int instanceNum = degreeOfParallelism(scanSourceMaxParallel);

            List<ScanSource> instanceToScanRanges;
            if (useLocalShuffle) {
                // only generate one instance to scan all data, in this step
                instanceToScanRanges = scanSource.parallelize(
                        scanNodes, 1
                );

                // Some tablets too big, we need add parallel to process these tablets after scan,
                // for example, use one OlapScanNode to scan data, and use some local instances
                // to process Aggregation parallel. We call it `share scan`. Backend will know this
                // instances share the same ScanSource, and will not scan same data multiple times.
                //
                // +-------------------------------- same fragment in one host ------------------------------------+
                // |                instance1      instance2     instance3     instance4                           |
                // |                    \              \             /            /                                |
                // |                                                                                               |
                // |                                     OlapScanNode                                              |
                // |(share scan node, and local shuffle data to other local instance to parallel compute this data)|
                // +-----------------------------------------------------------------------------------------------+
                ScanSource shareScanSource = instanceToScanRanges.get(0);
                for (int i = 0; i < instanceNum; i++) {
                    // one scan range generate instanceNum instances,
                    // different instances reference the same scan source
                    ShareScanAssignedJob instance = new ShareScanAssignedJob(
                            instanceIndexInFragment++, shareScanIndex, this, worker, shareScanSource);
                    instances.add(instance);
                }
                shareScanIndex++;
            } else {
                // split the scanRanges to some partitions, one partition for one instance
                // for example:
                //  [
                //     scan tbl1: [tablet_10001, tablet_10003], // instance 1
                //     scan tbl1: [tablet_10002, tablet_10004]  // instance 2
                //  ]
                instanceToScanRanges = scanSource.parallelize(
                        scanNodes, instanceNum
                );

                for (ScanSource instanceToScanRange : instanceToScanRanges) {
                    instances.add(assignWorkerAndDataSources(instanceIndexInFragment++, worker, instanceToScanRange));
                }
            }
        }

        return instances;
    }

    protected boolean useShareScan(Map<Worker, UninstancedScanSource> workerToScanRanges) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isForceToLocalShuffle()) {
            return true;
        }
        return parallelTooLittle(workerToScanRanges);
    }

    protected boolean parallelTooLittle(Map<Worker, UninstancedScanSource> workerToScanRanges) {
        if (scanNodes.size() > 1) {
            return noEnoughScanRange(workerToScanRanges) && noEnoughBuckets(workerToScanRanges);
        } else if (scanNodes.size() == 1) {
            return noEnoughScanRange(workerToScanRanges);
        } else {
            return false;
        }
    }

    protected boolean noEnoughScanRange(
            Map<Worker, UninstancedScanSource> workerToScanRanges) {
        // use share scan if `parallelExecInstanceNum * numBackends` is larger than scan ranges.
        return !scanNodes.stream()
                .allMatch(scanNode -> scanNode.ignoreStorageDataDistribution(
                        ConnectContext.get(), workerToScanRanges.size())
                );
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

    protected boolean noEnoughBuckets(Map<Worker, UninstancedScanSource> workerToScanRanges) {
        int parallelExecNum = fragment.getParallelExecNum();
        for (UninstancedScanSource uninstancedScanSource : workerToScanRanges.values()) {
            ScanSource scanSource = uninstancedScanSource.scanSource;
            if (scanSource instanceof BucketScanSource) {
                BucketScanSource bucketScanSource = (BucketScanSource) scanSource;
                int bucketNum = bucketScanSource.bucketIndexToScanNodeToTablets.size();
                if (bucketNum < parallelExecNum) {
                    return true;
                }
            }
        }
        return false;
    }
}
