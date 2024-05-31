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

package org.apache.doris.nereids.worker;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.worker.job.ScanRanges;
import org.apache.doris.nereids.worker.job.UnassignedJob;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

/** LoadBalanceScanWorkerSelector */
public class LoadBalanceScanWorkerSelector implements ScanWorkerSelector {
    private final BackendWorkerManager workerManager = new BackendWorkerManager();
    private final Map<Worker, WorkerWorkload> workloads = Maps.newLinkedHashMap();

    @Override
    public Map<Worker, Map<ScanNode, ScanRanges>> selectReplicaAndWorkerWithoutBucket(
            UnassignedJob unassignedJob) {
        List<ScanNode> nearStorageScanNodes = unassignedJob.getScanNodes();
        if (nearStorageScanNodes.size() != 1) {
            throw new IllegalStateException("Illegal fragment type, "
                    + "should only contains one OlapScanNode but meet " + nearStorageScanNodes);
        }
        return selectForSingleOlapTable(nearStorageScanNodes.get(0));
    }

    @Override
    public Map<Worker, Map<Integer, Map<ScanNode, ScanRanges>>> selectReplicaAndWorkerWithBucket(
            UnassignedJob unassignedJob) {
        PlanFragment fragment = unassignedJob.getFragment();
        List<ScanNode> scanNodes = unassignedJob.getScanNodes();
        List<OlapScanNode> olapScanNodes = filterOlapScanNodes(scanNodes);

        BiFunction<ScanNode, Integer, List<TScanRangeLocations>> bucketScanRangeSupplier = bucketScanRangeSupplier();
        Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier = bucketBytesSupplier();
        // all are olap scan nodes
        if (!scanNodes.isEmpty() && scanNodes.size() == olapScanNodes.size()) {
            if (olapScanNodes.size() == 1 && fragment.isBucketShuffleJoinInput()) {
                return selectForBucket(unassignedJob, scanNodes, bucketScanRangeSupplier, bucketBytesSupplier);
            } else if (fragment.hasColocatePlanNode()) {
                return selectForBucket(unassignedJob, scanNodes, bucketScanRangeSupplier, bucketBytesSupplier);
            }
        } else if (olapScanNodes.isEmpty() && fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
            return selectForBucket(unassignedJob, scanNodes, bucketScanRangeSupplier, bucketBytesSupplier);
        }
        throw new IllegalStateException(
                "Illegal bucket shuffle join or colocate join in fragment: " + fragment.getFragmentId()
        );
    }

    private BiFunction<ScanNode, Integer, List<TScanRangeLocations>> bucketScanRangeSupplier() {
        return (scanNode, bucketIndex) -> {
            if (scanNode instanceof OlapScanNode) {
                return (List) ((OlapScanNode) scanNode).bucketSeq2locations.get(bucketIndex);
            } else {
                return scanNode.getScanRangeLocations(0);
            }
        };
    }

    private Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier() {
        return scanNode -> {
            if (scanNode instanceof OlapScanNode) {
                return ((OlapScanNode) scanNode).bucketSeq2Bytes;
            } else {
                // just one bucket
                return ImmutableMap.of(0, 0L);
            }
        };
    }

    private Map<Worker, Map<Integer, Map<ScanNode, ScanRanges>>> selectForBucket(
            UnassignedJob unassignedJob, List<ScanNode> olapScanNodes,
            BiFunction<ScanNode, Integer, List<TScanRangeLocations>> bucketScanRangeSupplier,
            Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier) {
        Map<Worker, Map<Integer, Map<ScanNode, ScanRanges>>> assignment = Maps.newLinkedHashMap();

        Map<Integer, Long> bucketIndexToBytes =
                computeEachBucketScanBytes(unassignedJob.getFragment(), olapScanNodes, bucketBytesSupplier);

        ScanNode firstOlapScanNode = olapScanNodes.get(0);
        for (Entry<Integer, Long> kv : bucketIndexToBytes.entrySet()) {
            Integer bucketIndex = kv.getKey();
            long allScanNodeScanBytesInOneBucket = kv.getValue();

            List<TScanRangeLocations> allPartitionTabletsInOneBucketInFirstTable
                    = bucketScanRangeSupplier.apply(firstOlapScanNode, bucketIndex);
            SelectResult replicaAndWorker = selectScanReplicaAndMinWorkloadWorker(
                    allPartitionTabletsInOneBucketInFirstTable.get(0), allScanNodeScanBytesInOneBucket);
            Worker selectedWorker = replicaAndWorker.selectWorker;
            long workerId = selectedWorker.id();
            for (ScanNode olapScanNode : olapScanNodes) {
                List<TScanRangeLocations> allPartitionTabletsInOneBucket
                        = bucketScanRangeSupplier.apply(olapScanNode, bucketIndex);
                List<Pair<TScanRangeParams, Long>> selectedReplicasInOneBucket = filterReplicaByWorkerInBucket(
                                olapScanNode, workerId, bucketIndex, allPartitionTabletsInOneBucket
                );
                Map<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToTablets
                        = assignment.computeIfAbsent(selectedWorker, worker -> Maps.newLinkedHashMap());
                Map<ScanNode, ScanRanges> scanNodeToScanRanges = bucketIndexToScanNodeToTablets
                        .computeIfAbsent(bucketIndex, bucket -> Maps.newLinkedHashMap());
                ScanRanges scanRanges = scanNodeToScanRanges.computeIfAbsent(olapScanNode, node -> new ScanRanges());
                for (Pair<TScanRangeParams, Long> replica : selectedReplicasInOneBucket) {
                    TScanRangeParams replicaParam = replica.first;
                    Long scanBytes = replica.second;
                    scanRanges.addScanRange(replicaParam, scanBytes);
                }
            }
        }
        return assignment;
    }

    private Map<Worker, Map<ScanNode, ScanRanges>> selectForSingleOlapTable(
            ScanNode nearStorageScanNode) {
        Map<Worker, Map<ScanNode, ScanRanges>> workerToScanNodeAndReplicas = Maps.newHashMap();
        List<TScanRangeLocations> allScanTabletLocations = nearStorageScanNode.getScanRangeLocations(0);
        for (TScanRangeLocations onePartitionOneTabletLocation : allScanTabletLocations) {
            long tabletId = 0L; // onePartitionOneTabletLocation.getScanRange().getPaloScanRange().getTabletId();
            Long tabletBytes = 0L; //nearStorageScanNode.getTabletSingleReplicaSize(tabletId);

            SelectResult selectedReplicaAndWorker
                    = selectScanReplicaAndMinWorkloadWorker(onePartitionOneTabletLocation, tabletBytes);
            Worker selectedWorker = selectedReplicaAndWorker.selectWorker;
            TScanRangeLocation selectedReplica = selectedReplicaAndWorker.selectReplica;

            Map<ScanNode, ScanRanges> scanNodeToRanges
                    = workerToScanNodeAndReplicas.computeIfAbsent(selectedWorker, worker -> Maps.newLinkedHashMap());
            ScanRanges selectedReplicas
                    = scanNodeToRanges.computeIfAbsent(nearStorageScanNode, node -> new ScanRanges());
            TScanRangeParams scanReplicaParam = buildScanReplicaParams(onePartitionOneTabletLocation, selectedReplica);
            selectedReplicas.addScanRange(scanReplicaParam, tabletBytes);
        }

        // scan empty table, assign a random worker with empty
        if (workerToScanNodeAndReplicas.isEmpty()) {
            workerToScanNodeAndReplicas.put(
                    workerManager.randomAvailableWorker(),
                    ImmutableMap.of(nearStorageScanNode, new ScanRanges())
            );
        }
        return workerToScanNodeAndReplicas;
    }

    private SelectResult selectScanReplicaAndMinWorkloadWorker(
            TScanRangeLocations tabletLocation, long tabletBytes) {
        List<TScanRangeLocation> replicaLocations = tabletLocation.getLocations();
        int replicaNum = replicaLocations.size();
        WorkerWorkload minWorkload = new WorkerWorkload(Integer.MAX_VALUE, Long.MAX_VALUE);
        Worker minWorkLoadWorker = null;
        TScanRangeLocation selectedReplicaLocation = null;

        for (int i = 0; i < replicaNum; i++) {
            TScanRangeLocation replicaLocation = replicaLocations.get(i);
            Worker worker = workerManager.getWorker(replicaLocation.getBackendId());
            if (!worker.available()) {
                continue;
            }

            WorkerWorkload workload = getWorkload(worker);
            if (workload.compareTo(minWorkload) < 0) {
                minWorkLoadWorker = worker;
                minWorkload = workload;
                selectedReplicaLocation = replicaLocation;
            }
        }
        if (minWorkLoadWorker == null) {
            throw new AnalysisException("No available workers");
        } else {
            minWorkload.recordOneScanTask(tabletBytes);
            return new SelectResult(minWorkLoadWorker, selectedReplicaLocation, minWorkload.scanBytes);
        }
    }

    private List<OlapScanNode> filterOlapScanNodes(List<ScanNode> scanNodes) {
        ImmutableList.Builder<OlapScanNode> olapScanNodes = ImmutableList.builderWithExpectedSize(scanNodes.size());
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                olapScanNodes.add((OlapScanNode) scanNode);
            }
        }
        return olapScanNodes.build();
    }

    private List<Pair<TScanRangeParams, Long>> filterReplicaByWorkerInBucket(
            ScanNode olapScanNode, long filterWorkerId, int bucketIndex,
            List<TScanRangeLocations> allPartitionTabletsInOneBucket) {
        List<Pair<TScanRangeParams, Long>> selectedReplicasInOneBucket = Lists.newArrayList();
        for (TScanRangeLocations onePartitionOneTabletLocation : allPartitionTabletsInOneBucket) {
            TScanRange scanRange = onePartitionOneTabletLocation.getScanRange();
            if (scanRange.getPaloScanRange() != null) {
                long tabletId = scanRange.getPaloScanRange().getTabletId();
                for (TScanRangeLocation replicaLocation : onePartitionOneTabletLocation.getLocations()) {
                    if (replicaLocation.getBackendId() == filterWorkerId) {
                        TScanRangeParams scanReplicaParams =
                                buildScanReplicaParams(onePartitionOneTabletLocation, replicaLocation);
                        Long replicaSize = ((OlapScanNode) olapScanNode).getTabletSingleReplicaSize(tabletId);
                        selectedReplicasInOneBucket.add(Pair.of(scanReplicaParams, replicaSize));
                        break;
                    }
                }
                throw new IllegalStateException("Can not find tablet " + tabletId + " in the bucket: " + bucketIndex);
            } else if (onePartitionOneTabletLocation.getLocations().size() == 1) {
                TScanRangeLocation replicaLocation = onePartitionOneTabletLocation.getLocations().get(0);
                TScanRangeParams scanReplicaParams =
                        buildScanReplicaParams(onePartitionOneTabletLocation, replicaLocation);
                Long replicaSize = 0L;
                selectedReplicasInOneBucket.add(Pair.of(scanReplicaParams, replicaSize));
            } else {
                throw new IllegalStateException("Unsupported");
            }
        }
        return selectedReplicasInOneBucket;
    }

    private Map<Integer, Long> computeEachBucketScanBytes(
            PlanFragment fragment, List<ScanNode> olapScanNodes,
            Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier) {
        Map<Integer, Long> bucketIndexToBytes = Maps.newLinkedHashMap();
        for (ScanNode olapScanNode : olapScanNodes) {
            Map<Integer, Long> bucketSeq2Bytes = bucketBytesSupplier.apply(olapScanNode);
            // Set<Entry<Integer, Long>> bucketSeq2Bytes = olapScanNode.bucketSeq2Bytes.entrySet();
            if (!bucketIndexToBytes.isEmpty() && bucketIndexToBytes.size() != bucketSeq2Bytes.size()) {
                throw new IllegalStateException("Illegal fragment " + fragment.getFragmentId()
                        + ", every OlapScanNode should has same bucket num");
            }

            for (Entry<Integer, Long> bucketSeq2Byte : bucketSeq2Bytes.entrySet()) {
                Integer bucketIndex = bucketSeq2Byte.getKey();
                Long scanBytes = bucketSeq2Byte.getValue();
                bucketIndexToBytes.merge(bucketIndex, scanBytes, Long::sum);
            }
        }
        return bucketIndexToBytes;
    }

    private TScanRangeParams buildScanReplicaParams(
            TScanRangeLocations tabletLocation, TScanRangeLocation replicaLocation) {
        TScanRangeParams replicaParam = new TScanRangeParams();
        replicaParam.scan_range = tabletLocation.scan_range;
        // Volume is optional, so we need to set the value and the is-set bit
        replicaParam.setVolumeId(replicaLocation.volume_id);
        return replicaParam;
    }

    private WorkerWorkload getWorkload(Worker worker) {
        return workloads.computeIfAbsent(worker, w -> new WorkerWorkload());
    }

    private static class SelectResult {
        private final Worker selectWorker;
        private final TScanRangeLocation selectReplica;
        private final long bytes;

        public SelectResult(Worker selecteWorker, TScanRangeLocation selectReplica, long bytes) {
            this.selectWorker = selecteWorker;
            this.selectReplica = selectReplica;
            this.bytes = bytes;
        }
    }

    private static class WorkerWorkload implements Comparable<WorkerWorkload> {
        private int taskNum;
        private long scanBytes;

        public WorkerWorkload() {
            this(0, 1);
        }

        public WorkerWorkload(int taskNum, long scanBytes) {
            this.taskNum = taskNum;
            this.scanBytes = scanBytes;
        }

        public void recordOneScanTask(long scanBytes) {
            this.scanBytes += scanBytes;
        }

        // order by scanBytes asc, taskNum asc
        @Override
        public int compareTo(WorkerWorkload workerWorkload) {
            int compareScanBytes = Long.compare(this.scanBytes, workerWorkload.scanBytes);
            if (compareScanBytes != 0) {
                return compareScanBytes;
            }
            return taskNum - workerWorkload.taskNum;
        }
    }
}
