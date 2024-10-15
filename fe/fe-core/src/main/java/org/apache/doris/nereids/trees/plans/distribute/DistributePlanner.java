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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DummyWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJobBuilder;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.StaticAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJobBuilder;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedScanBucketOlapTableJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** DistributePlanner */
public class DistributePlanner {
    private final NereidsPlanner planner;
    private final CascadesContext cascadesContext;
    private final FragmentIdMapping<PlanFragment> idToFragments;

    public DistributePlanner(NereidsPlanner planner, List<PlanFragment> fragments) {
        this.planner = Objects.requireNonNull(planner, "planner can not be null");
        this.cascadesContext = planner.getCascadesContext();
        this.idToFragments = FragmentIdMapping.buildFragmentMapping(fragments);
    }

    public FragmentIdMapping<DistributedPlan> plan() {
        FragmentIdMapping<UnassignedJob> fragmentJobs = UnassignedJobBuilder.buildJobs(planner, idToFragments);
        ListMultimap<PlanFragmentId, AssignedJob> instanceJobs = AssignedJobBuilder.buildJobs(fragmentJobs);
        FragmentIdMapping<DistributedPlan> distributedPlans = buildDistributePlans(fragmentJobs, instanceJobs);
        return linkPlans(distributedPlans);
    }

    private FragmentIdMapping<DistributedPlan> buildDistributePlans(
            Map<PlanFragmentId, UnassignedJob> idToUnassignedJobs,
            ListMultimap<PlanFragmentId, AssignedJob> idToAssignedJobs) {
        FragmentIdMapping<PipelineDistributedPlan> idToDistributedPlans = new FragmentIdMapping<>();
        for (Entry<PlanFragmentId, PlanFragment> kv : idToFragments.entrySet()) {
            PlanFragmentId fragmentId = kv.getKey();
            PlanFragment fragment = kv.getValue();

            UnassignedJob fragmentJob = idToUnassignedJobs.get(fragmentId);
            List<AssignedJob> instanceJobs = idToAssignedJobs.get(fragmentId);

            ListMultimap<ExchangeNode, DistributedPlan> exchangeNodeToChildren = ArrayListMultimap.create();
            for (PlanFragment childFragment : fragment.getChildren()) {
                exchangeNodeToChildren.put(
                        childFragment.getDestNode(),
                        idToDistributedPlans.get(childFragment.getFragmentId())
                );
            }

            idToDistributedPlans.put(fragmentId,
                    new PipelineDistributedPlan(fragmentJob, instanceJobs, exchangeNodeToChildren)
            );
        }
        return (FragmentIdMapping) idToDistributedPlans;
    }

    private FragmentIdMapping<DistributedPlan> linkPlans(FragmentIdMapping<DistributedPlan> plans) {
        boolean enableShareHashTableForBroadcastJoin = cascadesContext.getConnectContext()
                .getSessionVariable()
                .enableShareHashTableForBroadcastJoin;
        for (DistributedPlan receiverPlan : plans.values()) {
            for (Entry<ExchangeNode, DistributedPlan> link : receiverPlan.getInputs().entries()) {
                linkPipelinePlan(
                        (PipelineDistributedPlan) receiverPlan,
                        (PipelineDistributedPlan) link.getValue(),
                        link.getKey(),
                        enableShareHashTableForBroadcastJoin
                );
            }
        }
        return plans;
    }

    // set shuffle destinations
    private void linkPipelinePlan(
            PipelineDistributedPlan receiverPlan,
            PipelineDistributedPlan senderPlan,
            ExchangeNode linkNode,
            boolean enableShareHashTableForBroadcastJoin) {
        List<AssignedJob> receiverInstances = filterInstancesWhichCanReceiveDataFromRemote(
                receiverPlan, enableShareHashTableForBroadcastJoin, linkNode);

        boolean receiveSideIsBucketShuffleJoinSide
                = receiverPlan.getFragmentJob() instanceof UnassignedScanBucketOlapTableJob;
        if (receiveSideIsBucketShuffleJoinSide) {
            receiverInstances = getDestinationsByBuckets(receiverPlan, receiverInstances);
        }
        senderPlan.setDestinations(receiverInstances);
    }

    private List<AssignedJob> getDestinationsByBuckets(
            PipelineDistributedPlan joinSide,
            List<AssignedJob> receiverInstances) {
        UnassignedScanBucketOlapTableJob bucketJob = (UnassignedScanBucketOlapTableJob) joinSide.getFragmentJob();
        int bucketNum = bucketJob.getOlapScanNodes().get(0).getBucketNum();
        return sortDestinationInstancesByBuckets(joinSide, receiverInstances, bucketNum);
    }

    private List<AssignedJob> filterInstancesWhichCanReceiveDataFromRemote(
            PipelineDistributedPlan receiverPlan,
            boolean enableShareHashTableForBroadcastJoin,
            ExchangeNode linkNode) {
        boolean useLocalShuffle = receiverPlan.getInstanceJobs().stream()
                .anyMatch(LocalShuffleAssignedJob.class::isInstance);
        if (useLocalShuffle) {
            return getFirstInstancePerShareScan(receiverPlan);
        } else if (enableShareHashTableForBroadcastJoin && linkNode.isRightChildOfBroadcastHashJoin()) {
            return getFirstInstancePerWorker(receiverPlan.getInstanceJobs());
        } else {
            return receiverPlan.getInstanceJobs();
        }
    }

    private List<AssignedJob> sortDestinationInstancesByBuckets(
            PipelineDistributedPlan plan, List<AssignedJob> unsorted, int bucketNum) {
        AssignedJob[] instances = new AssignedJob[bucketNum];
        for (AssignedJob instanceJob : unsorted) {
            BucketScanSource bucketScanSource = (BucketScanSource) instanceJob.getScanSource();
            for (Integer bucketIndex : bucketScanSource.bucketIndexToScanNodeToTablets.keySet()) {
                if (instances[bucketIndex] != null) {
                    throw new IllegalStateException(
                            "Multi instances scan same buckets: " + instances[bucketIndex] + " and " + instanceJob
                    );
                }
                instances[bucketIndex] = instanceJob;
            }
        }

        for (int i = 0; i < instances.length; i++) {
            if (instances[i] == null) {
                instances[i] = new StaticAssignedJob(
                        i,
                        new TUniqueId(-1, -1),
                        plan.getFragmentJob(),
                        DummyWorker.INSTANCE,
                        new DefaultScanSource(ImmutableMap.of())
                );
            }
        }
        return Arrays.asList(instances);
    }

    private List<AssignedJob> getFirstInstancePerShareScan(PipelineDistributedPlan plan) {
        List<AssignedJob> canReceiveDataFromRemote = Lists.newArrayListWithCapacity(plan.getInstanceJobs().size());
        for (AssignedJob instanceJob : plan.getInstanceJobs()) {
            LocalShuffleAssignedJob localShuffleJob = (LocalShuffleAssignedJob) instanceJob;
            if (!localShuffleJob.receiveDataFromLocal) {
                canReceiveDataFromRemote.add(localShuffleJob);
            }
        }
        return canReceiveDataFromRemote;
    }

    private List<AssignedJob> getFirstInstancePerWorker(List<AssignedJob> instances) {
        Map<DistributedPlanWorker, AssignedJob> firstInstancePerWorker = instances.stream()
                .collect(Collectors.toMap(
                        AssignedJob::getAssignedWorker,
                        Function.identity(),
                        (firstInstance, otherInstance) -> firstInstance)
                );
        return Utils.fastToImmutableList(firstInstancePerWorker.values());
    }
}
