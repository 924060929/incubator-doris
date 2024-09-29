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
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/** DistributePlanner */
public class DistributePlanner {
    private final List<PlanFragment> fragments;
    private final FragmentIdMapping<PlanFragment> idToFragments;

    public DistributePlanner(List<PlanFragment> fragments) {
        this.fragments = Objects.requireNonNull(fragments, "fragments can not be null");
        this.idToFragments = FragmentIdMapping.buildFragmentMapping(fragments);
    }

    public FragmentIdMapping<DistributedPlan> plan() {
        FragmentIdMapping<UnassignedJob> fragmentJobs = UnassignedJobBuilder.buildJobs(idToFragments);
        ListMultimap<PlanFragmentId, AssignedJob> instanceJobs = AssignedJobBuilder.buildJobs(fragmentJobs);
        FragmentIdMapping<DistributedPlan> plans = buildDistributePlans(fragmentJobs, instanceJobs);
        return linkPlans(plans);
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
        for (DistributedPlan plan : plans.values()) {
            for (DistributedPlan inputPlan : plan.getInputs().values()) {
                linkPipelinePlan((PipelineDistributedPlan) plan, (PipelineDistributedPlan) inputPlan);
            }
        }
        return plans;
    }

    private void linkPipelinePlan(PipelineDistributedPlan receiverPlan, PipelineDistributedPlan senderPlan) {
        boolean useLocalShuffle = receiverPlan.getInstanceJobs().stream()
                .anyMatch(LocalShuffleAssignedJob.class::isInstance);
        boolean isBucketShuffleJoinSide = receiverPlan.getFragmentJob() instanceof UnassignedScanBucketOlapTableJob;

        List<AssignedJob> receiverInstances;
        if (useLocalShuffle) {
            receiverInstances = getFirstInstancePerShareScan(receiverPlan);
        } else {
            receiverInstances = receiverPlan.getInstanceJobs();
        }

        if (isBucketShuffleJoinSide) {
            linkBucketShuffleJoinPlan(receiverPlan, senderPlan, receiverInstances);
        } else {
            senderPlan.setDestinations(receiverPlan.getInstanceJobs());
        }
    }

    private void linkBucketShuffleJoinPlan(
            PipelineDistributedPlan joinSide, PipelineDistributedPlan shuffleSide, List<AssignedJob> receiverInstances) {
        UnassignedScanBucketOlapTableJob bucketJob = (UnassignedScanBucketOlapTableJob) joinSide.getFragmentJob();
        int bucketNum = bucketJob.getOlapScanNodes().get(0).getBucketNum();
        List<AssignedJob> instancePerBucket = sortInstanceByBuckets(joinSide, receiverInstances, bucketNum);
        shuffleSide.setDestinations(instancePerBucket);
    }

    private List<AssignedJob> sortInstanceByBuckets(
            PipelineDistributedPlan plan, List<AssignedJob> unsorted, int bucketNum) {
        AssignedJob[] instances = new AssignedJob[bucketNum];
        for (AssignedJob instanceJob : unsorted) {
            BucketScanSource bucketScanSource = (BucketScanSource) instanceJob.getScanSource();
            for (Integer bucketIndex : bucketScanSource.bucketIndexToScanNodeToTablets.keySet()) {
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
        Map<Integer, AssignedJob> distinctShareScanJobs = Maps.newLinkedHashMap();
        for (AssignedJob instanceJob : plan.getInstanceJobs()) {
            LocalShuffleAssignedJob localShuffleJob = (LocalShuffleAssignedJob) instanceJob;
            distinctShareScanJobs.putIfAbsent(localShuffleJob.shareScanId, localShuffleJob);
        }
        return Utils.fastToImmutableList(distinctShareScanJobs.values());
    }
}
