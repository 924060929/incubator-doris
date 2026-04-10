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

package org.apache.doris.planner;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.LocalExchangeNode.RequireHash;

import java.util.List;

/** AddLocalExchange */
public class AddLocalExchange {
    public void addLocalExchange(List<PlanFragment> fragments, PlanTranslatorContext context) {
        for (PlanFragment fragment : fragments) {
            addLocalExchangeForFragment(fragment, context, false);
        }
    }

    /** addLocalExchange with distributed plans, skipping single-instance fragments.
     *  BE's _plan_local_exchange processes all pipelines regardless of _num_instances,
     *  but with _num_instances==1 all pipelines have 1 task so local exchange is a no-op.
     *  Skipping avoids inserting LOCAL_EXCHANGE_NODEs that change pipeline structure
     *  without benefit and may cause sender/receiver count mismatches. */
    public void addLocalExchange(FragmentIdMapping<DistributedPlan> distributedPlans,
            PlanTranslatorContext context) {
        for (DistributedPlan plan : distributedPlans.values()) {
            PipelineDistributedPlan pipePlan = (PipelineDistributedPlan) plan;
            int instanceCount = pipePlan.getInstanceJobs().size();
            if (instanceCount <= 1) {
                continue;
            }
            PlanFragment fragment = pipePlan.getFragmentJob().getFragment();
            boolean isLocalShuffle = pipePlan.getInstanceJobs().stream()
                    .anyMatch(j -> j
                            instanceof org.apache.doris.nereids.trees.plans.distribute
                                    .worker.job.LocalShuffleAssignedJob);
            addLocalExchangeForFragment(fragment, context, isLocalShuffle);
        }
    }

    private void addLocalExchangeForFragment(PlanFragment fragment, PlanTranslatorContext context,
            boolean isLocalShuffle) {
        DataSink sink = fragment.getSink();
        LocalExchangeTypeRequire require = sink == null
                ? LocalExchangeTypeRequire.noRequire() : sink.getLocalExchangeTypeRequire();
        PlanNode root = fragment.getPlanRoot();
        context.setHasSerialAncestorInPipeline(root, false);
        context.setIsLocalShuffleFragment(isLocalShuffle);
        Pair<PlanNode, LocalExchangeType> output = root
                .enforceAndDeriveLocalExchange(context, null, require);
        PlanNode newRoot = output.first;
        // The DataStreamSink runs in the same pipeline as the fragment root.
        // If the root is serial in a pooling-scan fragment, the sink pipeline has 1 task
        // and only instance 0 sends EOS → downstream receivers hang.
        // Insert PASSTHROUGH fan-out to create _num_instances sink tasks.
        //
        // Only check the root node itself (not recursive hasSerialChildren), because
        // after enforceAndDeriveLocalExchange(), any serial children deeper in the tree
        // are already handled by LocalExchangeNodes inserted during the tree walk.
        // Those LocalExchangeNodes create pipeline boundaries with _num_instances tasks.
        if (isLocalShuffle && newRoot.isSerialOperator()) {
            newRoot = new LocalExchangeNode(context.nextPlanNodeId(), newRoot,
                    LocalExchangeType.PASSTHROUGH, null);
        }
        if (newRoot != root) {
            fragment.setPlanRoot(newRoot);
        }
    }

    public static boolean isColocated(PlanNode plan) {
        if (plan instanceof AggregationNode) {
            return ((AggregationNode) plan).isColocate() && isColocated(plan.getChild(0));
        } else if (plan instanceof OlapScanNode) {
            return true;
        } else if (plan instanceof SelectNode) {
            return isColocated(plan.getChild(0));
        } else if (plan instanceof HashJoinNode) {
            return ((HashJoinNode) plan).isColocate()
                    && (isColocated(plan.getChild(0)) || isColocated(plan.getChild(1)));
        } else if (plan instanceof SetOperationNode) {
            if (!((SetOperationNode) plan).isColocate()) {
                return false;
            }
            for (PlanNode child : plan.getChildren()) {
                if (isColocated(child)) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    public static LocalExchangeType resolveExchangeType(LocalExchangeTypeRequire require,
            PlanTranslatorContext translatorContext, PlanNode parent, PlanNode child) {
        // Only generic RequireHash adapts to LOCAL_EXECUTION_HASH_SHUFFLE.
        // Explicit RequireSpecific (GLOBAL_EXECUTION_HASH_SHUFFLE, BUCKET_HASH_SHUFFLE, etc.)
        // must never be degraded — if they appear in an invalid context, the plan is wrong.
        //
        // Always prefer LOCAL_EXECUTION_HASH_SHUFFLE for FE-planned intra-fragment hash exchanges.
        // GLOBAL_EXECUTION_HASH_SHUFFLE requires shuffle_idx_to_instance_idx which may be empty
        // for fragments with non-hash sinks (UNPARTITIONED/MERGE). LOCAL_HASH is always safe
        // since it partitions by local instance count without needing external shuffle maps.
        if (require instanceof RequireHash) {
            return LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE;
        }
        return require.preferType();
    }
}
