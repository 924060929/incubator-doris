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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.planner.PhysicalLocalDistribute2;
import org.apache.doris.planner.PhysicalLocalDistribute2.LocalExchangeType;

/** AddLocalExchange */
public class AddLocalExchange2 extends PlanPostProcessor {
    private static final String DISTRIBUTE_FOLLOW_STORAGE = "distribute_follow_storage";

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        computeDistributeFollowStorage(plan);
        return super.processRoot(plan, ctx);
    }

    private boolean computeDistributeFollowStorage(Plan plan) {
        boolean childDistributeFollowStorage = false;
        for (Plan child : plan.children()) {
            if (child instanceof PhysicalOlapScan) {
                child.setMutableState(DISTRIBUTE_FOLLOW_STORAGE, true);
                childDistributeFollowStorage = true;
            } else {
                childDistributeFollowStorage |= computeDistributeFollowStorage(child);
            }
        }
        if (plan instanceof PhysicalDistribute) {
            return false;
        } else {
            return childDistributeFollowStorage;
        }
    }

    // private static class LocalExchangeEnforcer extends DefaultPlanVisitor<PhysicalPlan, LocalExchangeRequire> {
    //     @Override
    //     public PhysicalPlan visitPhysicalOlapScan(PhysicalOlapScan olapScan, LocalExchangeRequire required) {
    //
    //     }
    //
    //     private boolean isPoolingScan() {
    //
    //     }
    //
    //     private boolean isColocateFragment(Plan plan) {
    //         if (plan instanceof PhysicalOlapScan) {
    //             return true;
    //         } else if (plan instanceof PhysicalDistribute) {
    //             return false;
    //         } else {
    //             for (Plan child : plan.children()) {
    //                 if (isColocateFragment(child)) {
    //                     return true;
    //                 }
    //             }
    //             return false;
    //         }
    //     }
    //
    //     private PhysicalPlan enforce(PhysicalPlan current, LocalExchangeType require, LocalExchangeType provide) {
    //         switch (require) {
    //             case NOOP:
    //                 return current;
    //             case HASH_SHUFFLE:
    //             case BUCKET_HASH_SHUFFLE:
    //                 if (provide == LocalExchangeType.HASH_SHUFFLE
    //                         || provide == LocalExchangeType.BUCKET_HASH_SHUFFLE) {
    //                     return current;
    //                 }
    //             default:
    //                 return new PhysicalLocalDistribute2<>(require, current);
    //         }
    //     }
    // }

    private static class LocalExchangeRequire {
        public final PhysicalPlan parent;
        public final LocalExchangeType requiredType;

        public LocalExchangeRequire(PhysicalPlan parent, LocalExchangeType requiredType) {
            this.parent = parent;
            this.requiredType = requiredType;
        }
    }
}
