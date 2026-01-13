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

import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.rules.implementation.AddLocalShuffle.HashDistribution.HashMethod;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/** AddLocalShuffle */
public class AddLocalShuffle extends PlanPostProcessor {
    private static LocalPropertiesDeriver DERIVER = new LocalPropertiesDeriver();

    // @Override
    // public Plan processRoot(Plan plan, CascadesContext ctx) {
    //     // plan.rewriteUp(p -> {
    //     //     deriveAndEnforce()
    //     // })
    // }
    //
    // @Override
    // public Plan visit(Plan plan, CascadesContext context) {
    //     plan = super.visit(plan, context);
    //
    //     deriveAndEnforce()
    // }

    @Override
    public Plan visitPhysicalOlapScan(PhysicalOlapScan olapScan, CascadesContext context) {
        return super.visitPhysicalOlapScan(olapScan, context);
    }

    private Plan deriveAndEnforce(
            RequireLocalProperties parentRequire, PhysicalPlan plan, List<LocalProperties> inputProperties) {

        LocalProperties outputProperties = derive(plan, inputProperties);
        if (!parentRequire.satisfy(outputProperties)) {
            LocalProperties localProperties = parentRequire.enforce(plan, outputProperties);
            return new PhysicalLocalDistribute<>(localProperties, plan);
        }
        return plan;
    }

    private LocalProperties derive(PhysicalPlan plan, List<LocalProperties> inputs) {
        return plan.accept(DERIVER, inputs);
    }

    /** LocalPropertiesDeriver */
    private static class LocalPropertiesDeriver extends DefaultPlanVisitor<LocalProperties, List<LocalProperties>> {
        @Override
        public LocalProperties visit(Plan plan, List<LocalProperties> context) {
            // default output any distribution, means unknown distribution
            return new LocalProperties(AnyDistribution.INSTANCE, Parallelism.DEFAULT);
        }

        @Override
        public LocalProperties visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation,
                List<LocalProperties> context) {
            return new LocalProperties(AnyDistribution.INSTANCE, Parallelism.SINGLE);
        }

        @Override
        public LocalProperties visitPhysicalHashAggregate(
                PhysicalHashAggregate<? extends Plan> agg, List<LocalProperties> input) {
            if (agg.getAggMode().productAggregateBuffer) {
                return new LocalProperties(AnyDistribution.INSTANCE, Parallelism.SINGLE);
            }

            List<Expression> groupByExpressions = agg.getGroupByExpressions();
            if (groupByExpressions.isEmpty()) {
                return new LocalProperties(AnyDistribution.INSTANCE, Parallelism.SINGLE);
            }

            List<ExprId> exprIds = new ArrayList<>();
            for (Expression groupByExpression : groupByExpressions) {
                exprIds.add(((NamedExpression) groupByExpression).getExprId());
            }

            HashMethod hashMethod = HashMethod.EXECUTION_HASH;
            LocalProperties inputProperties = input.get(0);
            if (inputProperties.distribution instanceof HashDistribution) {
                hashMethod = ((HashDistribution) inputProperties.distribution).hashMethod;
            }

            return new LocalProperties(new HashDistribution(hashMethod, exprIds), Parallelism.MULTIPLE);
        }

        @Override
        public LocalProperties visitPhysicalOlapScan(PhysicalOlapScan olapScan, List<LocalProperties> context) {
            OlapTable table = olapScan.getTable();
            DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
            if (distributionInfo.getType() != DistributionInfoType.HASH || table.isAutoBucket()) {
                return super.visitPhysicalOlapScan(olapScan, context);
            }

            int bucketNum = distributionInfo.getBucketNum();
            Parallelism parallelism = bucketNum == 1 ? Parallelism.SINGLE : Parallelism.MULTIPLE;
            DistributionSpecHash hashDistribution = (DistributionSpecHash) olapScan.getDistributionSpec();
            List<ExprId> orderedShuffledColumns = hashDistribution.getOrderedShuffledColumns();
            return new LocalProperties(
                    new HashDistribution(HashMethod.STORAGE_HASH, ImmutableList.copyOf(orderedShuffledColumns)),
                    parallelism
            );
        }
    }

    /** RequireLocalProperties */
    private static class RequireLocalProperties {
        public boolean satisfy(LocalProperties localProperties) {
            return false;
        }

        public LocalProperties enforce(PhysicalPlan plan, LocalProperties outputProperties) {
            return null;
        }
    }

    /** LocalProperties */
    public static class LocalProperties {
        public final LocalDistribution distribution;
        public final Parallelism parallelism;

        public LocalProperties(LocalDistribution distribution, Parallelism parallelism) {
            this.distribution = distribution;
            this.parallelism = parallelism;
        }
    }

    /** LocalDistribution */
    public interface LocalDistribution {}

    /**
     * AnyDistribution: unknown or any distribution
     */
    public static class AnyDistribution implements LocalDistribution {
        public static final AnyDistribution INSTANCE = new AnyDistribution();

        private AnyDistribution() {}
    }

    /** HashDistribution */
    public static class HashDistribution implements LocalDistribution {
        public final HashMethod hashMethod;
        public final List<ExprId> hashExprIds;

        public HashDistribution(HashMethod hashMethod, List<ExprId> hashExprIds) {
            this.hashMethod = hashMethod;
            this.hashExprIds = hashExprIds;
        }

        /** HashMethod */
        public enum HashMethod {
            EXECUTION_HASH, STORAGE_HASH
        }
    }

    /** Parallelism */
    public enum Parallelism {
        DEFAULT, SINGLE, MULTIPLE
    }
}
