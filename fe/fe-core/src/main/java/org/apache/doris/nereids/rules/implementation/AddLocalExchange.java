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
import org.apache.doris.nereids.rules.implementation.AddLocalExchange.HashDistribution.HashMethod;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** AddLocalExchange */
public class AddLocalExchange extends PlanPostProcessor {
    private static final OutputLocalPropertiesDeriver LOCAL_PROPERTIES_DERIVER = new OutputLocalPropertiesDeriver();
    private static final LocalExchangeEnforcer LOCAL_EXCHANGE_ENFORCER = new LocalExchangeEnforcer();

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        try {
            PhysicalPlan result = plan.accept(LOCAL_EXCHANGE_ENFORCER, LocalPropertiesRequire.noRequire());
            if (result == null) {
                return plan;
            }
            return result;
        } catch (Throwable t) {
            return plan;
        }
    }

    @Override
    public Plan visitPhysicalOlapScan(PhysicalOlapScan olapScan, CascadesContext context) {
        return super.visitPhysicalOlapScan(olapScan, context);
    }

    private static class LocalExchangeEnforcer
            extends DefaultPlanVisitor<PhysicalPlan, LocalPropertiesRequire> {

        @Override
        public PhysicalPlan visitPhysicalOlapScan(
                PhysicalOlapScan olapScan, LocalPropertiesRequire parentRequire) {
            return deriveAndEnforce(parentRequire, olapScan, ImmutableList.of());
        }

        private PhysicalPlan deriveAndEnforce(
                LocalPropertiesRequire parentRequire, PhysicalPlan plan, List<LocalProperties> inputProperties) {
            LocalProperties scanProperties = deriveLogicalProperties(plan, inputProperties);
            if (!parentRequire.satisfy(scanProperties)) {
                LocalProperties localProperties = parentRequire.enforce(plan, scanProperties);
                return new PhysicalLocalDistribute<>(localProperties, plan);
            }
            return plan;
        }

        private LocalProperties deriveLogicalProperties(PhysicalPlan plan, List<LocalProperties> inputs) {
            return plan.accept(LOCAL_PROPERTIES_DERIVER, inputs);
        }
    }

    /** LocalPropertiesDeriver */
    private static class OutputLocalPropertiesDeriver
            extends DefaultPlanVisitor<LocalProperties, List<LocalProperties>> {
        @Override
        public LocalProperties visit(Plan plan, List<LocalProperties> context) {
            // default output any distribution, means unknown distribution
            return new LocalProperties(RandomDistribution.INSTANCE, Parallelism.DEFAULT);
        }

        @Override
        public LocalProperties visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation,
                List<LocalProperties> context) {
            return new LocalProperties(RandomDistribution.INSTANCE, Parallelism.SINGLE);
        }

        @Override
        public LocalProperties visitPhysicalHashAggregate(
                PhysicalHashAggregate<? extends Plan> agg, List<LocalProperties> input) {
            if (agg.getAggMode().productAggregateBuffer) {
                return new LocalProperties(RandomDistribution.INSTANCE, Parallelism.SINGLE);
            }

            List<Expression> groupByExpressions = agg.getGroupByExpressions();
            if (groupByExpressions.isEmpty()) {
                return new LocalProperties(RandomDistribution.INSTANCE, Parallelism.SINGLE);
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

    /** LocalPropertiesRequire */
    public static class LocalPropertiesRequire {
        public final LocalDistributionRequire distributionRequire;
        public final ParallelismRequire parallelismRequire;

        public LocalPropertiesRequire(
                LocalDistributionRequire distributionRequire, ParallelismRequire parallelismRequire) {
            this.distributionRequire = distributionRequire;
            this.parallelismRequire = parallelismRequire;
        }

        public boolean satisfy(LocalProperties localProperties) {
            return distributionRequire.satisfy(localProperties.distribution)
                    && parallelismRequire.satisfy(localProperties.parallelism);
        }

        public LocalProperties enforce(PhysicalPlan plan, LocalProperties localProperties) {
            return null;
        }

        public static LocalPropertiesRequire noRequire() {
            return new LocalPropertiesRequire(AnyLocalDistributionRequire.INSTANCE, AnyParallelismRequire.INSTANCE);
        }

        public static AnyLocalDistributionRequire anyDistribution() {
            return AnyLocalDistributionRequire.INSTANCE;
        }

        public static AnyParallelismRequire anyParallelism() {
            return AnyParallelismRequire.INSTANCE;
        }
    }

    public static class LocalDistributionRequireBuilder {
        private LocalDistributionRequire distributionRequire = AnyLocalDistributionRequire.INSTANCE;
        private ParallelismRequire parallelismRequire = AnyParallelismRequire.INSTANCE;

        public static LocalDistributionRequireBuilder builder() {
            return new LocalDistributionRequireBuilder();
        }

        public LocalDistributionRequireBuilder requireDistribution(LocalDistributionRequire distributionRequire) {
            this.distributionRequire = distributionRequire;
            return this;
        }

        public LocalDistributionRequireBuilder requireRandomDistribution() {
            this.distributionRequire = new SpecificDistributionRequire(RandomDistribution.INSTANCE);
            return this;
        }

        public LocalDistributionRequireBuilder requireExecutionHash(List<ExprId> orderedShuffledColumns) {
            this.distributionRequire = new SpecificDistributionRequire(new HashDistribution(
                    HashMethod.EXECUTION_HASH,
                    orderedShuffledColumns
            ));
            return this;
        }

        public LocalDistributionRequireBuilder requireStorageHash(List<ExprId> orderedShuffledColumns) {
            this.distributionRequire = new SpecificDistributionRequire(new HashDistribution(
                    HashMethod.STORAGE_HASH,
                    orderedShuffledColumns
            ));
            return this;
        }

        public LocalDistributionRequireBuilder requireParallelism(ParallelismRequire parallelismRequire) {
            this.parallelismRequire = parallelismRequire;
            return this;
        }

        public LocalPropertiesRequire build() {
            return new LocalPropertiesRequire(distributionRequire, parallelismRequire);
        }
    }

    /** LocalDistributionRequire */
    public interface LocalDistributionRequire {
        boolean satisfy(LocalDistribution distribution);
    }

    /** AnyLocalDistributionRequire */
    public static class AnyLocalDistributionRequire implements LocalDistributionRequire {
        public static final AnyLocalDistributionRequire INSTANCE = new AnyLocalDistributionRequire();

        private AnyLocalDistributionRequire() {}

        @Override
        public boolean satisfy(LocalDistribution distribution) {
            return true;
        }
    }

    public static class SpecificDistributionRequire implements LocalDistributionRequire {
        private final LocalDistribution distribution;

        public SpecificDistributionRequire(LocalDistribution distribution) {
            this.distribution = distribution;
        }

        public LocalDistribution getDistribution() {
            return distribution;
        }

        @Override
        public boolean satisfy(LocalDistribution distribution) {
            return this.distribution.equals(distribution);
        }
    }

    /** LocalDistribution */
    public interface LocalDistribution {}

    /**
     * RandomDistribution: unknown or any distribution
     */
    public static class RandomDistribution implements LocalDistribution {
        public static final RandomDistribution INSTANCE = new RandomDistribution();

        private RandomDistribution() {}
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

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HashDistribution that = (HashDistribution) o;
            return hashMethod == that.hashMethod && Objects.equals(hashExprIds, that.hashExprIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashMethod, hashExprIds);
        }
    }

    /** ParallelismRequire */
    public interface ParallelismRequire {
        boolean satisfy(Parallelism parallelism);
    }

    /** Parallelism */
    public enum Parallelism {
        DEFAULT, SINGLE, MULTIPLE
    }

    /** AnyParallelismRequire */
    public static class AnyParallelismRequire implements ParallelismRequire {
        public static final AnyParallelismRequire INSTANCE = new AnyParallelismRequire();

        private AnyParallelismRequire() {}

        @Override
        public boolean satisfy(Parallelism parallelism) {
            return true;
        }
    }

    /** SpecificParallelismRequire */
    public static class SpecificParallelismRequire implements ParallelismRequire {
        public final Parallelism parallelism;

        public SpecificParallelismRequire(Parallelism parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public boolean satisfy(Parallelism parallelism) {
            return this.parallelism.equals(parallelism);
        }
    }
}
