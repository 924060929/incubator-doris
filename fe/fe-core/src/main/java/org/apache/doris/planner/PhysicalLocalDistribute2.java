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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.implementation.AddLocalExchange.LocalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class PhysicalLocalDistribute2<CHILD_TYPE extends Plan>
        extends PhysicalUnary<CHILD_TYPE> implements PropagateFuncDeps {

    private LocalExchangeType exchangeType;


    // the upstream's physical property saves in base class
    public PhysicalLocalDistribute2(LocalExchangeType exchangeType, CHILD_TYPE child) {
        this(exchangeType, Optional.empty(), child.getLogicalProperties(), child);
    }

    public PhysicalLocalDistribute2(LocalExchangeType exchangeType, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_LOCAL_DISTRIBUTE, groupExpression, logicalProperties, child);
        this.exchangeType = exchangeType;
    }

    public PhysicalLocalDistribute2(LocalExchangeType exchangeType, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_LOCAL_DISTRIBUTE, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.exchangeType = exchangeType;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLocalDistribute2(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public PhysicalLocalDistribute2<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalLocalDistribute2<>(exchangeType, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, children.get(0));

    }

    @Override
    public PhysicalLocalDistribute2<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalLocalDistribute2<>(exchangeType, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalLocalDistribute2<>(exchangeType, groupExpression,
                logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalLocalDistribute2<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalLocalDistribute2<>(exchangeType, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalLocalDistribute2<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalLocalDistribute2<>(exchangeType, groupExpression,
                null, physicalProperties, statistics, child());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder("PhysicalLocalDistribute");
        builder.append("[").append(exchangeType).append("]");
        return builder.toString();
    }

    public enum LocalExchangeType {
        NOOP,
        HASH_SHUFFLE,
        BUCKET_HASH_SHUFFLE,
        PASSTHROUGH,
        ADAPTIVE_PASSTHROUGH,
        BROADCAST,
        PASS_TO_ONE,
        LOCAL_MERGE_SORT
    }
}
