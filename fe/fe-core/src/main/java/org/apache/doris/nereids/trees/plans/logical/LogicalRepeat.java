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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Repeat.
 */
public abstract class LogicalRepeat<CHILD_TYPE extends Plan>
        extends LogicalUnary<CHILD_TYPE> implements Repeat {

    public static final String COL_GROUPING_ID = "GROUPING_ID";
    public static final String GROUPING_PREFIX = "GROUPING_PREFIX_";
    // max num of distinct sets in grouping sets clause
    public static final int MAX_GROUPING_SETS_NUM = 64;
    protected final List<Expression> groupByExpressions;
    protected final List<NamedExpression> outputExpressions;
    protected final Set<VirtualSlotReference> virtualSlotRefs;
    protected final List<BitSet> groupingIdList;
    protected final List<Expression> virtualGroupingExprs;
    protected final boolean changedOutput;
    protected final boolean isNormalized;

    /**
     * initial construction method.
     */
    public LogicalRepeat(
            PlanType type,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingIdList = ImmutableList.of();
        VirtualSlotReference virtualSlotReference = new VirtualSlotReference(
                COL_GROUPING_ID, BigIntType.INSTANCE, new ArrayList<>(), false);
        this.virtualSlotRefs = ImmutableSet.of(virtualSlotReference);
        this.virtualGroupingExprs = ImmutableList.of();
        this.changedOutput = false;
        this.isNormalized = false;
    }

    /**
     * Constructor with all parameters.
     */
    public LogicalRepeat(
            PlanType type,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            boolean changedOutput,
            boolean isNormalized,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingIdList = ImmutableList.copyOf(groupingIdList);
        this.virtualSlotRefs = ImmutableSet.copyOf(virtualSlotRefs);
        this.virtualGroupingExprs = ImmutableList.copyOf(virtualGroupingExprs);
        this.changedOutput = changedOutput;
        this.isNormalized = isNormalized;
    }

    public abstract List<List<Expression>> getGroupingSets();

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public List<Expression> getOriginalGroupByExpressions() {
        return groupByExpressions;
    }

    public List<Expression> getGroupByExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(getOriginalGroupByExpressions())
                .addAll(virtualGroupingExprs)
                .build();
    }

    public Set<VirtualSlotReference> getVirtualSlotRefs() {
        return virtualSlotRefs;
    }

    public List<Expression> getVirtualGroupingExprs() {
        return virtualGroupingExprs;
    }

    public List<BitSet> getGroupingIdList() {
        return groupingIdList;
    }

    public boolean hasChangedOutput() {
        return changedOutput;
    }

    public boolean isNormalized() {
        return isNormalized;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalRepeat",
                "outputExpr", outputExpressions,
                "groupingIdList", groupingIdList,
                "virtualSlotRefs", virtualSlotRefs,
                "virtualGroupingExprs", virtualGroupingExprs
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRepeat(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRepeat that = (LogicalRepeat) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(virtualSlotRefs, that.virtualSlotRefs)
                && Objects.equals(groupingIdList, that.groupingIdList)
                && Objects.equals(virtualGroupingExprs, that.virtualGroupingExprs)
                && changedOutput == that.changedOutput
                && isNormalized == that.isNormalized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, virtualSlotRefs,
                groupingIdList, virtualGroupingExprs, changedOutput, isNormalized);
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .addAll(virtualGroupingExprs)
                .build();
    }

    public abstract LogicalRepeat<Plan> replace(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            boolean changedOutput,
            boolean isNormalized);

    public abstract LogicalRepeat<Plan> replaceWithChild(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            boolean changedOutput,
            boolean isNormalized,
            Plan child);

    public abstract List<BitSet> genGroupingIdList(List<Expression> groupingExprs, List<List<Expression>> groupingSets);
}
