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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * PhysicalRepeat.
 */
public abstract class PhysicalRepeat<CHILD_TYPE extends Plan>
        extends PhysicalUnary<CHILD_TYPE> implements Repeat {
    protected final List<Expression> groupByExpressions;
    protected final List<Expression> originGroupByExpressions;
    protected final List<NamedExpression> outputExpressions;
    protected final List<BitSet> groupingIdList;
    protected final List<Expression> virtualGroupByExpressions;

    /**
     * initial construction method.
     */
    public PhysicalRepeat(
            PlanType type,
            List<Expression> groupByExpressions,
            List<Expression> originGroupByExpressions,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            List<Expression> virtualGroupByExpressions,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.originGroupByExpressions = ImmutableList.copyOf(originGroupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingIdList = ImmutableList.copyOf(groupingIdList);
        this.virtualGroupByExpressions = ImmutableList.copyOf(virtualGroupByExpressions);
    }

    /**
     * Constructor with all parameters.
     */
    public PhysicalRepeat(
            PlanType type,
            List<Expression> groupByExpressions,
            List<Expression> originGroupByExpressions,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            List<Expression> virtualGroupByExpressions,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, physicalProperties, statsDeriveResult, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.originGroupByExpressions = ImmutableList.copyOf(originGroupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingIdList = ImmutableList.copyOf(groupingIdList);
        this.virtualGroupByExpressions = ImmutableList.copyOf(virtualGroupByExpressions);
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public List<Expression> getVirtualGroupByExpressions() {
        return virtualGroupByExpressions;
    }

    public List<BitSet> getGroupingIdList() {
        return groupingIdList;
    }

    public List<Expression> getOriginGroupByExpressions() {
        return originGroupByExpressions;
    }

    @Override
    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalRepeat",
                "groupByExpressions", groupByExpressions,
                "originGroupByExpressions", originGroupByExpressions,
                "outputExpr", outputExpressions,
                "groupingIdList", groupingIdList,
                "virtualGroupByExpressions", virtualGroupByExpressions
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRepeat(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRepeat that = (PhysicalRepeat) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(originGroupByExpressions, that.originGroupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(groupingIdList, that.groupingIdList)
                && Objects.equals(virtualGroupByExpressions, that.virtualGroupByExpressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, originGroupByExpressions, outputExpressions,
                groupingIdList, virtualGroupByExpressions);
    }
}
