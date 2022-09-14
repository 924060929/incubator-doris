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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * normalize aggregate's group keys and AggregateFunction's child to SlotReference
 * and generate a LogicalProject top on LogicalAggregate to hold to order of aggregate output,
 * since aggregate output's order could change when we do translate.
 * <p>
 * Apply this rule could simplify the processing of enforce and translate.
 * <pre>
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, K2#2 + 1],
 *   outputs:[k1#1, Alias(K2# + 1)#4, Alias(k1#1 + 1)#5, Alias(SUM(v1#3))#6,
 *            Alias(SUM(v1#3 + 1))#7, Alias(SUM(v1#3) + 1)#8])
 * </pre>
 * After rule:
 * Project(k1#1, Alias(SR#9)#4, Alias(k1#1 + 1)#5, Alias(SR#10))#6, Alias(SR#11))#7, Alias(SR#10 + 1)#8)
 * +-- Aggregate(keys:[k1#1, SR#9], outputs:[k1#1, SR#9, Alias(SUM(v1#3))#10, Alias(SUM(v1#3 + 1))#11])
 * +-- Project(k1#1, Alias(K2#2 + 1)#9, v1#3)
 * <p>
 * More example could get from UT {NormalizeAggregateTest}
 */
public class NormalizeAggregate extends OneRewriteRuleFactory implements NormalizePlan {
    @Override
    public Rule build() {
        return logicalAggregate().whenNot(LogicalAggregate::isNormalized).then(aggregate -> {
            List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
            Map<Expression, Alias> groupByNewAlias = genAliasChildToSlotForGroupBy(groupByExpressions);

            List<NamedExpression> outputs = aggregate.getOutputExpressions();
            Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                    .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)
                            || e.anyMatch(GroupingScalarFunction.class::isInstance)));
            Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                    .flatMap(e -> e.<Set<AggregateFunction>>collect(
                            AggregateFunction.class::isInstance).stream())
                    .collect(Collectors.toSet());
            Map<Expression, Alias> aggFuncInnerAlias = genInnerAliasForAggFunc(aggregateFunctions);
            Map<Expression, Alias> aggFuncOuterAlias = genOuterAliasForAggFunc(partitionedOutputs, aggFuncInnerAlias);

            // 1. generate new groupByExpression
            List<Expression> newGroupByExpressions = generateNewGroupByExpressions(
                    groupByExpressions, groupByNewAlias);

            // 2. generate NewVirSlotRef
            // For groupingFunc, you need to replace the real column corresponding to its virtual column.
            List<Pair<Expression, VirtualSlotReference>> newVirSlotRef =
                    genNewVirSlotRef(partitionedOutputs, groupByExpressions, groupByNewAlias);

            // 3. generate new substituteMap
            // substitution map used to substitute expression in repeat's output to use it as top projections
            Map<Expression, Expression> substitutionMap =
                    genSubstitutionMap(groupByExpressions, groupByNewAlias,
                            partitionedOutputs, aggFuncInnerAlias, aggFuncOuterAlias, newVirSlotRef);

            // 4. generate new outputs
            List<NamedExpression> newOutputs =
                    genNewOutputs(groupByExpressions, outputs, substitutionMap,
                            newVirSlotRef, partitionedOutputs, aggFuncInnerAlias, aggFuncOuterAlias);

            // 5. Check if needed BottomProjects
            boolean needBottomProjects = needBottomProjections(outputs, groupByExpressions);

            // 6. generate bottomProjections
            List<NamedExpression> bottomProjections = new ArrayList<>();
            if (needBottomProjects) {
                bottomProjections = genBottomProjections(groupByExpressions, groupByNewAlias,
                        partitionedOutputs, aggFuncInnerAlias, newVirSlotRef);
            }

            // assemble
            List<NamedExpression> newBottomProjections =
                    Utils.reorderProjections(new ArrayList<>(bottomProjections));
            List<NamedExpression> finalOutput =
                    Utils.reorderProjections(newOutputs);
            LogicalPlan root = aggregate.child();
            if (needBottomProjects) {
                root = new LogicalProject<>(newBottomProjections, root);
            }
            root = new LogicalAggregate<>(newGroupByExpressions, finalOutput, aggregate.isDisassembled(),
                    true, aggregate.isFinalPhase(), aggregate.getAggPhase(), root);
            List<NamedExpression> projections = outputs.stream()
                    .map(e -> ExpressionUtils.replace(e, substitutionMap))
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
            root = new LogicalProject<>(projections, root);

            return root;
        }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }

    /**
     * Generate Alias for non-slotReference in AggregateFunctions
     * eg: sum(k1#0)#1
     *      Alias: sum(k1#0)#1 as sum(k1)#2
     *      Map: (sum(k1#0)#1, Ailas)
     */
    private Map<Expression, Alias> genOuterAliasForAggFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncInnerNewSlot) {
        Map<Expression, Alias> aggFuncOuterNewSlot = new HashMap<>();
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    newChildren.add(child);
                } else {
                    newChildren.add(aggFuncInnerNewSlot.get(child).toSlot());
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            Alias alias = new Alias(newFunction, newFunction.toSql());
            aggFuncOuterNewSlot.put(newFunction, alias);
        }
        return ImmutableMap.copyOf(aggFuncOuterNewSlot);
    }

    private List<NamedExpression> genNewOutputs(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputs,
            Map<Expression, Expression> substitutionMap,
            List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPairs,
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncInnerNewSlot,
            Map<Expression, Alias> aggFuncOuterNewSlot) {
        return new ImmutableList.Builder<NamedExpression>()
                // 1. Extract slotReference and additionally generated dummy columns in GroupByExpressions
                .addAll(genNewOutputsWithGroupByExpressions(groupByExpressions, outputs, substitutionMap))
                // 2. Generate new Outputs in GroupingFunction.
                .addAll(newVirtualSlotRefPairs.stream().map(e -> e.second).collect(Collectors.toList()))
                // 3. Generate new Outputs in AggFunction.
                .addAll(genNewOutputsWithAggFunc(partitionedOutputs, aggFuncInnerNewSlot, aggFuncOuterNewSlot))
                .build();
    }

    private List<NamedExpression> genNewOutputsWithGroupByExpressions(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputs,
            Map<Expression, Expression> substitutionMap) {
        return groupByExpressions.stream().filter(k -> !(k instanceof VirtualSlotReference))
                .filter(k -> outputs.stream().anyMatch(e -> e.anyMatch(k::equals)))
                .map(k -> substitutionMap.get(k))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
    }

    private List<NamedExpression> genNewOutputsWithAggFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncInnerNewSlot,
            Map<Expression, Alias> aggFuncOuterNewSlot) {
        List<NamedExpression> newOutputs = new ArrayList<>();
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    newChildren.add(child);
                } else {
                    newChildren.add(aggFuncInnerNewSlot.get(child).toSlot());
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            newOutputs.add(aggFuncOuterNewSlot.get(newFunction));
        }
        return newOutputs;
    }

    private Map<Expression, Expression> genSubstitutionMap(
            List<Expression> groupByExpressions,
            Map<Expression, Alias> groupByNewSlot,
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncInnerNewSlot,
            Map<Expression, Alias> aggFuncOuterNewSlot,
            List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPair) {
        return new ImmutableMap.Builder<Expression, Expression>()
                .putAll(genSubstitutionMapWithGroupByExpressions(groupByExpressions, groupByNewSlot))
                .putAll(genSubstitutionMapWithAggFunc(partitionedOutputs, aggFuncInnerNewSlot, aggFuncOuterNewSlot))
                .putAll(genSubstitutionMapWithGroupingFunc(newVirtualSlotRefPair))
                .build();
    }

    private Map<Expression, Expression> genSubstitutionMapWithAggFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncInnerNewSlot,
            Map<Expression, Alias> aggFuncOuterNewSlot) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (!(child instanceof SlotReference || child instanceof Literal)) {
                    newChildren.add(aggFuncInnerNewSlot.get(child).toSlot());
                } else {
                    newChildren.add(child);
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            substitutionMap.put(aggregateFunction, aggFuncOuterNewSlot.get(newFunction).toSlot());
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    private List<NamedExpression> genBottomProjections(
            List<Expression> groupByExpressions,
            Map<Expression, Alias> groupByNewSlot,
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggNewSlot,
            List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPair) {
        return new ImmutableList.Builder<NamedExpression>()
                // 1. generate in groupByExpressions
                .addAll(genBottomProjectionsWithGroupByExpressions(groupByExpressions, groupByNewSlot))
                // 2. generate in AggregateFunction
                .addAll(genBottomProjectionsWithAggFunc(partitionedOutputs, aggNewSlot))
                // 3. generate in GroupingFunc
                .addAll(newVirtualSlotRefPair.stream()
                        .map(e -> e.second)
                        .map(NamedExpression.class::cast)
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * generate bottom projections with groupByExpressions.
     * eg:
     * groupByExpressions: k1#0, k2#1 + 1;
     * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
     */
    private List<NamedExpression> genBottomProjectionsWithGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Alias> groupByNewSlot) {
        List<NamedExpression> bottomProjections = new ArrayList<>();
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof SlotReference) {
                // skip VirtualSlotReference generate with GroupingFunc
                if (groupByExpression instanceof VirtualSlotReference
                        && !((VirtualSlotReference) groupByExpression).getRealSlots().isEmpty()) {
                    continue;
                }
                bottomProjections.add((NamedExpression) groupByExpression);
            } else {
                bottomProjections.add(groupByNewSlot.get(groupByExpression));
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    private boolean needBottomProjections(
            List<NamedExpression> outputs,
            List<Expression> groupByExpressions) {
        boolean hasVirtualSlot = !outputs.stream()
                .filter(e -> e.anyMatch(VirtualSlotReference.class::isInstance))
                .collect(Collectors.toList()).isEmpty();
        boolean hasAlias = !groupByExpressions.stream()
                .filter(e -> !(e instanceof SlotReference))
                .collect(Collectors.toList()).isEmpty();
        return hasAlias || hasVirtualSlot;
    }
}
