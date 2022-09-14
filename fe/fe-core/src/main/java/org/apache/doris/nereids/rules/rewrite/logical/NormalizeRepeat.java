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
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * normalize output, aggFunc and groupingFunc in grouping sets.
 * This rule is executed after NormalizeAggregate.
 *
 * eg: select sum(k2 + 1), grouping(k1) from t1 group by grouping sets ((k1));
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7],
 *   outputs:[sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, GROUPING_PREFIX_(k1#1)#7)
 * +-- Project(projections: k1#1, (k2#2 + 1) as (k2 + 1)#8), grouping_prefix(k1#1)#7
 *     +-- GroupingSets(
 *         keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *         outputs:sum(k2#2 + 1) as `sum(k2 + 1)`#3, group(grouping_prefix(k1#1)#7) as `grouping(k1 + 1)`#4
 *
 * After rule:
 * Aggregate(
 *   keys:[k1#1, SR#9]
 *   outputs:[sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, GROUPING_PREFIX_(k1#1)#7))
 *     +-- Project(k1#1, (K2 + 1)#10 as `(k2 + 1)`#8, grouping_id()#0, grouping_prefix(k1#1)#7)
 *         +-- GropingSets(
 *             keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *             outputs:k1#1, (k2 + 1)#10, grouping_prefix(k1#1)#7
 */
public class NormalizeRepeat extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalProject(logicalRepeat().whenNot(LogicalRepeat::isNormalized)))
                .then(agg -> {
                    LogicalProject<LogicalRepeat<GroupPlan>> project = agg.child();
                    LogicalRepeat<GroupPlan> repeat = project.child();

                    // substitution map used to substitute expression in repeat's output to use it as top projections
                    final Map<Expression, Expression> substitutionMap = Maps.newHashMap();
                    // substitution map used to substitute expression in aggreate's repeat
                    final Map<Expression, Expression> projectMap = Maps.newHashMap();
                    List<Expression> keys = repeat.getGroupByExpressions();
                    List<NamedExpression> newOutputs = Lists.newArrayList();
                    List<NamedExpression> bottomProjections = new ArrayList<>();
                    AtomicBoolean needBottomProjects = new AtomicBoolean(false);

                    // 1. generate new groupByExpression
                    List<Expression> newKeys = genNewGroupByExpressions(
                            keys, substitutionMap, bottomProjections, needBottomProjects);

                    // 2. generate new outputs, excluding outputs in aggFunc and groupingFunc
                    newOutputs.addAll(genFailingNewOutputs(
                            keys, repeat.getOutputExpressions(), substitutionMap));

                    // 3. Build a map that replaces the project
                    project.getProjects().forEach(p -> {
                        if (p instanceof SlotReference) {
                            if (!(p instanceof VirtualSlotReference)) {
                                projectMap.put(p, p);
                            }
                        } else {
                            projectMap.put(((Alias) p).child(), p.toSlot());
                        }
                    });

                    // 4. process expressions that contain repeat function
                    List<NamedExpression> outputs = repeat.getOutputExpressions();
                    Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                            .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)
                                    || e.anyMatch(GroupingScalarFunction.class::isInstance)));
                    if (partitionedOutputs.containsKey(true)) {
                        processAggFuncAndGroupingFunc(
                                partitionedOutputs, needBottomProjects, substitutionMap, newOutputs, bottomProjections);
                    }

                    // 5. assemble
                    // 5.1 Ensure that the columns are in order,
                    //   first slotReference and then virtualSlotReference
                    List<NamedExpression> projects = project.getProjects();
                    List<NamedExpression> newProjects = Utils.reorderProjections(projects);
                    List<NamedExpression> newBottomProjections =
                            Utils.reorderProjections(new ArrayList<>(bottomProjections));
                    List<NamedExpression> finalOutputs = Utils.reorderProjections(newOutputs);

                    LogicalPlan root = repeat.child();
                    if (needBottomProjects.get()) {
                        root = new LogicalProject<>(new ArrayList<>(newBottomProjections), root);
                    }

                    // 5.2. replace the alisa column
                    //    include: grouping Sets/project/aggregate
                    // replace grouping Sets output and repeat
                    List<Expression> newVirtualSlot =
                            repeat.getVirtualGroupingExprs().stream()
                                            .map(e -> replaceVirtualSlot(e, substitutionMap))
                                                    .collect(Collectors.toList());
                    root = repeat.replaceWithChild(repeat.getGroupingSets(), newKeys,
                            finalOutputs, repeat.getGroupingIdList(),
                            repeat.getVirtualSlotRefs(), newVirtualSlot,
                            repeat.hasChangedOutput(),
                            true, root);

                    // 5.3 replace project
                    List<NamedExpression> replacedProjects = newProjects.stream()
                            .map(e -> replaceVirtualSlot(e, projectMap))
                            .map(NamedExpression.class::cast)
                            .collect(Collectors.toList());
                    List<NamedExpression> finalProjects = replacedProjects.stream()
                            .map(e -> ExpressionUtils.replace(e, substitutionMap))
                            .map(NamedExpression.class::cast)
                            .collect(Collectors.toList());
                    root = new LogicalProject<>(finalProjects, root);

                    // 5.4 replace agg repeat
                    List<Expression> newAggGroupBy =
                            agg.getGroupByExpressions().stream()
                                            .map(e -> replaceVirtualSlot(e, projectMap))
                                                    .collect(Collectors.toList());
                    return new LogicalAggregate<>(newAggGroupBy, agg.getOutputExpressions(),
                            agg.isDisassembled(), agg.isNormalized(),
                            agg.isFinalPhase(), agg.getAggPhase(), root);
                }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }

    private List<Expression> genNewGroupByExpressions(
            List<Expression> keys, Map<Expression, Expression> substitutionMap,
            List<NamedExpression> bottomProjections, AtomicBoolean needBottomProjects) {
        List<Expression> newKeys = Lists.newArrayList();
        keys.stream().forEach(e -> {
            if (e instanceof SlotReference && !(e instanceof VirtualSlotReference)) {
                substitutionMap.put(e, e);
                bottomProjections.add((SlotReference) e);
                newKeys.add(e);
            }
            if (!(e instanceof SlotReference)) {
                Alias newExpr = new Alias(e, e.toSql());
                substitutionMap.put(newExpr.child(), newExpr.toSlot());
                bottomProjections.add(newExpr);
                newKeys.add(newExpr.toSlot());
                needBottomProjects.set(true);
            }
        });
        return newKeys;
    }

    private List<NamedExpression> genFailingNewOutputs(
            List<Expression> keys, List<NamedExpression> outputExpressions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> newOutputs = new ArrayList<>();
        keys.stream().filter(k -> !(k instanceof VirtualSlotReference))
                .map(k -> substitutionMap.get(k))
                .map(NamedExpression.class::cast)
                .forEach(newOutputs::add);
        keys.stream().filter(VirtualSlotReference.class::isInstance)
                .filter(k -> ((VirtualSlotReference) k).getRealSlots().isEmpty())
                .map(NamedExpression.class::cast)
                .forEach(newOutputs::add);
        return newOutputs;
    }

    private void processAggFuncAndGroupingFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            AtomicBoolean needBottomProjects, Map<Expression, Expression> substitutionMap,
            List<NamedExpression> newOutputs, List<NamedExpression> bottomProjections) {
        Set<GroupingScalarFunction> groupingSetsFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<GroupingScalarFunction>>collect(
                        GroupingScalarFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        // replace all non-slot expression in repeat functions children.
        for (GroupingScalarFunction groupingSetsFunction : groupingSetsFunctions) {
            for (Expression child : groupingSetsFunction.getArguments()) {
                List<Expression> innerChildren = Lists.newArrayList();
                for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                    if (realChild instanceof SlotReference || realChild instanceof Literal) {
                        innerChildren.add(realChild);
                    } else {
                        needBottomProjects.set(true);
                        innerChildren.add(substitutionMap.get(realChild));
                    }
                }
                VirtualSlotReference newVirtual =
                        new VirtualSlotReference(((VirtualSlotReference) child).getExprId(),
                                ((VirtualSlotReference) child).getName(),
                                child.getDataType(), child.nullable(),
                                ((VirtualSlotReference) child).getQualifier(),
                                innerChildren, ((VirtualSlotReference) child).hasCast());
                substitutionMap.put(child, newVirtual);
                newOutputs.add(newVirtual);
            }
        }

        // process expressions that contain agg function
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    newChildren.add(child);
                    if (child instanceof SlotReference) {
                        bottomProjections.add((SlotReference) child);
                        newOutputs.add((NamedExpression) child);
                    }
                } else {
                    needBottomProjects.set(true);
                    Alias alias = new Alias(child, child.toSql());
                    bottomProjections.add(alias);
                    newChildren.add(alias.toSlot());
                    substitutionMap.put(child, alias.toSlot());
                    newOutputs.add(alias.toSlot());
                }
            }
        }
    }

    /**
     * Replace children in virtualSlotReference.
     */
    private Expression replaceVirtualSlot(
            Expression repeat, Map<Expression, Expression> substitutionMap) {
        if (repeat instanceof VirtualSlotReference) {
            if (!((VirtualSlotReference) repeat).getRealSlots().isEmpty()) {
                List<Expression> newChildren = ((VirtualSlotReference) repeat).getRealSlots().stream()
                        .map(child -> {
                            if (substitutionMap.containsKey(child)) {
                                return substitutionMap.get(child);
                            } else {
                                return child;
                            }
                        })
                        .collect(Collectors.toList());
                return new VirtualSlotReference(((VirtualSlotReference) repeat).getExprId(),
                        ((VirtualSlotReference) repeat).getName(), repeat.getDataType(),
                        repeat.nullable(), ((VirtualSlotReference) repeat).getQualifier(),
                        newChildren, ((VirtualSlotReference) repeat).hasCast());
            }
        }
        return repeat;
    }
}
