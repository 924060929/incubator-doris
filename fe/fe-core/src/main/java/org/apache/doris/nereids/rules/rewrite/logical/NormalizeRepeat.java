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
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class NormalizeRepeat extends OneRewriteRuleFactory implements NormalizePlan {
    @Override
    public Rule build() {
        return logicalAggregate(logicalProject(logicalRepeat().whenNot(LogicalRepeat::isNormalized)))
                .then(agg -> {
                    LogicalProject<LogicalRepeat<GroupPlan>> project = agg.child();
                    LogicalRepeat<GroupPlan> repeat = project.child();

                    List<Expression> groupByExpressions = repeat.getGroupByExpressions();
                    Map<Expression, Alias> groupByNewAlias = genAliasChildToSlotForGroupBy(groupByExpressions);
                    List<NamedExpression> outputs = repeat.getOutputExpressions();
                    Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                            .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)
                                    || e.anyMatch(GroupingScalarFunction.class::isInstance)));
                    Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                            .flatMap(e -> e.<Set<AggregateFunction>>collect(
                                    AggregateFunction.class::isInstance).stream())
                            .collect(Collectors.toSet());
                    Map<Expression, Alias> aggNewAlias = genInnerAliasForAggFunc(aggregateFunctions);

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
                                    partitionedOutputs, aggNewAlias, newVirSlotRef);

                    // 4. generate new outputs
                    List<NamedExpression> newOutputs =
                            genNewOutputs(groupByExpressions, substitutionMap,
                                    newVirSlotRef, partitionedOutputs, aggNewAlias);

                    // 5. Check if needed BottomProjects
                    boolean needBottomProjections = needBottomProjections(groupByExpressions, partitionedOutputs);

                    // 6. generate bottomProjections
                    List<NamedExpression> bottomProjections = new ArrayList<>();
                    if (needBottomProjections) {
                        bottomProjections = genBottomProjections(groupByExpressions,
                                groupByNewAlias, partitionedOutputs, aggNewAlias);
                    }

                    // 7. Build a map that replaces the project
                    Map<Expression, Expression> projectMap = genProjectMap(project.getProjects());

                    // 8. Ensure that the columns are in order,
                    //   first slotReference and then virtualSlotReference
                    List<NamedExpression> projects = project.getProjects();
                    List<NamedExpression> newProjects = Utils.reorderProjections(projects);
                    List<NamedExpression> newBottomProjections =
                            Utils.reorderProjections(new ArrayList<>(bottomProjections));
                    List<NamedExpression> finalOutputs = Utils.reorderProjections(newOutputs);

                    return generateNewPlan(agg, repeat,
                            needBottomProjections, newBottomProjections,
                            newGroupByExpressions, finalOutputs,
                            newProjects, projectMap, substitutionMap);
                }).toRule(RuleType.NORMALIZE_REPEAT);
    }

    private List<NamedExpression> genNewOutputs(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap,
            List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPairs,
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> newAggAlias) {

        return new ImmutableList.Builder<NamedExpression>()
                // 1. Extract slotReference and additionally generated dummy columns in GroupByExpressions
                .addAll(genNewOutputsWithGroupByExpressions(groupByExpressions, substitutionMap))
                // 2. Generate new Outputs in GroupingFunction.
                .addAll(newVirtualSlotRefPairs.stream().map(e -> e.second).collect(Collectors.toList()))
                // 3. Generate new Outputs in AggFunction.
                .addAll(genNewOutputsWithAggFunc(partitionedOutputs, newAggAlias))
                .build();
    }

    private List<NamedExpression> genNewOutputsWithGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> newOutputs = new ArrayList<>();
        groupByExpressions.stream().filter(k -> !(k instanceof VirtualSlotReference))
                .map(substitutionMap::get)
                .map(NamedExpression.class::cast)
                .forEach(newOutputs::add);
        groupByExpressions.stream().filter(VirtualSlotReference.class::isInstance)
                .filter(k -> ((VirtualSlotReference) k).getRealSlots().isEmpty())
                .map(NamedExpression.class::cast)
                .forEach(newOutputs::add);
        return ImmutableList.copyOf(newOutputs);
    }

    private List<NamedExpression> genNewOutputsWithAggFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncNewSlot) {
        List<NamedExpression> newOutputs = new ArrayList<>();
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    if (child instanceof SlotReference) {
                        newOutputs.add((NamedExpression) child);
                    }
                } else {
                    newOutputs.add(aggFuncNewSlot.get(child).toSlot());
                }
            }
        }
        return ImmutableList.copyOf(newOutputs);
    }

    private Map<Expression, Expression> genSubstitutionMap(
            List<Expression> groupByExpressions,
            Map<Expression, Alias> groupByNewSlot,
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggFuncNewSlot,
            List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPair) {
        return new ImmutableMap.Builder<Expression, Expression>()
                // 1. Fill the expression in groupBy into the map
                .putAll(genSubstitutionMapWithGroupByExpressions(groupByExpressions, groupByNewSlot))
                // 2. Fill the expression in aggFunc into the map
                .putAll(genSubstitutionMapWithAggFunc(partitionedOutputs, aggFuncNewSlot))
                // 3. Fill the expression in groupingFunc into the map
                .putAll(genSubstitutionMapWithGroupingFunc(newVirtualSlotRefPair))
                .build();
    }

    private Map<Expression, Expression> genSubstitutionMapWithAggFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> newSlot) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (!(child instanceof SlotReference || child instanceof Literal)) {
                    substitutionMap.put(child, newSlot.get(child).toSlot());
                }
            }
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    private List<NamedExpression> genBottomProjections(
            List<Expression> groupByExpressions,
            Map<Expression, Alias> groupByNewSlot,
            Map<Boolean, List<NamedExpression>> partitionedOutputs,
            Map<Expression, Alias> aggNewSlot) {
        return new ImmutableList.Builder<NamedExpression>()
                // 1. generate in groupByExpressions
                .addAll(genBottomProjectionsWithGroupByExpressions(groupByExpressions, groupByNewSlot))
                // 2. generate in AggregateFunction
                .addAll(genBottomProjectionsWithAggFunc(partitionedOutputs, aggNewSlot))
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
            if (groupByExpression instanceof VirtualSlotReference) {
                continue;
            } else if (groupByExpression instanceof SlotReference) {
                bottomProjections.add((NamedExpression) groupByExpression);
            } else {
                bottomProjections.add(groupByNewSlot.get(groupByExpression));
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    private boolean needBottomProjections(
            List<Expression> groupByExpressions,
            Map<Boolean, List<NamedExpression>> partitionedOutputs) {
        return checkInGroupByExpressions(groupByExpressions)
                || checkInGroupingFunc(partitionedOutputs)
                || checkInAggFunc(partitionedOutputs);
    }

    private boolean checkInGroupByExpressions(List<Expression> groupByExpressions) {
        return !groupByExpressions.stream()
                .filter(e -> !(e instanceof SlotReference))
                .collect(Collectors.toList()).isEmpty();
    }

    private boolean checkInGroupingFunc(
            Map<Boolean, List<NamedExpression>> partitionedOutputs) {
        boolean needBottomProjects = false;
        Set<GroupingScalarFunction> groupingSetsFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<GroupingScalarFunction>>collect(
                        GroupingScalarFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        for (GroupingScalarFunction groupingSetsFunction : groupingSetsFunctions) {
            for (Expression child : groupingSetsFunction.getArguments()) {
                for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                    if (!(realChild instanceof SlotReference || realChild instanceof Literal)) {
                        needBottomProjects = true;
                    }
                }
            }
        }
        return needBottomProjects;
    }

    private boolean checkInAggFunc(Map<Boolean, List<NamedExpression>> partitionedOutputs) {
        boolean needBottomProjects = false;
        Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet());

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (!(child instanceof SlotReference || child instanceof Literal)) {
                    needBottomProjects = true;
                }
            }
        }
        return needBottomProjects;
    }

    private Map<Expression, Expression> genProjectMap(List<NamedExpression> projections) {
        Map<Expression, Expression> projectMap = new HashMap<>();
        for (NamedExpression project : projections) {
            if (project instanceof SlotReference) {
                if (!(project instanceof VirtualSlotReference)) {
                    projectMap.put(project, project);
                }
            } else {
                projectMap.put(((Alias) project).child(), project.toSlot());
            }
        }
        return ImmutableMap.copyOf(projectMap);
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

    private LogicalPlan generateNewPlan(
            LogicalAggregate<LogicalProject<LogicalRepeat<GroupPlan>>> agg,
            LogicalRepeat<GroupPlan> repeat,
            boolean needBottomProjections,
            List<NamedExpression> newBottomProjections,
            List<Expression> newGroupByExpressions,
            List<NamedExpression> finalOutputs,
            List<NamedExpression> newProjects,
            Map<Expression, Expression> projectMap,
            Map<Expression, Expression> substitutionMap) {
        LogicalPlan root = repeat.child();
        if (needBottomProjections) {
            root = new LogicalProject<>(new ArrayList<>(newBottomProjections), root);
        }

        // 1. replace the alisa column
        // replace grouping Sets output and repeat
        root = repeat.replaceWithChild(repeat.getGroupingSets(), newGroupByExpressions,
                finalOutputs, repeat.getGroupingSetIdSlots(), true, root);

        // 2. replace project
        List<NamedExpression> replacedProjects = newProjects.stream()
                .map(e -> replaceVirtualSlot(e, projectMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        List<NamedExpression> finalProjects = replacedProjects.stream()
                .map(e -> ExpressionUtils.replace(e, substitutionMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        root = new LogicalProject<>(finalProjects, root);

        // 3. replace agg repeat
        List<Expression> newAggGroupBy =
                agg.getGroupByExpressions().stream()
                        .map(e -> replaceVirtualSlot(e, projectMap))
                        .collect(Collectors.toList());
        return new LogicalAggregate<>(newAggGroupBy, agg.getOutputExpressions(),
                agg.isDisassembled(), agg.isNormalized(),
                agg.isFinalPhase(), agg.getAggPhase(), root);
    }
}
