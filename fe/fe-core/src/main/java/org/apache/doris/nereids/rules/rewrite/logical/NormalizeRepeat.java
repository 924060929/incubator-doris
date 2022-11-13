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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * What does normalize meaning here?
 * 1: some expressions will be converted to Alias and push down to bottom plan.
 * 2: up plan reference the Alias by some slot references
 *
 * Q1: What expression should be normalized here?
 *
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
 *
 *
 *
 * 1. normalize groupByExpression: group by slot or alias
 * e.g.
 * group by                                 ->           group by
 *    a#1          -> keep as slot a        ->              a#1,
 *    a#1 + 1      -> new Alias(a#1 + 1)#2  ->              slot(#2)
 *
 *
 * 2. normalize virtual slot reference
 * We should keep the realSlots in virtualSlotReference which is argument of grouping set functions
 * as slot or literal, if no slot or literal, we will convert to slot by new Alias(expr).toSlot().
 * e.g.
 * VirtualSlotReference(realSlots=[                  ->  VirtualSlotReference(realSlots=[
 *     a#1,        -> keep as slot a                 ->      a#1,
 *     10,         -> keep as literal 10             ->      10,
 *     a#1 + 1     -> new Alias(#a + 1).toSlot()#2   ->      slot(#2)
 * ])                                                ->  ])
 */
public class NormalizeRepeat extends OneRewriteRuleFactory implements NormalizePlan {
    @Override
    public Rule build() {
        return logicalAggregate(logicalProject(logicalRepeat().whenNot(LogicalRepeat::isNormalized)))
                .then(agg -> {
                    LogicalRepeat<Plan> normalizedRepeat = normalizeRepeatAndBottom((LogicalProject) agg.child());
                    return normalizeAggregateAndProject((LogicalAggregate) agg, normalizedRepeat);
                }).toRule(RuleType.NORMALIZE_REPEAT);
    }

    private LogicalAggregate<LogicalProject<Plan>> normalizeAggregateAndProject(
            LogicalAggregate<LogicalProject<Plan>> agg, Plan normalizedChildOfProject) {
        LogicalProject<Plan> project = agg.child();

        ImmutableMap<NamedExpression, Slot> aliasToSlot = project.getProjects()
                .stream()
                .filter(output -> output instanceof Alias)
                .distinct()
                .map(output -> Pair.of(output, output.toSlot()))
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));

        // after normalized the child of project, all alias in the top project already exists in the
        // output of child, so we can simply rewrite to slotRef.
        LogicalProject<Plan> normalizedTopProject = new LogicalProject<>(
                project.getProjects()
                        .stream()
                        .map(output -> {
                            Slot slot = aliasToSlot.get(output);
                            // if slot == null, the output would be a slot, so not replace
                            return slot == null ? output : slot;
                        })
                        .collect(ImmutableList.toImmutableList()),
                normalizedChildOfProject
        );

        // finally the realSlots in the top aggregate should be replaced to slot
        return new LogicalAggregate<>(
                agg.getGroupByExpressions(),
                agg.getOutputExpressions(),
                agg.isDisassembled(),
                agg.isNormalized(),
                agg.isFinalPhase(),
                agg.getAggPhase(),
                normalizedTopProject
        );
    }

    private LogicalRepeat<Plan> normalizeRepeatAndBottom(LogicalProject<LogicalRepeat<Plan>> project) {
        LogicalRepeat<Plan> repeat = project.child();

        List<Expression> originGroupByInRepeat = repeat.getGroupByExpressions();
        List<NamedExpression> originOutputInRepeat = repeat.getOutputExpressions();

        // 1: which expression in group by should convert to alias or slot?
        Set<Expression> groupByShouldToAliasOrSlot = collectNonSlot(originGroupByInRepeat);

        // 2: which argument of aggregate function should convert to alias or slot?
        Set<Expression> argsOfAggFun = collectArgumentsOfAggregateFunction(
                collect(originOutputInRepeat, AggregateFunction.class)
        );
        Set<Expression> argOfAggFunShouldToAliasOrSlot = collectNonSlotAndNonLiteral(argsOfAggFun);

        // 3: which real slots of virtual slot reference should convert to alias or slot?
        Set<Expression> realSlotsShouldToAliasOrSlot = collectNonSlotAndNonLiteral(
                collectRealSlotsOfVirtualSlots(
                        combine(originGroupByInRepeat, originOutputInRepeat)
                )
        );

        ImmutableMap<Expression, Alias> alreadyHasAlias = ImmutableSet.<Alias>builder()
                .addAll(collect(repeat.getOutputExpressions(), Alias.class))
                .addAll(collect(repeat.getOutputExpressions(), Alias.class))
                .addAll(collect(collectRealSlotsOfVirtualSlots(repeat.getOutputExpressions()), Alias.class))
                .build()
                .stream()
                .collect(ImmutableMap.toImmutableMap(alias -> alias.child(), alias -> alias));

        // Q1: collect the expression that should convert to alias or slot for normalize
        Set<Expression> shouldToAliasOrSlot = ImmutableSet.<Expression>builder()
                .addAll(groupByShouldToAliasOrSlot)
                .addAll(argOfAggFunShouldToAliasOrSlot)
                .addAll(realSlotsShouldToAliasOrSlot)
                .build();

        // so we need 2 replace maps here:
        // 1: replace map 1: expr to wrap an alias for bottom plan
        ImmutableMap<Expression, Alias> exprToAlias = shouldToAliasOrSlot.stream()
                .map(e -> {
                    if (e instanceof Alias) {
                        return Pair.of(e, (Alias) e);
                    } else if (alreadyHasAlias.containsKey(e)) {
                        return Pair.of(e, alreadyHasAlias.get(e));
                    } else {
                        return Pair.of(e, new Alias(e, e.toSql()));
                    }
                })
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));

        // 2: replace map 2: expr to alias to slotRef for top plan
        ImmutableMap<Expression, Slot> exprToSlot = ImmutableMap.<Expression, Alias>builder()
                .putAll(alreadyHasAlias)
                .putAll(exprToAlias)
                .build()
                .entrySet()
                .stream()
                .map(e2a -> Pair.of(e2a.getKey(), e2a.getValue().toSlot()))
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));

        boolean needBottomProjection = !shouldToAliasOrSlot.isEmpty();

        // we can see the normalized bottom plan: output some SlotReference or Alias.
        Plan normalizedBottomPlan = needBottomProjection
                ? pushDownSlotRefOrAliasInRepeat(repeat.child(), originGroupByInRepeat, argsOfAggFun, exprToAlias)
                : repeat.child();

        // after normalized the bottom plan, we can rewrite some expression to slotRef in LogicalRepeat.
        LogicalRepeat<Plan> normalizedRepeat = normalizeRepeat(repeat, normalizedBottomPlan, exprToSlot);
        return normalizedRepeat;
    }

    private LogicalRepeat<Plan> normalizeRepeat(LogicalRepeat<Plan> repeat, Plan normalizedBottomPlan,
            Map<Expression, Slot> exprToSlot) {
        return repeat.replaceWithChild(
                repeat.getGroupingSets(),
                // normalized group by
                toSlot(repeat.getGroupByExpressions(), exprToSlot),
                // normalized output
                orderByVirtualSlotsLast(
                        toSlot(
                                realSlotsOfVirtualSlotsToSlotOrLiteral(repeat.getOutputExpressions(), exprToSlot),
                                exprToSlot
                        )
                ),
                repeat.getGroupingSetIdSlots(), true, normalizedBottomPlan);
    }

    private Plan pushDownSlotRefOrAliasInRepeat(Plan childOfRepeat, List<Expression> groupByInRepeat,
            Set<Expression> argsOfAggFunInRepeat, Map<Expression, Alias> exprToAlias) {
        List<NamedExpression> outputSlotRefOrAlias = orderByVirtualSlotsLast(
                ImmutableList.<NamedExpression>builder()
                        .addAll(groupByInRepeat.stream()
                                .filter(e -> !(e instanceof Literal) && !(e instanceof VirtualSlotReference))
                                .map(e -> (e instanceof SlotReference) ? e : exprToAlias.get(e))
                                .map(NamedExpression.class::cast)
                                .collect(Collectors.toList())
                        )
                        .addAll(argsOfAggFunInRepeat.stream()
                                .filter(arg -> !(arg instanceof Literal) && !(arg instanceof VirtualSlotReference))
                                .map(e -> (e instanceof SlotReference) ? e : exprToAlias.get(e))
                                .map(NamedExpression.class::cast)
                                .collect(Collectors.toList())
                        )
                        .build()
        );
        return new LogicalProject<>(outputSlotRefOrAlias, childOfRepeat);
    }

    private List<NamedExpression> realSlotsOfVirtualSlotsToSlotOrLiteral(
            List<NamedExpression> outputs, Map<Expression, Slot> exprToSlot) {
        return outputs.stream()
                .map(output -> (NamedExpression) output.rewriteUp(e -> {
                    if (e instanceof VirtualSlotReference) {
                        VirtualSlotReference virtualSlot = (VirtualSlotReference) e;
                        List<Expression> realSlots = virtualSlot.getRealSlots();
                        if (realSlots.isEmpty()) {
                            return e;
                        }

                        List<Expression> normalizedRealSlots = realSlots.stream()
                                .map(originSlot -> {
                                    if (originSlot instanceof Slot || originSlot instanceof Literal) {
                                        return originSlot;
                                    }
                                    Slot replacedSlot = exprToSlot.get(originSlot);
                                    return replacedSlot == null ? originSlot : replacedSlot;
                                }).collect(Collectors.toList());
                        if (normalizedRealSlots.equals(virtualSlot.getRealSlots())) {
                            return virtualSlot;
                        }
                        return virtualSlot.withRealSlots(normalizedRealSlots);
                    }
                    return e;
                })).collect(ImmutableList.toImmutableList());
    }

    private <E extends Expression> List<E> toSlot(
            List<E> originExpressions, Map<Expression, Slot> replaceMap) {
        return originExpressions
                .stream()
                .map(originExpr -> {
                    if (originExpr instanceof VirtualSlotReference) {
                        VirtualSlotReference virtualSlotReference = (VirtualSlotReference) originExpr;
                        List<Expression> realSlots = toSlot(virtualSlotReference.getRealSlots(), replaceMap);
                        return (E) virtualSlotReference.withRealSlots(realSlots);
                    }
                    if (originExpr instanceof Slot) {
                        return originExpr;
                    }
                    Slot replaceSlot = replaceMap.get(originExpr);
                    return (E) (replaceSlot == null ? originExpr : replaceSlot);
                })
                .collect(ImmutableList.toImmutableList());
    }
}
