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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Normalize Plan.
 */
public interface NormalizePlan {
    /**
     * get AggFunc from outputExpressions
     */
    default <E extends Expression> Set<E> collect(Collection<? extends Expression> expressions,
            Class<E> type) {
        return expressions.stream()
                .flatMap(e -> e.<Set<E>>collect(type::isInstance).stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    default Set<Expression> collectNonSlot(List<Expression> expressions) {
        return expressions.stream()
                .filter(e -> !(e instanceof SlotReference))
                .collect(ImmutableSet.toImmutableSet());
    }

    default Set<Expression> combine(Collection<? extends Expression>... exprLists) {
        ImmutableSet.Builder<Expression> appendList = ImmutableSet.builder();
        for (Collection<? extends Expression> exprList : exprLists) {
            appendList.addAll(exprList);
        }
        return appendList.build();
    }

    default Set<Expression> collectNamedExpression(Collection<? extends Expression> expressions) {
        return expressions.stream()
                .filter(NamedExpression.class::isInstance)
                .collect(Collectors.toSet());
    }

    default Set<Expression> collectArgumentsOfAggregateFunction(Collection<? extends AggregateFunction> expressions) {
        return collect(expressions, AggregateFunction.class)
                .stream()
                .flatMap(agg -> agg.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    default Set<Expression> collectRealSlotsOfVirtualSlots(Collection<? extends Expression> expressions) {
        return expressions.stream()
                .flatMap(o ->o.<Set<VirtualSlotReference>>collect(VirtualSlotReference.class::isInstance).stream())
                .flatMap(v -> v.getRealSlots().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    default Set<Expression> collectNonSlotAndNonLiteral(Set<Expression> expressions) {
        return expressions.stream()
                .filter(e -> !(e instanceof Slot) && !(e instanceof Literal))
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Generate Alias for non-slotReference in groupByExpressions
     */

    default Map<Expression, Alias> nonSlotRefToAlias(List<Expression> expressions) {
        return expressions.stream()
                .filter(e -> !(e instanceof SlotReference))
                .collect(ImmutableMap.toImmutableMap(Function.identity(), e -> new Alias(e, e.toSql())));
    }

    /**
     * Generate Alias for non-slotReference in AggregateFunctions
     * eg: sum(k1#0 + 1)
     *     Alias: k1#0 + 1 -> (k1 + 1)#1
     * Map((k1#0 + 1), ((k1 + 1)#1
     */
    default Map<Expression, Alias> aggregateFunctionArgumentToAlias(Set<AggregateFunction> aggregateFunctions) {
        Builder<Expression, Alias> argToAlias = ImmutableMap.builder();
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression arg : aggregateFunction.getArguments()) {
                if (!(arg instanceof SlotReference || arg instanceof Literal)) {
                    argToAlias.put(arg, new Alias(arg, arg.toSql()));
                }
            }
        }
        return argToAlias.build();
    }

    /**
     * generate new groupByExpressions.
     */
    default List<Expression> replaceToAlias(List<Expression> expressions, Map<Expression, Alias> exprToAlias) {
        ImmutableList.Builder<Expression> replaced = ImmutableList.builder();
        for (Expression expression : expressions) {
            Alias alias = exprToAlias.get(expression);
            replaced.add(alias == null ? expression : alias);
        }
        return replaced.build();
    }

    /**
     * generate SubstitutionMap with groupByExpressions.
     */
    default Map<Expression, Expression> genSubstitutionMapWithGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Alias> newSlot) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof SlotReference) {
                // skip groupingFunction
                if (groupByExpression instanceof VirtualSlotReference
                        && !((VirtualSlotReference) groupByExpression).getRealSlots().isEmpty()) {
                    continue;
                }
                substitutionMap.put(groupByExpression, groupByExpression);
            } else {
                substitutionMap.put(groupByExpression, newSlot.get(groupByExpression).toSlot());
            }
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    /**
     * Generate a new virtualSlotReference for each groupingFunc by
     * replacing non-slotReference internal expressions with alisa.
     *
     * eg:
     *      old: GROUPING_PREFIX_k1(k1#0 + 1), the argument: k1#0 + 1
     *      new: GROUPING_PREFIX_k1((k1 + 1)#2), the argument: VirtualSlotReference(realSlots=k1#0 + 1)
     * @return List(Pair(old, new))
     */
    default List<Pair<Expression, VirtualSlotReference>> wrapArgumentToVirtualSlot(
            Set<GroupingScalarFunction> groupingSetsFunctions,
            List<Expression> groupByExpressions,
            Map<Expression, Alias> groupByNewSlot) {
        Map<Expression, Expression> substitutionMap =
                genSubstitutionMapWithGroupByExpressions(groupByExpressions, groupByNewSlot);
        List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPair = new ArrayList<>();
        for (GroupingScalarFunction groupingSetsFunction : groupingSetsFunctions) {
            for (Expression child : groupingSetsFunction.getArguments()) {
                List<Expression> innerChildren = Lists.newArrayList();
                for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                    if (realChild instanceof SlotReference || realChild instanceof Literal) {
                        innerChildren.add(realChild);
                    } else {
                        innerChildren.add(substitutionMap.get(realChild));
                    }
                }
                VirtualSlotReference newVirtual =
                        new VirtualSlotReference(((VirtualSlotReference) child).getExprId(),
                                ((VirtualSlotReference) child).getName(),
                                child.getDataType(), child.nullable(),
                                ((VirtualSlotReference) child).getQualifier(),
                                innerChildren, ((VirtualSlotReference) child).hasCast());
                newVirtualSlotRefPair.add(Pair.of(child, newVirtual));
            }
        }
        return newVirtualSlotRefPair;
    }

    /**
     * generate substitutionMap with groupingFunc.
     */
    default Map<Expression, Expression> genSubstitutionMapWithGroupingFunc(
            List<Pair<Expression, VirtualSlotReference>> newVirtualSlotRefPair) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        newVirtualSlotRefPair.stream().forEach(pair -> substitutionMap.put(pair.first, pair.second));
        return substitutionMap;
    }

    /**
     * generate bottom projections with AggFunc.
     * eg: sum(k1#0)
     * bottom: k1#0
     */
    default List<NamedExpression> genBottomProjectionsWithAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Alias> aggNewSlot) {
        List<NamedExpression> bottomProjections = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    if (child instanceof SlotReference) {
                        bottomProjections.add((SlotReference) child);
                    }
                } else {
                    bottomProjections.add(aggNewSlot.get(child));
                }
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    /**
     * Rearrange the order of the projects to ensure that
     * slotReference is in the front and virtualSlotReference is in the back.
     */
    default List<NamedExpression> orderByVirtualSlotsLast(Collection<? extends NamedExpression> projections) {
        Map<Boolean, List<NamedExpression>> partitionProjections = projections.stream()
                .collect(Collectors.groupingBy(
                        VirtualSlotReference.class::isInstance,
                        LinkedHashMap::new, Collectors.toList()));

        List<NamedExpression> nonVirtualSlotOutput = partitionProjections.get(false);
        List<NamedExpression> virtualSlotOutput = partitionProjections.get(true);

        return ImmutableList.<NamedExpression>builder()
                .addAll(nonVirtualSlotOutput == null ? ImmutableList.of() : nonVirtualSlotOutput)
                .addAll(virtualSlotOutput == null ? ImmutableList.of() : virtualSlotOutput)
                .build();
    }
}
