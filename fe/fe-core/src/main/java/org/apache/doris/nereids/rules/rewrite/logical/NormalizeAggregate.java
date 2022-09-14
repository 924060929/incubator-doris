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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
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
public class NormalizeAggregate extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().whenNot(LogicalAggregate::isNormalized).then(aggregate -> {
            // substitution map used to substitute expression in aggregate's output to use it as top projections
            Map<Expression, Expression> substitutionMap = Maps.newHashMap();
            List<Expression> keys = aggregate.getGroupByExpressions();
            List<NamedExpression> newOutputs = Lists.newArrayList();

            // keys
            List<NamedExpression> bottomProjections = new ArrayList<>();
            List<Expression> newKeys = Lists.newArrayList();
            AtomicBoolean isNeedBottomProjects = new AtomicBoolean(false);
            keys.stream().forEach(e -> {
                if (e instanceof SlotReference) {
                    // process SlotReference keys
                    substitutionMap.put(e, e);
                    newKeys.add(e);
                    if (!(e instanceof VirtualSlotReference)
                            || ((VirtualSlotReference) e).getRealSlots().isEmpty()) {
                        bottomProjections.add((SlotReference) e);
                    }
                } else {
                    // process non-SlotReference keys
                    Alias newExpr = new Alias(e, e.toSql());
                    substitutionMap.put(newExpr.child(), newExpr.toSlot());
                    bottomProjections.add(newExpr);
                    newKeys.add(newExpr.toSlot());
                    isNeedBottomProjects.set(true);
                }
            });

            // add all necessary key to output
            keys.stream().filter(k -> !(k instanceof VirtualSlotReference))
                    .filter(k -> aggregate.getOutputExpressions().stream()
                            .anyMatch(e -> e.anyMatch(k::equals)))
                    .map(k -> substitutionMap.get(k))
                    .map(NamedExpression.class::cast)
                    .forEach(newOutputs::add);

            // if we generate bottom, we need to generate to project too.
            // output
            List<NamedExpression> outputs = aggregate.getOutputExpressions();
            Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                    .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)
                            || e.anyMatch(GroupingScalarFunction.class::isInstance)));

            List<NamedExpression> hasVirtualSlot = outputs.stream()
                    .filter(e -> e.anyMatch(VirtualSlotReference.class::isInstance))
                    .collect(Collectors.toList());
            boolean needBottomProjects = isNeedBottomProjects.get()
                    || !hasVirtualSlot.isEmpty();
            if (partitionedOutputs.containsKey(true)) {
                // process expressions that contain aggregate function
                Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                        .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                        .collect(Collectors.toSet());

                // replace all non-slot expression in aggregate functions children.
                for (AggregateFunction aggregateFunction : aggregateFunctions) {
                    List<Expression> newChildren = Lists.newArrayList();
                    for (Expression child : aggregateFunction.getArguments()) {
                        if (child instanceof SlotReference || child instanceof Literal) {
                            newChildren.add(child);
                            if (child instanceof SlotReference) {
                                bottomProjections.add((SlotReference) child);
                            }
                        } else {
                            needBottomProjects = true;
                            Alias alias = new Alias(child, child.toSql());
                            bottomProjections.add(alias);
                            newChildren.add(alias.toSlot());
                        }
                    }
                    AggregateFunction newFunction = (AggregateFunction) aggregateFunction.withChildren(newChildren);
                    Alias alias = new Alias(newFunction, newFunction.toSql());
                    newOutputs.add(alias);
                    substitutionMap.put(aggregateFunction, alias.toSlot());
                }

                Set<GroupingScalarFunction> groupingSetsFunctions = partitionedOutputs.get(true).stream()
                        .flatMap(e -> e.<Set<GroupingScalarFunction>>collect(
                                GroupingScalarFunction.class::isInstance).stream())
                        .collect(Collectors.toSet());

                for (GroupingScalarFunction groupingSetsFunction : groupingSetsFunctions) {
                    for (Expression child : groupingSetsFunction.getArguments()) {
                        List<Expression> newChildren = Lists.newArrayList();
                        if (child instanceof VirtualSlotReference) {
                            for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                                if (realChild instanceof SlotReference || realChild instanceof Literal) {
                                    newChildren.add(realChild);
                                } else {
                                    needBottomProjects = true;
                                    newChildren.add(substitutionMap.get(realChild));
                                }
                            }
                        }
                        VirtualSlotReference newVirtual =
                                new VirtualSlotReference(((VirtualSlotReference) child).getExprId(),
                                        ((VirtualSlotReference) child).getName(), child.getDataType(),
                                        child.nullable(), ((VirtualSlotReference) child).getQualifier(),
                                        newChildren, ((VirtualSlotReference) child).hasCast());
                        substitutionMap.put(child, newVirtual);
                        newOutputs.add(newVirtual);
                        bottomProjections.add(newVirtual);
                    }
                }
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
            root = new LogicalAggregate<>(newKeys, finalOutput, aggregate.isDisassembled(),
                    true, aggregate.isFinalPhase(), aggregate.getAggPhase(), root);
            List<NamedExpression> projections = outputs.stream()
                    .map(e -> ExpressionUtils.replace(e, substitutionMap))
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
            root = new LogicalProject<>(projections, root);

            return root;
        }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }
}
