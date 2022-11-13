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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** NormalizeAggregate */
public class NormalizeAgg implements RewriteRuleFactory, NormalizePlan {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(any()).whenNot(LogicalAggregate::isNormalized).then(agg -> {
                    PushDownContext context = initPushDownContextByExistentAlias(agg.getGroupByExpressions());

                    // NOTE: PushDownContext will be dynamic modify when invoke pushDown
                    ImmutableList<PushDownResult> pushDownGroupByResults = agg.getGroupByExpressions()
                            .stream()
                            .map(groupBy -> pushDown(groupBy, context))
                            .collect(ImmutableList.toImmutableList());

                    List<NamedExpression> replacedOutput = orderByVirtualSlotsLast(
                            agg.getOutputExpressions()
                                    .stream()
                                    .map(output -> replaceOutputByPushDownContext(output, context))
                                    .collect(ImmutableList.toImmutableList())
                    );

                    ImmutableSet<NamedExpression> pushedNamedExpressions = pushDownGroupByResults.stream()
                            .flatMap(result -> result.pushedExprs.stream())
                            .collect(ImmutableSet.toImmutableSet());

                    Set<Slot> outputUsedOtherSlots = agg.getOutputExpressions().stream()
                            .flatMap(output -> output.getInputSlots().stream())
                            // virtual slots already push down in the group by
                            .filter(slot -> !(slot instanceof VirtualSlotReference))
                            // not exist in the pushedNamedExpressions
                            .filter(slot -> pushedNamedExpressions.stream().noneMatch(pushedNamedExpression ->
                                    pushedNamedExpression.anyMatch(slot::equals)
                            )).collect(Collectors.toSet());

                    List<NamedExpression> pushedExprs = orderByVirtualSlotsLast(
                            ImmutableSet.<NamedExpression>builder()
                                .addAll(pushedNamedExpressions)
                                .addAll(outputUsedOtherSlots)
                                .build()
                    );

                    Plan child = pushDownToChildPlan(agg.child(), pushedExprs);

                    List<Expression> normalizedGroupBy = pushDownGroupByResults.stream()
                            .map(result -> result.remainExpr)
                            .collect(ImmutableList.toImmutableList());

                    boolean normalized = true;
                    return new LogicalAggregate(normalizedGroupBy, replacedOutput, agg.isDisassembled(),
                            normalized, agg.isFinalPhase(), agg.getAggPhase(), child);
                }).toRule(RuleType.NORMALIZE_AGGREGATE)
        );
    }

    // initPushDownContext, you should pass all the expressions which you want to push down for normalize,
    // or else if the push down expression contains Alias, it will throw exception because can not find
    // the alias in the replaced triplet.
    private PushDownContext initPushDownContextByExistentAlias(List<? extends Expression>... needToPushDown) {
        Map<Expression, PushDownTriplet> pushDownTriplets = Maps.newLinkedHashMap();
        for (List<? extends Expression> exprList : needToPushDown) {
            for (Expression expression : exprList) {
                Set<Alias> aliases = expression.collect(Alias.class::isInstance);
                // TODO: check if contains undetermined function
                for (Alias alias : aliases) {
                    Expression originExpr = alias.child();
                    PushDownTriplet triplet = new PushDownTriplet(originExpr, alias.toSlot(), alias);
                    pushDownTriplets.put(originExpr, triplet);
                }

                Set<VirtualSlotReference> virtualSlotReferences
                        = expression.collect(VirtualSlotReference.class::isInstance);
                for (VirtualSlotReference virtualSlotReference : virtualSlotReferences) {
                    List<Expression> realSlots = virtualSlotReference.getRealSlots();
                    for (Expression realSlot : realSlots) {
                        if (realSlot instanceof Alias) {
                            Alias alias = (Alias) realSlot;
                            Expression originExpr = alias.child();
                            PushDownTriplet triplet = new PushDownTriplet(originExpr, alias.toSlot(), alias);
                            pushDownTriplets.put(originExpr, triplet);
                        }
                    }
                }
            }
        }
        return new PushDownContext(pushDownTriplets);
    }

    private Plan pushDownToChildPlan(Plan childPlan, List<NamedExpression> pushedExprs) {
        // useless project
        if (isSameOutput(pushedExprs, childPlan.getOutput())) {
            return childPlan;
        }

        if (!(childPlan instanceof LogicalProject)) {
            return new LogicalProject<>(pushedExprs, childPlan);
        }

        return mergeProject((LogicalProject<Plan>) childPlan, pushedExprs);
    }

    private LogicalProject<Plan> mergeProject(LogicalProject<Plan> project, List<NamedExpression> otherProjects) {

        return project;
    }

    private boolean isSameOutput(List<? extends NamedExpression> output1, List<? extends NamedExpression> output2) {
        if (output1.size() == output2.size()) {
            List<NamedExpression> defaultSortedOutput1 = output1.stream().sorted().collect(Collectors.toList());
            List<NamedExpression> defaultSortedOutput2 = output2.stream().sorted().collect(Collectors.toList());
            return defaultSortedOutput1.equals(defaultSortedOutput2);
        }
        return false;
    }

    // e.g. LogicalAggregate(groupBy=[a + 1], output=[Alias(a + 1)#1])
    //
    // we will generate a remainExpr `SlotReference(a + 1)#2` and pushedExpr `Alias(a + 1)#2` when invoke
    // `pushDown(groupByExpressions)`, then invoke `replaceOutputByPushDownContext` will replace the output
    // `Alias(a + 1)#1` to `Alias(SlotReference(a + 1)#2)#1` for normalized
    private NamedExpression replaceOutputByPushDownContext(NamedExpression expression, PushDownContext context) {
        return (NamedExpression) expression.rewriteDownShortCircuit(originExpr -> {
            PushDownTriplet pushDownTriplet = context.getPushDownTriplet(originExpr);
            if (pushDownTriplet != null) {
                return pushDownTriplet.remainSlotRef;
            }
            return originExpr;
        });
    }

    private PushDownResult pushDown(Expression expression, PushDownContext context) {
        PushDownTriplet pushDownTriplet = context.getPushDownTriplet(expression);
        if (pushDownTriplet != null) {
            return PushDownResult
                    .remain(pushDownTriplet.remainSlotRef)
                    .pushDown(pushDownTriplet.pushDownExprs)
                    .build();
        }

        Class<? extends Expression> exprClass = expression.getClass();

        if (exprClass.equals(VirtualSlotReference.class)) {
            return pushDownVirtualSlotReference((VirtualSlotReference) expression, context);
        }

        if (exprClass.equals(SlotReference.class)) {
            return pushDownSlotReference((SlotReference) expression, context);
        }

        if (expression instanceof Literal) {
            return pushDownLiteral((Literal) expression, context);
        }

        if (expression instanceof Alias) {
            return pushDownAlias((Alias) expression, context);
        }

        return pushDownOther(expression, context);
    }

    private PushDownResult pushDownVirtualSlotReference(VirtualSlotReference slot, PushDownContext context) {
        ImmutableSet.Builder<NamedExpression> pushedExprs = ImmutableSet.builder();
        ImmutableList.Builder<Expression> newRealSlots = ImmutableList.builder();

        for (Expression realSlot : slot.getRealSlots()) {
            PushDownResult pushDownResult = pushDown(realSlot, context);
            newRealSlots.add(pushDownResult.remainExpr);
            pushedExprs.addAll(pushDownResult.pushedExprs);
        }

        VirtualSlotReference remainExpr = slot.withRealSlots(newRealSlots.build());
        pushedExprs.add(remainExpr);
        PushDownTriplet triplet = new PushDownTriplet(slot, remainExpr, pushedExprs.build());
        context.addPushDownTriplet(triplet);

        return PushDownResult
                .remain(remainExpr)
                .pushDown(triplet.pushDownExprs)
                .build();
    }

    private PushDownResult pushDownSlotReference(SlotReference slotReference, PushDownContext context) {
        PushDownTriplet pushDownTriplet = new PushDownTriplet(slotReference, slotReference, slotReference);
        context.addPushDownTriplet(pushDownTriplet);

        return PushDownResult
                .remain(slotReference)
                .pushDown(slotReference)
                .build();
    }

    private PushDownResult pushDownLiteral(Literal literal, PushDownContext context) {
        // no push down
        return PushDownResult
                .remain(literal)
                .build();
    }

    private PushDownResult pushDownAlias(Alias alias, PushDownContext context) {
        // should not enter this method, it must a bug.
        throw new AnalysisException("Can not push down Alias for normalize because " + alias.toSql()
                + " not exist in the PushDownContext. Have you init PushDownContext by the existent Alias");
    }

    private PushDownResult pushDownOther(Expression expression, PushDownContext context) {
        Alias pushDownExpr = new Alias(expression, expression.toSql());
        SlotReference remainSlotRef = pushDownExpr.toSlot();
        PushDownTriplet triplet = new PushDownTriplet(expression, remainSlotRef, pushDownExpr);
        context.addPushDownTriplet(triplet);
        return PushDownResult
                .remain(triplet.remainSlotRef)
                .pushDown(triplet.pushDownExprs)
                .build();
    }

    private static class PushDownContext {
        private Map<Expression, PushDownTriplet> replaceTriplets;

        public PushDownContext(Map<Expression, PushDownTriplet> replaceTriplets) {
            this.replaceTriplets = replaceTriplets;
        }

        public PushDownTriplet getPushDownTriplet(Expression expression) {
            return replaceTriplets.get(expression);
        }

        public void addPushDownTriplet(PushDownTriplet triplet) {
            replaceTriplets.put(triplet.originExpr, triplet);
        }
    }

    private static class PushDownTriplet {
        private Expression originExpr;

        // remain the mapping SlotRef in current plan
        private SlotReference remainSlotRef;

        // push down the mapping NamedExpression to child plan
        private Set<NamedExpression> pushDownExprs;

        public PushDownTriplet(Expression originExpr, SlotReference remainSlotRef, NamedExpression pushDownExpr) {
            this(originExpr, remainSlotRef, ImmutableSet.of(pushDownExpr));

        }
        public PushDownTriplet(Expression originExpr, SlotReference remainSlotRef, Set<NamedExpression> pushDownExprs) {
            this.originExpr = originExpr;
            this.remainSlotRef = remainSlotRef;
            this.pushDownExprs = ImmutableSet.copyOf(pushDownExprs);
        }
    }

    private static class PushDownResult {
        private Expression remainExpr;

        // one expression maybe push down multi expressions, e.g. VirtualSlotReference(realSlots=[a+1, b+1]),
        // we should push down `a+1` and `b+1`
        private Set<NamedExpression> pushedExprs;

        public PushDownResult(Expression remainExpr, Set<NamedExpression> pushedExprs) {
            this.remainExpr = remainExpr;
            this.pushedExprs = pushedExprs;
        }

        public static PushDownResultBuilder remain(Expression remainExpr) {
            return new PushDownResultBuilder(remainExpr);
        }

        public static class PushDownResultBuilder {
            private Expression remainExpr;
            private ImmutableSet.Builder<NamedExpression> pushedExprs = ImmutableSet.builder();

            public PushDownResultBuilder(Expression remainExpr) {
                this.remainExpr = remainExpr;
            }

            public PushDownResultBuilder pushDown(NamedExpression pushedExpr) {
                pushedExprs.add(pushedExpr);
                return this;
            }

            public PushDownResultBuilder pushDown(Collection<NamedExpression> pushedExprs) {
                this.pushedExprs.addAll(pushedExprs);
                return this;
            }

            public PushDownResult build() {
                return new PushDownResult(remainExpr, pushedExprs.build());
            }
        }
    }
}
