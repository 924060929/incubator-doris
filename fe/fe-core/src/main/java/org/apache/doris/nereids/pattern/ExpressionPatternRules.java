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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatchRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/** ExpressionPatternMapping */
public class ExpressionPatternRules extends TypeMappings<Expression, ExpressionPatternMatchRule> {
    private static final Logger LOG = LogManager.getLogger(ExpressionPatternRules.class);

    public ExpressionPatternRules(List<ExpressionPatternMatchRule> typeMappings) {
        super(typeMappings);
    }

    @Override
    protected Set<Class<? extends Expression>> getChildrenClasses(Class<? extends Expression> clazz) {
        return org.apache.doris.nereids.pattern.GeneratedExpressionRelations.CHILDREN_CLASS_MAP.get(clazz);
    }

    /** matchesAndApply */
    public Expression matchesAndApply(Expression expr, ExpressionRewriteContext context, Expression parent) {
        ExpressionMatchingContext<Expression> matchingContext
                = new ExpressionMatchingContext<>(expr, parent, context.cascadesContext);
        List<ExpressionPatternMatchRule> rules = singleMappings.get(expr.getClass());
        switch (rules.size()) {
            case 0: {
                for (ExpressionPatternMatchRule multiMatchRule : multiMappings) {
                    if (multiMatchRule.matchesTypeAndPredicates(matchingContext)) {
                        // return multiMatchRule.newExpr(matchingContext);
                        Expression newExpr = multiMatchRule.apply(matchingContext);
                        if (!newExpr.equals(expr)) {
                            // try {
                            //     Field[] declaredFields = (multiMatchRule.matchingAction).getClass().getDeclaredFields();
                            //     Class ruleClass;
                            //     if (declaredFields.length == 0) {
                            //         ruleClass = multiMatchRule.matchingAction.getClass();
                            //     } else {
                            //         Field field = declaredFields[0];
                            //         field.setAccessible(true);
                            //         ruleClass = field.get(multiMatchRule.matchingAction).getClass();
                            //     }
                            //     LOG.info("RULE: " + ruleClass + "\nbefore: " + expr + "\nafter: " + newExpr);
                            // } catch (Throwable t) {
                            //     LOG.error(t.getMessage(), t);
                            // }
                            return newExpr;
                        }
                    }
                }
                return expr;
            }
            case 1: {
                ExpressionPatternMatchRule rule = rules.get(0);
                if (rule.matchesPredicates(matchingContext)) {
                    // return rule.newExpr(matchingContext);
                    Expression newExpr = rule.apply(matchingContext);
                    // if (!newExpr.equals(expr)) {
                        // try {
                        //     Field[] declaredFields = (rule.matchingAction).getClass().getDeclaredFields();
                        //     Class ruleClass;
                        //     if (declaredFields.length == 0) {
                        //         ruleClass = rule.matchingAction.getClass();
                        //     } else {
                        //         Field field = declaredFields[0];
                        //         field.setAccessible(true);
                        //         ruleClass = field.get(rule.matchingAction).getClass();
                        //     }
                        //     LOG.info("RULE: " + ruleClass + "\nbefore: " + expr + "\nafter: " + newExpr);
                        // } catch (Throwable t) {
                        //     LOG.error(t.getMessage(), t);
                        // }
                    // }
                    return newExpr;
                }
                return expr;
            }
            default: {
                for (ExpressionPatternMatchRule rule : rules) {
                    if (rule.matchesPredicates(matchingContext)) {
                        Expression newExpr = rule.apply(matchingContext);
                        if (!expr.equals(newExpr)) {
                            // try {
                            //     Field[] declaredFields = (rule.matchingAction).getClass().getDeclaredFields();
                            //     Class ruleClass;
                            //     if (declaredFields.length == 0) {
                            //         ruleClass = rule.matchingAction.getClass();
                            //     } else {
                            //         Field field = declaredFields[0];
                            //         field.setAccessible(true);
                            //         ruleClass = field.get(rule.matchingAction).getClass();
                            //     }
                            //     LOG.info("RULE: " + ruleClass + "\nbefore: " + expr + "\nafter: " + newExpr);
                            // } catch (Throwable t) {
                            //     LOG.error(t.getMessage(), t);
                            // }
                            return newExpr;
                        }
                    }
                }
                return expr;
            }
        }
    }
}
