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

package org.apache.doris.nereids.jobs.batch;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.RewriteJob;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.AvgDistinctToSumDivCount;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewrite;
import org.apache.doris.nereids.rules.mv.SelectMaterializedIndexWithAggregate;
import org.apache.doris.nereids.rules.mv.SelectMaterializedIndexWithoutAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.logical.BuildAggForUnion;
import org.apache.doris.nereids.rules.rewrite.logical.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.logical.CountDistinctRewrite;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateGroupByConstant;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateLimit;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateOrderByConstant;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateUnnecessaryProject;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractFilterFromCrossJoin;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractSingleTableExpressionFromDisjunction;
import org.apache.doris.nereids.rules.rewrite.logical.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.logical.InferPredicates;
import org.apache.doris.nereids.rules.rewrite.logical.InnerToCrossJoin;
import org.apache.doris.nereids.rules.rewrite.logical.LimitPushDown;
import org.apache.doris.nereids.rules.rewrite.logical.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.logical.MergeSetOperations;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanTablet;
import org.apache.doris.nereids.rules.rewrite.logical.PushFilterInsideJoin;
import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;

import java.util.List;

/**
 * Apply rules to optimize logical plan.
 */
public class NereidsRewriter extends BatchRewriteJob {
    private static final List<RewriteJob> REWRITE_JOBS = jobs(
            topic("Normalization",
                topDown(
                    new EliminateOrderByConstant(),
                    new EliminateGroupByConstant(),

                    // MergeProjects depends on this rule
                    new LogicalSubQueryAliasToLogicalProject(),

                    // rewrite expressions, no depends
                    new ExpressionNormalization(),
                    new ExpressionOptimization(),
                    new AvgDistinctToSumDivCount(),
                    new CountDistinctRewrite(),

                    new NormalizeAggregate(),
                    new ExtractFilterFromCrossJoin(),
                    new AdjustAggregateNullableForEmptySet()
                ),

                // ExtractSingleTableExpressionFromDisjunction conflict to InPredicateToEqualToRule
                // in the ExpressionNormalization, so must invoke in another job, or else run into
                // deep loop
                topDown(
                        new ExtractSingleTableExpressionFromDisjunction()
                )
            ),

            topic("Subquery unnesting", topDown(
                    /*
                     * Eliminate useless plans in the subquery, including limit and sort.
                     * Compatible with the old optimizer, the sort and limit in the subquery will not take effect,
                     * just delete it directly.
                     */
                    new EliminateUselessPlanUnderApply(),

                    // CorrelateApplyToUnCorrelateApply and ApplyToJoin
                    // and SelectMaterializedIndexWithAggregate depends on this rule
                    new MergeProjects(),

                    /*
                     * Subquery unnesting.
                     * 1. Adjust the plan in correlated logicalApply
                     *    so that there are no correlated columns in the subquery.
                     * 2. Convert logicalApply to a logicalJoin.
                     *  TODO: group these rules to make sure the result plan is what we expected.
                     */
                    new CorrelateApplyToUnCorrelateApply(),
                    new ApplyToJoin()
            )),

            topic("Rewrite join",
                    // ReorderJoin depends PUSH_DOWN_FILTERS
                    topDown(RuleSet.PUSH_DOWN_FILTERS, false),

                    topDown(
                        new ReorderJoin(),
                        new PushFilterInsideJoin(),
                        new FindHashConditionForJoin(),
                        new InnerToCrossJoin()
                    )
            ),

            topic("Column pruning and infer predicate",
                    topDown(new ColumnPruning()),

                    custom(RuleType.INFER_PREDICATES, () -> new InferPredicates()),

                    // column pruning create new project, so we should use PUSH_DOWN_FILTERS
                    // to change filter-project to project-filter
                    topDown(RuleSet.PUSH_DOWN_FILTERS, false)
            ),

            // this rule should invoke after ColumnPruning
            custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, () -> new EliminateUnnecessaryProject()),

            // we need to execute this rule at the end of rewrite
            // to avoid two consecutive same project appear when we do optimization.
            topic("Others optimization", topDown(
                    new EliminateLimit(),
                    new EliminateFilter(),
                    new PruneOlapScanPartition(),
                    new SelectMaterializedIndexWithAggregate(),
                    new SelectMaterializedIndexWithoutAggregate(),
                    new PruneOlapScanTablet(),
                    new EliminateAggregate(),
                    new MergeSetOperations(),
                    new LimitPushDown(),
                    new BuildAggForUnion()
            )),

            // this rule batch must keep at the end of rewrite to do some plan check
            topic("Final rewrite and check", bottomUp(
                new AdjustNullable(),
                new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE),
                new CheckAfterRewrite()
            ))
    );

    public NereidsRewriter(CascadesContext cascadesContext) {
        super(cascadesContext);
    }

    @Override
    public List<RewriteJob> getJobs() {
        return REWRITE_JOBS;
    }
}
