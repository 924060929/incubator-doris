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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.RewriteJob;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Use visitor to rewrite the plan.
 */
public class VisitorRewriteJob implements RewriteJob {
    private final RuleType ruleType;

    private final DefaultPlanRewriter<JobContext> planRewriter;

    /**
     * Constructor.
     */
    public VisitorRewriteJob(DefaultPlanRewriter<JobContext> rewriter, RuleType ruleType) {
        this.ruleType = Objects.requireNonNull(ruleType, "ruleType cannot be null");
        this.planRewriter = Objects.requireNonNull(rewriter, "planRewriter cannot be null");
    }

    @Override
    public void execute(JobContext context) {
        Set<String> disableRules = Job.getDisableRules(context);
        if (disableRules.contains(ruleType.name().toUpperCase(Locale.ROOT))) {
            return;
        }
        Plan root = context.getCascadesContext().getRewritePlan();
        // COUNTER_TRACER.log(CounterEvent.of(Memo.get=-StateId(), CounterType.JOB_EXECUTION, group, logicalExpression,
        //         root));
        Plan rewrittenRoot = root.accept(planRewriter, context);
        context.getCascadesContext().setRewritePlan(rewrittenRoot);
    }

    @Override
    public boolean isOnce() {
        return false;
    }
}
