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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.RewriteJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.RootPlanTreeRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.VisitorRewriteJob;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Base class for executing all jobs.
 *
 * Each batch of rules will be uniformly executed.
 */
public abstract class BatchRewriteJob {
    protected CascadesContext cascadesContext;

    BatchRewriteJob(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not null");
    }

    public static List<RewriteJob> jobs(RewriteJob... batches) {
        return Arrays.asList(batches);
    }

    public static RewriteJob bottomUp(RuleFactory... ruleFactories) {
        return bottomUp(Arrays.asList(ruleFactories));
    }

    public static RewriteJob bottomUp(List<RuleFactory> ruleFactories) {
        List<Rule> rules = new ArrayList<>();
        for (RuleFactory ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        return new RootPlanTreeRewriteJob(rules, PlanTreeRewriteBottomUpJob::new, true);
    }

    public static RewriteJob topDown(RuleFactory... ruleFactories) {
        return topDown(Arrays.asList(ruleFactories));
    }

    public static RewriteJob topDown(List<RuleFactory> ruleFactories) {
        return topDown(ruleFactories, true);
    }

    public static RewriteJob topDown(List<RuleFactory> ruleFactories, boolean once) {
        List<Rule> rules = new ArrayList<>();
        for (RuleFactory ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        return new RootPlanTreeRewriteJob(rules, PlanTreeRewriteTopDownJob::new, once);
    }

    public static RewriteJob visitor(RuleType ruleType, DefaultPlanRewriter<JobContext> planRewriter) {
        return new VisitorRewriteJob(planRewriter, ruleType);
    }

    /**
     * execute.
     */
    public void execute() {
        for (RewriteJob job : getJobs()) {
            do {
                job.execute(cascadesContext.getCurrentJobContext());
            } while (!job.isOnce() && cascadesContext.getCurrentJobContext().isRewritten());
        }
    }

    public abstract List<RewriteJob> getJobs();
}
