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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.runtime.SqlExecutionPipelineTask;
import org.apache.doris.qe.runtime.SqlExecutionPipelineTaskBuilder;
import org.apache.doris.qe.runtime.ThriftExecutionBuilder;
import org.apache.doris.thrift.TPipelineFragmentParamsList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/** NereidsCoordinator */
public class NereidsCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(NereidsCoordinator.class);

    private final ExecContext execContext;
    private volatile SqlExecutionPipelineTask executionTask;

    public NereidsCoordinator(ConnectContext context, Analyzer analyzer,
            Planner planner, StatsErrorEstimator statsErrorEstimator, NereidsPlanner nereidsPlanner) {
        super(context, analyzer, planner, statsErrorEstimator);
        this.execContext = ThriftExecutionBuilder.buildExecContext(nereidsPlanner);
    }

    @Override
    protected void execInternal() throws Exception {
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragments
                = ThriftExecutionBuilder.plansToThrift(execContext);
        executionTask = SqlExecutionPipelineTaskBuilder.build(execContext, workerToFragments);
        executionTask.execute();

        this.receivers = executionTask.getReceivers();
    }
}
