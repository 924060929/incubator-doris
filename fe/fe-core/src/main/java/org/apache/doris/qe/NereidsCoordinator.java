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
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.runtime.MultiResultReceivers;
import org.apache.doris.qe.runtime.SqlPipelineTask;
import org.apache.doris.qe.runtime.SqlPipelineTaskBuilder;
import org.apache.doris.qe.runtime.ThriftExecutionBuilder;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/** NereidsCoordinator */
public class NereidsCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(NereidsCoordinator.class);

    private final CoordinatorContext coordinatorContext;
    private final MultiResultReceivers resultReceivers;

    private volatile SqlPipelineTask executionTask;

    public NereidsCoordinator(ConnectContext context, Analyzer analyzer,
            Planner planner, StatsErrorEstimator statsErrorEstimator, NereidsPlanner nereidsPlanner) {
        super(context, analyzer, planner, statsErrorEstimator);

        this.coordinatorContext = CoordinatorContext.build(nereidsPlanner, this);
        this.resultReceivers = MultiResultReceivers.build(coordinatorContext);
    }

    @Override
    protected void execInternal() throws Exception {
        QeProcessorImpl.INSTANCE.registerInstances(queryId, coordinatorContext.instanceNum);

        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragments
                = ThriftExecutionBuilder.plansToThrift(coordinatorContext);
        executionTask = SqlPipelineTaskBuilder.build(coordinatorContext, workerToFragments);
        executionTask.execute();
    }

    @Override
    public RowBatch getNext() throws Exception {
        return resultReceivers.getNext();
    }

    public boolean isEof() {
        return resultReceivers.isEof();
    }

    @Override
    public void cancel(Status cancelReason) {
        for (ScanNode scanNode : scanNodes) {
            scanNode.stop();
        }
        if (cancelReason.ok()) {
            throw new RuntimeException("Should use correct cancel reason, but it is " + cancelReason);
        }
        TUniqueId queryId = coordinatorContext.queryId;
        Status queryStatus = coordinatorContext.updateStatusIfOk(cancelReason);
        if (!queryStatus.ok()) {
            // Print an error stack here to know why send cancel again.
            LOG.warn("Query {} already in abnormal status {}, but received cancel again,"
                            + "so that send cancel to BE again",
                    DebugUtil.printId(queryId), queryStatus.toString(),
                    new Exception("cancel failed"));
        }
        LOG.warn("Cancel execution of query {}, this is a outside invoke, cancelReason {}",
                DebugUtil.printId(queryId), cancelReason);
        cancelInternal(cancelReason);
    }

    protected void cancelInternal(Status cancelReason) {
        if (executionTask != null) {
            executionTask.cancelSchedule(cancelReason);
        }
    }
}
