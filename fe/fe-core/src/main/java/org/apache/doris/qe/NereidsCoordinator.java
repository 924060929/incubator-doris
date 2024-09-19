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
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Status;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.MultiResultReceivers;
import org.apache.doris.qe.runtime.SqlPipelineTask;
import org.apache.doris.qe.runtime.SqlPipelineTaskBuilder;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
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
        this.coordinatorContext.updateProfileIfPresent(SummaryProfile::setAssignFragmentTime);

        this.resultReceivers = MultiResultReceivers.build(coordinatorContext);

        Preconditions.checkState(!planner.getFragments().isEmpty() && coordinatorContext.instanceNum > 0,
                "Fragment and Instance can not be empty˚");
    }

    @Override
    protected void execInternal() throws Exception {
        QeProcessorImpl.INSTANCE.registerInstances(queryId, coordinatorContext.instanceNum);

        processTopFragment(coordinatorContext.connectContext, coordinatorContext.planner);
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragments
                = ThriftPlansBuilder.plansToThrift(coordinatorContext);
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

    // this method is used to provide profile metrics: `Instances Num Per BE`
    @Override
    public Map<String, Integer> getBeToInstancesNum() {
        if (executionTask == null) {
            return ImmutableMap.of();
        }
        Map<String, Integer> result = Maps.newLinkedHashMap();
        for (MultiFragmentsPipelineTask beTasks : executionTask.getChildrenTasks().values()) {
            TNetworkAddress brpcAddress = beTasks.getBackend().getBrpcAddress();
            String brpcAddrString = brpcAddress.hostname.concat(":").concat("" + brpcAddress.port);
            result.put(brpcAddrString, beTasks.getChildrenTasks().size());
        }
        return result;
    }

    protected void cancelInternal(Status cancelReason) {
        if (executionTask != null) {
            executionTask.cancelSchedule(cancelReason);
        }
    }

    private void processTopFragment(ConnectContext connectContext, NereidsPlanner nereidsPlanner)
            throws AnalysisException {
        PipelineDistributedPlan topPlan = (PipelineDistributedPlan) nereidsPlanner.getDistributedPlans().last();
        setForArrowFlight(connectContext, topPlan);
        setForBroker(topPlan);
    }

    private void setForArrowFlight(ConnectContext connectContext, PipelineDistributedPlan topPlan) {
        DataSink topDataSink = topPlan.getFragmentJob().getFragment().getSink();
        if (topDataSink instanceof ResultSink || topDataSink instanceof ResultFileSink) {
            if (connectContext != null && !connectContext.isReturnResultFromLocal()) {
                Preconditions.checkState(connectContext.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL));

                AssignedJob firstInstance = topPlan.getInstanceJobs().get(0);
                BackendWorker worker = (BackendWorker) firstInstance.getAssignedWorker();
                Backend backend = worker.getBackend();

                connectContext.setFinstId(firstInstance.instanceId());
                if (backend.getArrowFlightSqlPort() < 0) {
                    throw new IllegalStateException("be arrow_flight_sql_port cannot be empty.");
                }
                connectContext.setResultFlightServerAddr(backend.getArrowFlightAddress());
                connectContext.setResultInternalServiceAddr(backend.getBrpcAddress());
                connectContext.setResultOutputExprs(topPlan.getFragmentJob().getFragment().getOutputExprs());
            }
        }
    }

    private void setForBroker(PipelineDistributedPlan topPlan) throws AnalysisException {
        DataSink topDataSink = topPlan.getFragmentJob().getFragment().getSink();
        if (topDataSink instanceof ResultFileSink
                && ((ResultFileSink) topDataSink).getStorageType() == StorageBackend.StorageType.BROKER) {
            // set the broker address for OUTFILE sink
            ResultFileSink topResultFileSink = (ResultFileSink) topDataSink;
            DistributedPlanWorker worker = topPlan.getInstanceJobs().get(0).getAssignedWorker();
            FsBroker broker = Env.getCurrentEnv().getBrokerMgr()
                    .getBroker(topResultFileSink.getBrokerName(), worker.host());
            topResultFileSink.setBrokerAddr(broker.host, broker.port);
        }
    }
}
