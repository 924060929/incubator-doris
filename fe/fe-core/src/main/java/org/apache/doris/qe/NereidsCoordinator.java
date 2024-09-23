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
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.runtime.LoadProcessor;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.MultiResultReceivers;
import org.apache.doris.qe.runtime.SqlPipelineTask;
import org.apache.doris.qe.runtime.SqlPipelineTaskBuilder;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** NereidsCoordinator */
public class NereidsCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(NereidsCoordinator.class);

    private final CoordinatorContext coordinatorContext;

    private volatile SqlPipelineTask executionTask;

    public NereidsCoordinator(ConnectContext context, Analyzer analyzer,
            NereidsPlanner planner, StatsErrorEstimator statsErrorEstimator) {
        super(context, analyzer, planner, statsErrorEstimator);

        this.coordinatorContext = CoordinatorContext.build(planner, this);

        Preconditions.checkState(!planner.getFragments().isEmpty() && coordinatorContext.instanceNum > 0,
                "Fragment and Instance can not be empty˚");
    }

    @Override
    protected void execInternal() throws Exception {
        DataSink topDataSink = processTopSink(coordinatorContext.connectContext, coordinatorContext.planner);
        coordinatorContext.updateProfileIfPresent(SummaryProfile::setAssignFragmentTime);

        QeProcessorImpl.INSTANCE.registerInstances(queryId, coordinatorContext.instanceNum);

        setResultProcessor(topDataSink);

        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragments
                = ThriftPlansBuilder.plansToThrift(coordinatorContext);
        executionTask = SqlPipelineTaskBuilder.build(coordinatorContext, workerToFragments);
        executionTask.execute();
    }

    @Override
    public RowBatch getNext() throws Exception {
        return coordinatorContext.asQueryProcessor().getNext();
    }

    public boolean isEof() {
        return coordinatorContext.asQueryProcessor().isEof();
    }

    @Override
    public boolean isQueryCancelled() {
        return coordinatorContext.readCloneStatus().isCancelled();
    }

    @Override
    public boolean isTimeout() {
        return System.currentTimeMillis() > coordinatorContext.timeoutDeadline;
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

    @Override
    public boolean join(int timeoutS) {
        final long fixedMaxWaitTime = 30;

        long leftTimeoutS = timeoutS;
        while (leftTimeoutS > 0) {
            long waitTime = Math.min(leftTimeoutS, fixedMaxWaitTime);
            boolean awaitRes = false;
            try {
                awaitRes = executionTask.await(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Do nothing
            }
            if (awaitRes) {
                return true;
            }

            if (!executionTask.checkHealthy()) {
                return true;
            }

            leftTimeoutS -= waitTime;
        }
        return false;
    }

    @Override
    public boolean isDone() {
        return executionTask.isDone();
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        executionTask.processReportExecStatus(params);
    }

    @Override
    public Status getExecStatus() {
        return coordinatorContext.readCloneStatus();
    }

    @Override
    public void setQueryType(TQueryType type) {
        coordinatorContext.queryOptions.setQueryType(type);
    }

    @Override
    public void setLoadZeroTolerance(boolean loadZeroTolerance) {
        coordinatorContext.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
    }

    @Override
    public TQueryOptions getQueryOptions() {
        return coordinatorContext.queryOptions;
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return coordinatorContext.asLoadProcessor().loadContext.getLoadCounters();
    }

    @Override
    public List<String> getDeltaUrls() {
        return coordinatorContext.asLoadProcessor().loadContext.getDeltaUrls();
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return coordinatorContext.asLoadProcessor().loadContext.getCommitInfos();
    }

    // this method is used to provide profile metrics: `Instances Num Per BE`
    @Override
    public Map<String, Integer> getBeToInstancesNum() {
        Map<String, Integer> result = Maps.newLinkedHashMap();
        for (MultiFragmentsPipelineTask beTasks : executionTask.getChildrenTasks().values()) {
            TNetworkAddress brpcAddress = beTasks.getBackend().getBrpcAddress();
            String brpcAddrString = brpcAddress.hostname.concat(":").concat("" + brpcAddress.port);
            result.put(brpcAddrString, beTasks.getChildrenTasks().size());
        }
        return result;
    }

    protected void cancelInternal(Status cancelReason) {
        executionTask.cancelSchedule(cancelReason);
    }

    private DataSink processTopSink(ConnectContext connectContext, NereidsPlanner nereidsPlanner)
            throws AnalysisException {
        PipelineDistributedPlan topPlan = (PipelineDistributedPlan) nereidsPlanner.getDistributedPlans().last();
        DataSink topDataSink = topPlan.getFragmentJob().getFragment().getSink();
        setForArrowFlight(connectContext, topPlan, topDataSink);
        setForBroker(topPlan, topDataSink);
        return topDataSink;
    }

    private void setForArrowFlight(
            ConnectContext connectContext, PipelineDistributedPlan topPlan, DataSink topDataSink) {
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

    private void setForBroker(PipelineDistributedPlan topPlan, DataSink topDataSink) throws AnalysisException {
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

    private void setResultProcessor(DataSink topDataSink) {
        if ((topDataSink instanceof ResultSink || topDataSink instanceof ResultFileSink)) {
            coordinatorContext.setJobProcessor(MultiResultReceivers.build(coordinatorContext));
        } else {
            coordinatorContext.setJobProcessor(new LoadProcessor(coordinatorContext));
        }
    }
}
