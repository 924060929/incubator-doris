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

package org.apache.doris.qe.runtime;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.hive.HMSTransaction;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.JobProcessor;
import org.apache.doris.qe.LoadContext;
import org.apache.doris.thrift.TFragmentInstanceReport;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LoadProcessor implements JobProcessor {
    private static final Logger LOG = LogManager.getLogger(LoadProcessor.class);

    public final CoordinatorContext coordinatorContext;
    public final LoadContext loadContext;
    public final long jobId;
    private final SqlPipelineTask executionTask;

    // this latch is used to wait finish for load, for example, insert into statement
    // MarkedCountDownLatch:
    //  key: fragmentId, value: backendId
    private volatile MarkedCountDownLatch<Integer, Long> latch;

    private final Map<BackendFragmentId, SingleFragmentPipelineTask> backendFragmentTasks;

    public LoadProcessor(CoordinatorContext coordinatorContext, long jobId, SqlPipelineTask sqlPipelineTask) {
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.loadContext = new LoadContext();
        this.executionTask = Objects.requireNonNull(sqlPipelineTask, "executionTask can not be null");

        // only we set is report success, then the backend would report the fragment status,
        // then we can not the fragment is finished, and we can return in the NereidsCoordinator::join
        coordinatorContext.queryOptions.setIsReportSuccess(true);
        // the insert into statement isn't a job
        this.jobId = jobId;
        TUniqueId queryId = coordinatorContext.queryId;
        Env.getCurrentEnv().getLoadManager().initJobProgress(
                jobId, queryId, coordinatorContext.instanceIds.get(),
                Utils.fastToImmutableList(coordinatorContext.backends.get().values())
        );
        Env.getCurrentEnv().getProgressManager().addTotalScanNums(
                String.valueOf(jobId), coordinatorContext.scanRangeNum.get()
        );

        this.backendFragmentTasks = buildBackendFragmentTasks(executionTask);

        this.latch = new MarkedCountDownLatch<>(this.backendFragmentTasks.size());
        for (BackendFragmentId backendFragmentId : this.backendFragmentTasks.keySet()) {
            this.latch.addMark(backendFragmentId.fragmentId, backendFragmentId.backendId);
        }

        LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId), coordinatorContext.backends.get().keySet());
    }

    public boolean isDone() {
        return latch.getCount() == 0;
    }

    public boolean join(int timeoutS) {
        long fixedMaxWaitTime = 30;

        long leftTimeoutS = timeoutS;
        while (leftTimeoutS > 0) {
            long waitTime = Math.min(leftTimeoutS, fixedMaxWaitTime);
            boolean awaitRes = false;
            try {
                awaitRes = await(waitTime, TimeUnit.SECONDS);
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

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (latch == null) {
            return true;
        }
        return latch.await(timeout, unit);
    }

    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        SingleFragmentPipelineTask fragmentTask = backendFragmentTasks.get(
                new BackendFragmentId(params.getBackendId(), params.getFragmentId()));
        if (fragmentTask == null || !fragmentTask.processReportExecStatus(params)) {
            return;
        }
        TUniqueId queryId = coordinatorContext.queryId;
        Status status = new Status(params.status);
        // for now, abort the query if we see any error except if the error is cancelled
        // and returned_all_results_ is true.
        // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
        if (!status.ok()) {
            if (coordinatorContext.isEof() && status.isCancelled()) {
                LOG.warn("Query {} has returned all results, fragment_id={} instance_id={}, be={}"
                                + " is reporting failed status {}",
                        DebugUtil.printId(queryId), params.getFragmentId(),
                        DebugUtil.printId(params.getFragmentInstanceId()),
                        params.getBackendId(),
                        status.toString());
            } else {
                LOG.warn("one instance report fail, query_id={} fragment_id={} instance_id={}, be={},"
                                + " error message: {}",
                        DebugUtil.printId(queryId), params.getFragmentId(),
                        DebugUtil.printId(params.getFragmentInstanceId()),
                        params.getBackendId(), status.toString());
                coordinatorContext.updateStatusIfOk(status);
            }
        }
        LoadContext loadContext = coordinatorContext.asLoadProcessor().loadContext;
        if (params.isSetDeltaUrls()) {
            loadContext.updateDeltaUrls(params.getDeltaUrls());
        }
        if (params.isSetLoadCounters()) {
            loadContext.updateLoadCounters(params.getLoadCounters());
        }
        if (params.isSetTrackingUrl()) {
            loadContext.updateTrackingUrl(params.getTrackingUrl());
        }
        if (params.isSetTxnId()) {
            loadContext.updateTransactionId(params.getTxnId());
        }
        if (params.isSetLabel()) {
            loadContext.updateLabel(params.getLabel());
        }
        if (params.isSetExportFiles()) {
            loadContext.addExportFiles(params.getExportFiles());
        }
        if (params.isSetCommitInfos()) {
            loadContext.updateCommitInfos(params.getCommitInfos());
        }
        if (params.isSetErrorTabletInfos()) {
            loadContext.updateErrorTabletInfos(params.getErrorTabletInfos());
        }
        long txnId = loadContext.getTransactionId();
        if (params.isSetHivePartitionUpdates()) {
            ((HMSTransaction) Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().getTxnById(txnId))
                    .updateHivePartitionUpdates(params.getHivePartitionUpdates());
        }
        if (params.isSetIcebergCommitDatas()) {
            ((IcebergTransaction) Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().getTxnById(txnId))
                    .updateIcebergCommitData(params.getIcebergCommitDatas());
        }

        if (fragmentTask.isDone()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query {} fragment {} is marked done",
                        DebugUtil.printId(queryId), params.getFragmentId());
            }
            latch.markedCountDown(params.getFragmentId(), params.getBackendId());
        }

        if (params.isSetLoadedRows() && jobId != -1) {
            if (params.isSetFragmentInstanceReports()) {
                for (TFragmentInstanceReport report : params.getFragmentInstanceReports()) {
                    Env.getCurrentEnv().getLoadManager().updateJobProgress(
                            jobId, params.getBackendId(), params.getQueryId(), report.getFragmentInstanceId(),
                            params.getLoadedRows(), params.getLoadedBytes(), params.isDone());
                    Env.getCurrentEnv().getProgressManager().updateProgress(String.valueOf(jobId),
                            params.getQueryId(), report.getFragmentInstanceId(), report.getNumFinishedRange());
                }
            } else {
                Env.getCurrentEnv().getLoadManager().updateJobProgress(
                        jobId, params.getBackendId(), params.getQueryId(), params.getFragmentInstanceId(),
                        params.getLoadedRows(), params.getLoadedBytes(), params.isDone());
                Env.getCurrentEnv().getProgressManager().updateProgress(String.valueOf(jobId),
                        params.getQueryId(), params.getFragmentInstanceId(), params.getFinishedScanRanges());
            }
        }
    }

    private Map<BackendFragmentId, SingleFragmentPipelineTask> buildBackendFragmentTasks(
            SqlPipelineTask executionTask) {
        ImmutableMap.Builder<BackendFragmentId, SingleFragmentPipelineTask> backendFragmentTasks
                = ImmutableMap.builder();
        for (Entry<Long, MultiFragmentsPipelineTask> backendTask : executionTask.getChildrenTasks().entrySet()) {
            Long backendId = backendTask.getKey();
            for (Entry<Integer, SingleFragmentPipelineTask> fragmentIdToTask : backendTask.getValue()
                    .getChildrenTasks().entrySet()) {
                Integer fragmentId = fragmentIdToTask.getKey();
                SingleFragmentPipelineTask fragmentTask = fragmentIdToTask.getValue();
                backendFragmentTasks.put(new BackendFragmentId(backendId, fragmentId), fragmentTask);
            }
        }
        return backendFragmentTasks.build();
    }

    @Override
    public void cancel(Status cancelReason) {
        for (MultiFragmentsPipelineTask fragmentsTask : executionTask.getChildrenTasks().values()) {
            fragmentsTask.cancelExecute(cancelReason);
        }
        latch.countDownToZero(new Status());
    }
}
