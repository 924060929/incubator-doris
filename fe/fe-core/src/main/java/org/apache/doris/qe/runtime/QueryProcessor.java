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

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.JobProcessor;
import org.apache.doris.qe.ResultReceiver;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueryProcessor implements JobProcessor {
    private static final Logger LOG = LogManager.getLogger(QueryProcessor.class);

    // constant fields
    private final long limitRows;
    private final SqlPipelineTask sqlPipelineTask;

    // mutable field
    private final CoordinatorContext coordinatorContext;
    private final List<ResultReceiver> runningReceivers;
    private int receiverOffset;
    private long numReceivedRows;

    public QueryProcessor(CoordinatorContext coordinatorContext,
            SqlPipelineTask sqlPipelineTask, List<ResultReceiver> runningReceivers) {
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.sqlPipelineTask = Objects.requireNonNull(sqlPipelineTask, "sqlPipelineTask can not be null");
        this.runningReceivers = new CopyOnWriteArrayList<>(
                Objects.requireNonNull(runningReceivers, "runningReceivers can not be null")
        );

        List<DistributedPlan> fragments = coordinatorContext.planner.getDistributedPlans().valueList();
        this.limitRows = fragments.get(fragments.size() - 1)
                .getFragmentJob()
                .getFragment()
                .getPlanRoot()
                .getLimit();
    }

    public static QueryProcessor build(CoordinatorContext coordinatorContext, SqlPipelineTask sqlPipelineTask) {
        List<DistributedPlan> distributedPlans = coordinatorContext.planner.getDistributedPlans().valueList();
        PipelineDistributedPlan topFragment =
                (PipelineDistributedPlan) distributedPlans.get(distributedPlans.size() - 1);

        Boolean enableParallelResultSink = coordinatorContext.queryOptions.isEnableParallelResultSink()
                && topFragment.getFragmentJob().getFragment().getSink() instanceof ResultSink;

        List<AssignedJob> topInstances = topFragment.getInstanceJobs();
        List<ResultReceiver> receivers = Lists.newArrayListWithCapacity(topInstances.size());
        for (AssignedJob topInstance : topInstances) {
            DistributedPlanWorker topWorker = topInstance.getAssignedWorker();
            TNetworkAddress execBeAddr = new TNetworkAddress(topWorker.host(), topWorker.brpcPort());
            receivers.add(
                    new ResultReceiver(
                            coordinatorContext.queryId,
                            topInstance.instanceId(),
                            topWorker.id(),
                            execBeAddr,
                            coordinatorContext.timeoutDeadline,
                            coordinatorContext.planner.getCascadesContext()
                                    .getConnectContext()
                                    .getSessionVariable()
                                    .getMaxMsgSizeOfResultReceiver(),
                            enableParallelResultSink
                    )
            );
        }
        return new QueryProcessor(coordinatorContext, sqlPipelineTask, receivers);
    }

    public boolean isEof() {
        return runningReceivers.isEmpty();
    }

    public RowBatch getNext() throws UserException, TException, RpcException {
        if (runningReceivers.isEmpty()) {
            throw new UserException("There is no receiver.");
        }

        ResultReceiver receiver = runningReceivers.get(receiverOffset);
        Status status = new Status();
        RowBatch resultBatch = receiver.getNext(status);
        if (!status.ok()) {
            LOG.warn("Query {} coordinator get next fail, {}, need cancel.",
                    DebugUtil.printId(coordinatorContext.queryId), status.getErrorMsg());
        }

        Status copyStatus = coordinatorContext.updateStatusIfOk(status);
        if (!copyStatus.ok()) {
            if (Strings.isNullOrEmpty(copyStatus.getErrorMsg())) {
                copyStatus.rewriteErrorMsg();
            }
            if (copyStatus.isRpcError()) {
                throw new RpcException(null, copyStatus.getErrorMsg());
            } else {
                String errMsg = copyStatus.getErrorMsg();
                LOG.warn("query failed: {}", errMsg);

                // hide host info exclude localhost
                if (errMsg.contains("localhost")) {
                    throw new UserException(errMsg);
                }
                int hostIndex = errMsg.indexOf("host");
                if (hostIndex != -1) {
                    errMsg = errMsg.substring(0, hostIndex);
                }
                throw new UserException(errMsg);
            }
        }

        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().dryRunQuery) {
            if (resultBatch.isEos()) {
                numReceivedRows += resultBatch.getQueryStatistics().getReturnedRows();
            }
        } else if (resultBatch.getBatch() != null) {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
        }

        if (resultBatch.isEos()) {
            runningReceivers.remove(receiver);
            if (!runningReceivers.isEmpty()) {
                resultBatch.setEos(false);
            }

            // if this query is a block query do not cancel.
            boolean hasLimit = limitRows > 0;
            if (!coordinatorContext.planner.isBlockQuery()
                    && coordinatorContext.instanceNum > 1
                    && hasLimit && numReceivedRows >= limitRows) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("no block query, return num >= limit rows, need cancel");
                }
                coordinatorContext.cancelSchedule(new Status(TStatusCode.LIMIT_REACH, "query reach limit"));
            }
        }

        if (!runningReceivers.isEmpty()) {
            receiverOffset = (receiverOffset + 1) % runningReceivers.size();
        }
        return resultBatch;
    }

    public void cancel(Status cancelReason) {
        for (ResultReceiver receiver : runningReceivers) {
            receiver.cancel(cancelReason);
        }

        for (MultiFragmentsPipelineTask fragmentsTask : sqlPipelineTask.getChildrenTasks().values()) {
            fragmentsTask.cancelExecute(cancelReason);
        }
    }

    public int getReceiverOffset() {
        return receiverOffset;
    }

    public long getNumReceivedRows() {
        return numReceivedRows;
    }
}
