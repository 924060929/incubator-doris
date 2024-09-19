package org.apache.doris.qe.runtime;

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.ExecContext;
import org.apache.doris.qe.ResultReceiver;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class MultiResultReceivers {
    private static final Logger LOG = LogManager.getLogger(MultiResultReceivers.class);

    // constant fields
    private final NereidsPlanner nereidsPlanner;
    private final TUniqueId queryId;
    private final int instanceNum;
    private final long limitRows;

    // mutable field
    private final CoordinatorContext coordinatorContext;
    private final List<ResultReceiver> runningReceivers;
    private int receiverOffset;
    private long numReceivedRows;

    public MultiResultReceivers(NereidsPlanner planner, CoordinatorContext coordinatorContext,
            TUniqueId queryId, List<ResultReceiver> runningReceivers) {
        this.runningReceivers = new CopyOnWriteArrayList<>(
                Objects.requireNonNull(runningReceivers, "runningReceivers can not be null")
        );
        this.coordinatorContext = coordinatorContext;
        this.nereidsPlanner = planner;
        this.queryId = queryId;
        this.instanceNum = planner.getDistributedPlans().valueList().stream()
                .map(plan -> ((PipelineDistributedPlan) plan).getInstanceJobs().size())
                .reduce(Integer::sum)
                .get();

        List<DistributedPlan> fragments = nereidsPlanner.getDistributedPlans().valueList();
        this.limitRows = fragments.get(fragments.size() - 1)
                .getFragmentJob()
                .getFragment()
                .getPlanRoot()
                .getLimit();
    }

    public static MultiResultReceivers build(
            ExecContext execContext, CoordinatorContext coordinatorContext) {
        List<DistributedPlan> distributedPlans = execContext.planner.getDistributedPlans().valueList();
        PipelineDistributedPlan topFragment =
                (PipelineDistributedPlan) distributedPlans.get(distributedPlans.size() - 1);

        Boolean enableParallelResultSink = execContext.queryOptions.isEnableParallelResultSink()
                && topFragment.getFragmentJob().getFragment().getSink() instanceof ResultSink;

        List<AssignedJob> topInstances = topFragment.getInstanceJobs();
        List<ResultReceiver> receivers = Lists.newArrayListWithCapacity(topInstances.size());
        for (AssignedJob topInstance : topInstances) {
            DistributedPlanWorker topWorker = topInstance.getAssignedWorker();
            TNetworkAddress execBeAddr = new TNetworkAddress(topWorker.host(), topWorker.brpcPort());
            receivers.add(
                    new ResultReceiver(
                            execContext.queryId,
                            topInstance.instanceId(),
                            topWorker.id(),
                            execBeAddr,
                            execContext.timeoutDeadline,
                            execContext.planner.getCascadesContext()
                                    .getConnectContext()
                                    .getSessionVariable()
                                    .getMaxMsgSizeOfResultReceiver(),
                            enableParallelResultSink
                    )
            );
        }
        return new MultiResultReceivers(execContext.planner, coordinatorContext, execContext.queryId, receivers);
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
                    DebugUtil.printId(queryId), status.getErrorMsg());
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
            if (!nereidsPlanner.isBlockQuery() && instanceNum > 1 && hasLimit && numReceivedRows >= limitRows) {
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
    }
}
