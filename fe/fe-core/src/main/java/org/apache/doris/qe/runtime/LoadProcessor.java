package org.apache.doris.qe.runtime;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.JobProcessor;
import org.apache.doris.qe.LoadContext;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LoadProcessor implements JobProcessor {
    private static final Logger LOG = LogManager.getLogger(LoadProcessor.class);

    public final CoordinatorContext coordinatorContext;
    public final LoadContext loadContext;
    private final SqlPipelineTask executionTask;

    public LoadProcessor(CoordinatorContext coordinatorContext, SqlPipelineTask sqlPipelineTask) {
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.loadContext = new LoadContext();
        this.executionTask = Objects.requireNonNull(sqlPipelineTask, "executionTask can not be null");

        // only we set is report success, then the backend would report the fragment status,
        // then we can not the fragment is finished, and we can return in the NereidsCoordinator::join
        coordinatorContext.queryOptions.setIsReportSuccess(true);
        // the insert into statement isn't a job
        long jobId = -1;
        TUniqueId queryId = coordinatorContext.queryId;
        Env.getCurrentEnv().getLoadManager().initJobProgress(
                jobId, queryId, coordinatorContext.instanceIds.get(),
                Utils.fastToImmutableList(coordinatorContext.backends.get().values())
        );
        Env.getCurrentEnv().getProgressManager().addTotalScanNums(
                String.valueOf(jobId), coordinatorContext.scanRangeNum.get()
        );
        LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId), coordinatorContext.backends.get().keySet());
    }

    public boolean isDone() {
        return executionTask.isDone();
    }

    public boolean join(int timeoutS) {
        long fixedMaxWaitTime = 30;

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
}
