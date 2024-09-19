package org.apache.doris.qe;

import org.apache.doris.common.Status;

import java.util.Objects;

public class CoordinatorContext {
    private NereidsCoordinator coordinator;

    private volatile Status status;

    public CoordinatorContext(NereidsCoordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator can not be null");
        this.status = new Status();
    }

    public void cancelSchedule(Status cancelReason) {
        coordinator.cancelInternal(cancelReason);
    }

    public synchronized Status readCloneStatus() {
        return new Status(status.getErrorCode(), status.getErrorMsg());
    }

    public synchronized Status updateStatusIfOk(Status newStatus) {
        // If query is done, we will ignore their cancelled updates, and let the remote fragments to clean up async.
        Status originStatus = readCloneStatus();
        if (coordinator.isEof() && newStatus.isCancelled()) {
            return originStatus;
        }
        // nothing to update
        if (newStatus.ok()) {
            return originStatus;
        }

        // don't override an error status; also, cancellation has already started
        if (!this.status.ok()) {
            return originStatus;
        }

        status = new Status(newStatus.getErrorCode(), newStatus.getErrorMsg());
        coordinator.cancelInternal(readCloneStatus());
        return originStatus;
    }
}
