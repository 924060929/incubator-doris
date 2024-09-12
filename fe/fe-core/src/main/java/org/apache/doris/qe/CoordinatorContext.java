package org.apache.doris.qe;

import org.apache.doris.common.Status;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CoordinatorContext {
    private final Supplier<Boolean> isEof;
    private final Consumer<Status> cancelCallback;

    private volatile Status status;

    public CoordinatorContext(
            Supplier<Boolean> isEof,
            Consumer<Status> cancelCallback) {
        this.isEof = Objects.requireNonNull(isEof, "isEof can not be null");
        this.cancelCallback = Objects.requireNonNull(cancelCallback, "cancelCallback can not be null");
        this.status = new Status();
    }

    public void cancelSchedule(Status cancelReason) {
        cancelCallback.accept(cancelReason);
    }

    public synchronized Status readCloneStatus() {
        return new Status(status.getErrorCode(), status.getErrorMsg());
    }

    public synchronized Status updateStatusIfOk(Status newStatus) {
        // The query is done and we are just waiting for remote fragments to clean up.
        // Ignore their cancelled updates.
        if (isEof.get() && newStatus.isCancelled()) {
            return readCloneStatus();
        }
        // nothing to update
        if (newStatus.ok()) {
            return readCloneStatus();
        }

        // don't override an error status; also, cancellation has already started
        if (!this.status.ok()) {
            return readCloneStatus();
        }

        status = new Status(newStatus.getErrorCode(), newStatus.getErrorMsg());
        Status status = readCloneStatus();
        cancelCallback.accept(status);
        return status;
    }
}
