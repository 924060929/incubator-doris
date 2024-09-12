package org.apache.doris.qe.runtime;

import java.util.Objects;
import java.util.concurrent.Callable;

public abstract class AbstractRuntimeTask<ChildId, Child extends AbstractRuntimeTask<?, ?>> {
    protected final ChildrenRuntimeTasks<ChildId, Child> childrenTasks;

    public AbstractRuntimeTask(ChildrenRuntimeTasks<ChildId, Child> childrenTasks) {
        this.childrenTasks = Objects.requireNonNull(childrenTasks, "childrenTasks can not be null");
    }

    public void execute() throws Throwable {
        for (Child childrenTask : childrenTasks.allTasks()) {
            childrenTask.execute();
        }
    }

    protected Child childTask(ChildId childId) {
        return childrenTasks.get(childId);
    }

    protected final void withLock(Runnable callback) {
        withLock(() -> {
            callback.run();
            return null;
        });
    }

    protected final <T> T withLock(Callable<T> callback) {
        synchronized (this) {
            try {
                return callback.call();
            } catch (Throwable t) {
                throw new IllegalStateException(t.getMessage(), t);
            }
        }
    }
}
