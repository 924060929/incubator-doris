package org.apache.doris.qe.runtime;

import org.apache.doris.system.Backend;

import java.util.concurrent.atomic.AtomicBoolean;

public class SingleFragmentPipelineTask extends LeafRuntimeTask {
    // immutable parameters
    private final Backend backend;
    private final int fragmentId;

    // mutate states
    private final AtomicBoolean finished = new AtomicBoolean();

    public SingleFragmentPipelineTask(Backend backend, int fragmentId) {
        this.backend = backend;
        this.fragmentId = fragmentId;
    }

    public AtomicBoolean getFinished() {
        return finished;
    }

    public boolean setFinished() {
        return finished.compareAndSet(false, true);
    }

    public Backend getBackend() {
        return backend;
    }

    public int getFragmentId() {
        return fragmentId;
    }

    public boolean isFinished() {
        return finished.get();
    }
}
