package org.apache.doris.qe.runtime;

import com.google.common.collect.ImmutableMap;

public class LeafRuntimeTask extends AbstractRuntimeTask<Void, LeafRuntimeTask> {
    public LeafRuntimeTask() {
        super(new ChildrenRuntimeTasks<>(ImmutableMap.of()));
    }
}
