package org.apache.doris.qe.runtime;

import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class ChildrenRuntimeTasks<Id, C extends AbstractRuntimeTask> {
    private final Map<Id, C> childrenTasks = Maps.newConcurrentMap();

    public ChildrenRuntimeTasks(Map<Id, C> childrenTasks) {
        this.childrenTasks.putAll(childrenTasks);
    }

    public C get(Id id) {
        return childrenTasks.get(id);
    }

    public List<C> allTasks() {
        return Utils.fastToImmutableList(childrenTasks.values());
    }
}
