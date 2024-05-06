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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.nereids.worker.job.AssignedJob;
import org.apache.doris.nereids.worker.job.UnassignedJob;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** DistributedPlan */
@lombok.Getter
public class DistributedPlan implements TreeNode<DistributedPlan> {
    private final UnassignedJob fragmentJob;
    private final List<AssignedJob> instanceJobs;
    private final List<DistributedPlan> inputs;

    public DistributedPlan(
            UnassignedJob fragmentJob, List<AssignedJob> instanceJobs, List<DistributedPlan> inputs) {
        this.fragmentJob = Objects.requireNonNull(fragmentJob, "fragmentJob can not be null");
        this.instanceJobs = Utils.fastToImmutableList(
                Objects.requireNonNull(instanceJobs, "instanceJobs can not be null"));
        this.inputs = Utils.fastToImmutableList(Objects.requireNonNull(inputs, "inputs can not be null"));
    }

    @Override
    public List<DistributedPlan> children() {
        return inputs;
    }

    @Override
    public DistributedPlan child(int index) {
        return inputs.get(index);
    }

    @Override
    public int arity() {
        return inputs.size();
    }

    @Override
    public <T> Optional<T> getMutableState(String key) {
        return Optional.empty();
    }

    @Override
    public void setMutableState(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DistributedPlan withChildren(List<DistributedPlan> children) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        String instancesStr = StringUtils.join(instanceJobs, ",\n");
        String instancesStrWithIndent = Utils.addLinePrefix(instancesStr, "    ");

        return "DistributedPlan(\n  parallel: " + instanceJobs.size() + ",\n  fragmentJob: " + fragmentJob
                + "\n  instanceJobs: [\n" + instancesStrWithIndent + "\n  ]\n)";
    }
}
