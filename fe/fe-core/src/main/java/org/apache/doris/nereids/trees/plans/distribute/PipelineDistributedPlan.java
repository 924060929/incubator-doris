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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.thrift.TExplainLevel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** PipelineDistributedPlan */
public class PipelineDistributedPlan extends DistributedPlan {
    protected final List<AssignedJob> instanceJobs;
    // current, we only support all instances of the same fragment reuse the same destination
    private List<AssignedJob> destinations;

    public PipelineDistributedPlan(
            UnassignedJob fragmentJob,
            List<AssignedJob> instanceJobs,
            ListMultimap<ExchangeNode, DistributedPlan> inputs) {
        super(fragmentJob, inputs);
        this.instanceJobs = Utils.fastToImmutableList(
                Objects.requireNonNull(instanceJobs, "instanceJobs can not be null")
        );
        this.destinations = ImmutableList.of();
    }

    public List<AssignedJob> getInstanceJobs() {
        return instanceJobs;
    }

    public List<AssignedJob> getDestinations() {
        return destinations;
    }

    public void setDestinations(List<AssignedJob> destinations) {
        this.destinations = destinations;
    }

    @Override
    public int hashCode() {
        return fragmentJob.getFragment().getFragmentId().asInt();
    }

    @Override
    public String toString(int displayFragmentId) {
        StringBuilder instancesStr = new StringBuilder();
        for (int i = 0; i < instanceJobs.size(); i++) {
            instancesStr.append(instanceJobs.get(i).toString(false));
            if (i + 1 < instanceJobs.size()) {
                instancesStr.append(",\n");
            }
        }
        String instancesStrWithIndent = Utils.addLinePrefix(instancesStr.toString(), "    ");

        String explainString = Utils.addLinePrefix(
                fragmentJob.getFragment().getExplainString(TExplainLevel.VERBOSE).trim(), "  "
        );

        AtomicInteger bucketNum = new AtomicInteger(0);
        String destinationStr = destinations.stream()
                .map(destination -> "    "
                        + "#" + bucketNum.getAndIncrement() + ": "
                        + DebugUtil.printId(destination.instanceId()))
                .collect(Collectors.joining(",\n"));
        return "PipelineDistributedPlan(\n"
                + "  id: " + displayFragmentId + ",\n"
                + "  parallel: " + instanceJobs.size() + ",\n"
                + "  fragmentJob: " + fragmentJob + ",\n"
                + "  destinations: [" + (destinationStr.isEmpty() ? "" : "\n" + destinationStr + "\n  ") + "],\n"
                + "  fragment: {\n"
                + "  " + explainString + "\n"
                + "  },\n"
                + "  instanceJobs: [\n" + instancesStrWithIndent + "\n"
                + "  ]\n"
                + ")";
    }
}
