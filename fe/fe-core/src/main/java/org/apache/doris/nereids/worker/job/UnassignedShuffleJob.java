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

package org.apache.doris.nereids.worker.job;

import org.apache.doris.nereids.worker.Worker;
import org.apache.doris.nereids.worker.WorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/** UnassignedExchangeJob */
public class UnassignedShuffleJob extends AbstractUnassignedJob {
    public UnassignedShuffleJob(PlanFragment fragment, Map<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(fragment, ImmutableList.of(), exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            WorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        int expectInstanceNum = degreeOfParallelism();
        List<AssignedJob> biggestParallelChildFragment = getInstancesOfBiggestParallelChildFragment(inputJobs);

        if (expectInstanceNum > 0 && expectInstanceNum < biggestParallelChildFragment.size()) {
            List<Worker> shuffleWorkersInBiggestParallelChildFragment = shuffleWorkers(biggestParallelChildFragment);

            // random select expectInstanceNum workers in the biggestParallelChildFragment
            Function<Integer, Worker> workerSelector = shuffleWorkersInBiggestParallelChildFragment::get;
            return buildInstances(expectInstanceNum, workerSelector);
        } else {
            // select workers based on the same position as biggestParallelChildFragment
            Function<Integer, Worker> workerSelector =
                    instanceIndex -> biggestParallelChildFragment.get(instanceIndex).getAssignedWorker();
            return buildInstances(biggestParallelChildFragment.size(), workerSelector);
        }
    }

    private int degreeOfParallelism() {
        if (!fragment.getDataPartition().isPartitioned()) {
            return 1;
        }

        int expectInstanceNum = -1;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            expectInstanceNum = ConnectContext.get().getSessionVariable().getExchangeInstanceParallel();
        }
        return expectInstanceNum;
    }

    private List<AssignedJob> getInstancesOfBiggestParallelChildFragment(
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        int maxInstanceNum = -1;
        List<AssignedJob> biggestParallelChildFragment = ImmutableList.of();
        // skip broadcast exchange
        for (Entry<ExchangeNode, Collection<AssignedJob>> exchangeToChildInstances : inputJobs.asMap().entrySet()) {
            List<AssignedJob> instances = (List) exchangeToChildInstances.getValue();
            if (instances.size() > maxInstanceNum) {
                biggestParallelChildFragment = instances;
                maxInstanceNum = instances.size();
            }
        }
        return biggestParallelChildFragment;
    }

    private List<AssignedJob> buildInstances(
            int instanceNum,
            Function<Integer, Worker> workerSelector) {
        ImmutableList.Builder<AssignedJob> instances = ImmutableList.builderWithExpectedSize(instanceNum);
        for (int i = 0; i < instanceNum; i++) {
            Worker selectedWorker = workerSelector.apply(i);
            AssignedJob assignedJob = assignWorkerAndDataSources(
                    i, selectedWorker, new DefaultScanSource(ImmutableMap.of())
            );
            instances.add(assignedJob);
        }
        return instances.build();
    }

    private List<Worker> shuffleWorkers(List<AssignedJob> instances) {
        Set<Worker> candidateWorkerSet = Sets.newLinkedHashSet();
        for (AssignedJob instance : instances) {
            candidateWorkerSet.add(instance.getAssignedWorker());
        }
        List<Worker> candidateWorkers = Lists.newArrayList(candidateWorkerSet);
        Collections.shuffle(candidateWorkers);
        return candidateWorkers;
    }
}
