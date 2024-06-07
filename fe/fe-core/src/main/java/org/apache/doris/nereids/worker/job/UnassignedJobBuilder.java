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

import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.worker.LoadBalanceScanWorkerSelector;
import org.apache.doris.nereids.worker.ScanWorkerSelector;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

import com.google.common.collect.Maps;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * UnassignedJobBuilder.
 * build UnassignedJob by fragment
 */
public class UnassignedJobBuilder {
    private final ScanWorkerSelector scanWorkerSelector = new LoadBalanceScanWorkerSelector();

    /**
     * build job from fragment.
     */
    public static FragmentIdMapping<UnassignedJob> buildJobs(FragmentIdMapping<PlanFragment> fragments) {
        UnassignedJobBuilder builder = new UnassignedJobBuilder();

        FragmentLineage fragmentLineage = buildFragmentLineage(fragments);
        FragmentIdMapping<UnassignedJob> unassignedJobs = new FragmentIdMapping<>();

        // build from leaf to parent
        for (Entry<PlanFragmentId, PlanFragment> kv : fragments.entrySet()) {
            PlanFragmentId fragmentId = kv.getKey();
            PlanFragment fragment = kv.getValue();

            Map<ExchangeNode, UnassignedJob> inputJobs = findInputJobs(
                    fragmentLineage, fragmentId, unassignedJobs);
            UnassignedJob unassignedJob = builder.buildJob(fragment, inputJobs);
            unassignedJobs.put(fragmentId, unassignedJob);
        }
        return unassignedJobs;
    }

    private UnassignedJob buildJob(
            PlanFragment planFragment, Map<ExchangeNode, UnassignedJob> inputJobs) {
        List<ScanNode> scanNodes = collectScanNodesInThisFragment(planFragment);
        if (planFragment.specifyInstances.isPresent()) {
            return buildSpecifyInstancesJob(planFragment, scanNodes, inputJobs);
        } else if (!scanNodes.isEmpty() || isLeafFragment(planFragment)) {
            return buildLeafOrScanJob(planFragment, scanNodes, inputJobs);
        } else {
            return buildShuffleJob(planFragment, inputJobs);
        }
    }

    private UnassignedJob buildLeafOrScanJob(
            PlanFragment planFragment, List<ScanNode> scanNodes, Map<ExchangeNode, UnassignedJob> inputJobs) {
        int olapScanNodeNum = olapScanNodeNum(scanNodes);
        if (!scanNodes.isEmpty() && olapScanNodeNum == scanNodes.size()) {
            // we need assign a backend which contains the data,
            // so that the OlapScanNode can find the data in the backend
            // e.g. select * from olap_table
            return buildScanLocalTableJob(planFragment, scanNodes, inputJobs, scanWorkerSelector);
        } else if (scanNodes.isEmpty()) {
            // select constant without table,
            // e.g. select 100 union select 200
            return buildQueryConstantJob(planFragment);
        } else if (olapScanNodeNum == 0) {
            // only scan external tables or cloud tables or table valued functions
            // e,g. select * from numbers('number'='100')
            return buildScanRemoteTableJob(planFragment, scanNodes, inputJobs, scanWorkerSelector);
        } else {
            throw new IllegalStateException(
                    "Unsupported fragment which contains multiple scan nodes and some of them are not OlapScanNode"
            );
        }
    }

    private UnassignedJob buildSpecifyInstancesJob(
            PlanFragment planFragment, List<ScanNode> scanNodes, Map<ExchangeNode, UnassignedJob> inputJobs) {
        return new UnassignedSpecifyInstancesJob(planFragment, scanNodes, inputJobs);
    }

    private UnassignedScanNativeTableJob buildScanLocalTableJob(
            PlanFragment planFragment, List<ScanNode> scanNodes, Map<ExchangeNode, UnassignedJob> inputJobs,
            ScanWorkerSelector scanWorkerSelector) {
        return new UnassignedScanNativeTableJob(planFragment, scanNodes, inputJobs, scanWorkerSelector);
    }

    private List<ScanNode> collectScanNodesInThisFragment(PlanFragment planFragment) {
        return planFragment.getPlanRoot().collectInCurrentFragment(ScanNode.class::isInstance);
    }

    private int olapScanNodeNum(List<ScanNode> scanNodes) {
        int olapScanNodeNum = 0;
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                olapScanNodeNum++;
            }
        }
        return olapScanNodeNum;
    }

    private boolean isLeafFragment(PlanFragment planFragment) {
        return planFragment.getChildren().isEmpty();
    }

    private UnassignedQueryConstantJob buildQueryConstantJob(PlanFragment planFragment) {
        return new UnassignedQueryConstantJob(planFragment);
    }

    private UnassignedJob buildScanRemoteTableJob(
            PlanFragment planFragment, List<ScanNode> scanNodes, Map<ExchangeNode, UnassignedJob> inputJobs,
            ScanWorkerSelector scanWorkerSelector) {
        return new UnassignedScanRemoteTableJob(planFragment, scanNodes, inputJobs, scanWorkerSelector);
    }

    private UnassignedShuffleJob buildShuffleJob(
            PlanFragment planFragment, Map<ExchangeNode, UnassignedJob> inputJobs) {
        return new UnassignedShuffleJob(planFragment, inputJobs);
    }

    private static Map<ExchangeNode, UnassignedJob> findInputJobs(
            FragmentLineage lineage, PlanFragmentId fragmentId, FragmentIdMapping<UnassignedJob> unassignedJobs) {
        Map<ExchangeNode, UnassignedJob> inputJobs = new IdentityHashMap<>();
        Map<PlanNodeId, ExchangeNode> exchangeNodes = lineage.parentFragmentToExchangeNode.get(fragmentId);
        if (exchangeNodes != null) {
            for (Entry<PlanNodeId, ExchangeNode> idToExchange : exchangeNodes.entrySet()) {
                PlanNodeId exchangeId = idToExchange.getKey();
                ExchangeNode exchangeNode = idToExchange.getValue();
                PlanFragmentId childFragmentId = lineage.exchangeToChildFragment.get(exchangeId);
                inputJobs.put(exchangeNode, unassignedJobs.get(childFragmentId));
            }
        }
        return inputJobs;
    }

    private static List<ExchangeNode> collectExchangeNodesInThisFragment(PlanFragment planFragment) {
        return planFragment
                .getPlanRoot()
                .collectInCurrentFragment(ExchangeNode.class::isInstance);
    }

    private static FragmentLineage buildFragmentLineage(
            FragmentIdMapping<PlanFragment> fragments) {
        Map<PlanNodeId, PlanFragmentId> exchangeToChildFragment = new LinkedHashMap<>();
        FragmentIdMapping<Map<PlanNodeId, ExchangeNode>> parentFragmentToExchangeNode = new FragmentIdMapping<>();

        for (PlanFragment fragment : fragments.values()) {
            PlanFragmentId fragmentId = fragment.getFragmentId();

            // 1. link child fragment to exchange node
            DataSink sink = fragment.getSink();
            if (sink instanceof DataStreamSink) {
                PlanNodeId exchangeNodeId = sink.getExchNodeId();
                exchangeToChildFragment.put(exchangeNodeId, fragmentId);
            }

            // 2. link parent fragment to exchange node
            List<ExchangeNode> exchangeNodes = collectExchangeNodesInThisFragment(fragment);
            Map<PlanNodeId, ExchangeNode> exchangeNodesInFragment = Maps.newLinkedHashMap();
            for (ExchangeNode exchangeNode : exchangeNodes) {
                exchangeNodesInFragment.put(exchangeNode.getId(), exchangeNode);
            }
            parentFragmentToExchangeNode.put(fragmentId, exchangeNodesInFragment);
        }

        return new FragmentLineage(fragments, parentFragmentToExchangeNode, exchangeToChildFragment);
    }

    // the class support find exchange nodes in the fragment, and find child fragment by exchange node id
    private static class FragmentLineage {
        private final FragmentIdMapping<PlanFragment> idToFragments;
        private final FragmentIdMapping<Map<PlanNodeId, ExchangeNode>> parentFragmentToExchangeNode;
        private final Map<PlanNodeId, PlanFragmentId> exchangeToChildFragment;

        public FragmentLineage(
                FragmentIdMapping<PlanFragment> idToFragments,
                FragmentIdMapping<Map<PlanNodeId, ExchangeNode>> parentFragmentToExchangeNode,
                Map<PlanNodeId, PlanFragmentId> exchangeToChildFragment) {
            this.idToFragments = idToFragments;
            this.parentFragmentToExchangeNode = parentFragmentToExchangeNode;
            this.exchangeToChildFragment = exchangeToChildFragment;
        }
    }
}
