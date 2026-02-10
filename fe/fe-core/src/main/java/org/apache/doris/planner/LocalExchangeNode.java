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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExchangeNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** LocalExchangeNode */
public class LocalExchangeNode extends PlanNode {
    public static final String EXCHANGE_NODE = "LOCAL-EXCHANGE";

    private LocalExchangeType exchangeType;

    /**
     * use for Nereids only.
     */
    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode, LocalExchangeType exchangeType) {
        super(id, inputNode, EXCHANGE_NODE);
        this.offset = 0;
        this.limit = -1;
        this.conjuncts = Collections.emptyList();
        this.children.add(inputNode);
        this.exchangeType = exchangeType;
        this.fragment = inputNode.getFragment();

        List<Expr> distributeExprs = inputNode.getDistributeExprLists();
        boolean isHashShuffle = (exchangeType == LocalExchangeType.BUCKET_HASH_SHUFFLE
                || exchangeType == LocalExchangeType.EXECUTION_HASH_SHUFFLE);
        if (isHashShuffle && distributeExprs != null && !distributeExprs.isEmpty()) {
            setDistributeExprLists(distributeExprs);
            List<List<Expr>> distributeExprsList = new ArrayList<>();
            distributeExprsList.add(distributeExprs);
            setChildrenDistributeExprLists(distributeExprsList);
        }
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
        updateTupleIds(outputTupleDesc);
    }

    public void updateTupleIds(TupleDescriptor outputTupleDesc) {
        if (outputTupleDesc != null) {
            clearTupleIds();
            tupleIds.add(outputTupleDesc.getId());
        } else {
            clearTupleIds();
            tupleIds.addAll(getChild(0).getOutputTupleIds());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "type: " + exchangeType.name() + "\n";
    }

    /** LocalExchangeTypeRequire */
    public interface LocalExchangeTypeRequire {
        boolean satisfy(LocalExchangeType provide);

        LocalExchangeType preferType();

        default LocalExchangeTypeRequire autoHash() {
            return RequireHash.INSTANCE;
        }

        static NoRequire noRequire() {
            return NoRequire.INSTANCE;
        }

        static RequireHash requireHash() {
            return RequireHash.INSTANCE;
        }

        static RequireSpecific requirePassthrough() {
            return requireSpecific(LocalExchangeType.PASSTHROUGH);
        }

        static RequireSpecific requirePassToOne() {
            return requireSpecific(LocalExchangeType.PASS_TO_ONE);
        }

        static RequireSpecific requireBroadcast() {
            return requireSpecific(LocalExchangeType.BROADCAST);
        }

        static RequireSpecific requireAdaptivePassthrough() {
            return requireSpecific(LocalExchangeType.ADAPTIVE_PASSTHROUGH);
        }

        static RequireSpecific requireBucketHash() {
            return requireSpecific(LocalExchangeType.BUCKET_HASH_SHUFFLE);
        }

        static RequireSpecific requireExecutionHash() {
            return requireSpecific(LocalExchangeType.EXECUTION_HASH_SHUFFLE);
        }

        static RequireSpecific requireSpecific(LocalExchangeType require) {
            return new RequireSpecific(require);
        }

        default LocalExchangeType noopTo(LocalExchangeType defaultType) {
            LocalExchangeType preferType = preferType();
            return (preferType == LocalExchangeType.NOOP) ? defaultType : preferType;
        }
    }

    /** NoRequire */
    public static class NoRequire implements LocalExchangeTypeRequire {
        public static final NoRequire INSTANCE = new NoRequire();

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            return true;
        }

        @Override
        public LocalExchangeType preferType() {
            return LocalExchangeType.NOOP;
        }
    }

    /** RequireHash */
    public static class RequireHash implements LocalExchangeTypeRequire {
        public static final RequireHash INSTANCE = new RequireHash();

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            switch (provide) {
                case EXECUTION_HASH_SHUFFLE:
                case BUCKET_HASH_SHUFFLE:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public LocalExchangeType preferType() {
            return LocalExchangeType.EXECUTION_HASH_SHUFFLE;
        }

        @Override
        public LocalExchangeTypeRequire autoHash() {
            return this;
        }
    }

    public static class RequireSpecific implements LocalExchangeTypeRequire {
        LocalExchangeType requireType;

        public RequireSpecific(LocalExchangeType requireType) {
            this.requireType = requireType;
        }

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            return requireType == provide;
        }

        @Override
        public LocalExchangeType preferType() {
            return requireType;
        }

        @Override
        public LocalExchangeTypeRequire autoHash() {
            if (requireType == LocalExchangeType.EXECUTION_HASH_SHUFFLE
                    || requireType == LocalExchangeType.BUCKET_HASH_SHUFFLE) {
                return this;
            }
            return new RequireSpecific(LocalExchangeType.EXECUTION_HASH_SHUFFLE);
        }
    }

    public enum LocalExchangeType {
        NOOP,
        EXECUTION_HASH_SHUFFLE,
        BUCKET_HASH_SHUFFLE,
        PASSTHROUGH,
        ADAPTIVE_PASSTHROUGH,
        BROADCAST,
        PASS_TO_ONE,
        LOCAL_MERGE_SORT;
    }
}
