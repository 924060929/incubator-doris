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

import org.apache.doris.thrift.TPlanNode;

import java.util.Collections;

/** LocalExchangeNode */
public class LocalExchangeNode extends PlanNode {
    public static final String EXCHANGE_NODE = "LOCAL-EXCHANGE";

    private LocalExchangeType exchangeType;

    /**
     * use for Nereids only.
     */
    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode) {
        super(id, inputNode, EXCHANGE_NODE);
        this.offset = 0;
        this.limit = -1;
        this.conjuncts = Collections.emptyList();
        this.children.add(inputNode);
        // TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }

    /** LocalExchangeTypeRequire */
    public interface LocalExchangeTypeRequire {
        boolean satisfy(LocalExchangeType provide);

        static NoRequire noRequire() {
            return NoRequire.INSTANCE;
        }

        static RequireHash requireHash() {
            return RequireHash.INSTANCE;
        }

        static RequireSpecific requireSpecific(LocalExchangeType require) {
            return new RequireSpecific(require);
        }
    }

    /** NoRequire */
    public static class NoRequire implements LocalExchangeTypeRequire {
        public static final NoRequire INSTANCE = new NoRequire();

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            return true;
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
