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

package org.apache.doris.qe;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import java.util.List;

/** ExecGlobalInfo */
public class ExecContext {
    public final ConnectContext connectContext;
    public final NereidsPlanner planner;
    public final TUniqueId queryId;
    public final TQueryGlobals queryGlobals;
    public final TQueryOptions queryOptions;
    public final TDescriptorTable descriptorTable;
    public final List<TPipelineWorkloadGroup> workloadGroups;

    // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
    // else use exec_plan_fragments directly.
    // we choose #fragments > 1 because in some cases
    // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
    // For example: select * from numbers("number"="10") will generate ExchangeNode and
    // TableValuedFunctionScanNode, we should ensure TableValuedFunctionScanNode does not
    // send data until ExchangeNode is ready to receive.
    public final boolean twoPhaseExecution;

    public ExecContext(
            ConnectContext connectContext,
            NereidsPlanner planner,
            TQueryGlobals queryGlobals,
            TQueryOptions queryOptions,
            TDescriptorTable descriptorTable,
            List<TPipelineWorkloadGroup> workloadGroups) {
        this.connectContext = connectContext;
        this.planner = planner;
        this.queryId = connectContext.queryId();
        this.queryGlobals = queryGlobals;
        this.queryOptions = queryOptions;
        this.descriptorTable = descriptorTable;
        this.workloadGroups = workloadGroups;
        this.twoPhaseExecution = planner.getDistributedPlans().size() > 1;
    }
}
