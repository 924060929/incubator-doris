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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Map;
import java.util.function.Function;

/** SlotTypeReplacer */
public class SlotTypeReplacer extends DefaultPlanRewriter<Map<Integer, Function<Slot, Slot>>> {
    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            Map<Integer, Function<Slot, Slot>> context) {
        return super.visitLogicalCTEAnchor(cteAnchor, context);
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, Map<Integer, Function<Slot, Slot>> context) {
        return super.visitLogicalUnion(union, context);
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Map<Integer, Function<Slot, Slot>> context) {
        return super.visitLogicalOlapScan(olapScan, context);
    }
}
