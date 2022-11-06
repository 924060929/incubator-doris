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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.trees.expressions.Slot;

import java.util.Objects;

/**
 * GroupingFunctionSlot is used to compute the result of 'grouping(column)'.
 *
 * e.g. if we have groupingSlots array: [a, b]
 * <pre>
 * select
 *   a,
 *   b,
 *   grouping(a),    -- GroupingFunctionSlot(groupingSlotIndex=0), 0 is the index of 'a' in the groupingSlots
 *   grouping(b)     -- GroupingFunctionSlot(groupingSlotIndex=1), 1 is the index of 'b' in the groupingSlots
 * from tbl
 * group by
 * grouping sets
 * (
 *      (a, b)
 *      (   b)
 *      (    )
 * )
 * </pre>
 */
public class GroupingFunctionSlot implements ComputeByGroupingSetIdSlot {
    // this field is only used to print the used grouping slot for explain. e.g. 'a' in the grouping(a)
    private final Slot groupingSlot;

    // this field is only used for nereids planner, backend need use the literal which generate by toLongValue.
    private final int groupingSlotIndex;

    public GroupingFunctionSlot(Slot groupingSlot, int index) {
        this.groupingSlot = Objects.requireNonNull(groupingSlot, "groupingSlot can not be null");
        this.groupingSlotIndex = index;
    }

    /**
     * Compute result of 'grouping(column)' by a grouping set to long value for backend fill the result.
     *
     * Same example as class comment, there are 3 GroupingSetIdSlots:
     * 1. GroupingSetIdSlot(shouldBeErasedToNull=[false, false])
     * 2. GroupingSetIdSlot(shouldBeErasedToNull=[true, false])
     * 3. GroupingSetIdSlot(shouldBeErasedToNull=[true, true])
     *
     * If we want to compute grouping(a), we will generate 3 grouping result values, and one value mapping
     * to one grouping set: [0, 1, 1].
     * If we want to compute grouping(b), the result are [0, 0, 1]:
     *
     * grouping sets
     * (
     *      (a, b)       -- grouping(a) = 0      grouping(b) = 0
     *      (   b)       -- grouping(a) = 1      grouping(b) = 0
     *      (    )       -- grouping(a) = 1      grouping(b) = 1
     * )
     */
    @Override
    public long toLongValue(GroupingSetIdSlot groupingSetIdSlot) {
        boolean shouldBeErasedToNull = groupingSetIdSlot.shouldBeErasedToNull(groupingSlotIndex);
        return shouldBeErasedToNull ? 1 : 0;
    }

    @Override
    public String toString() {
        return "GroupingFunctionSlot(groupingSlot=" + groupingSlot + ", groupingSlotIndex=" + groupingSlotIndex + ")";
    }
}
