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
import org.apache.doris.nereids.util.BitUtils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * GroupingFunctionSlot is used to compute the result of 'grouping(column)'.
 *
 * e.g. if we have groupingSlots array: [a, b]
 * <pre>
 * select
 *   a,
 *   b,
 *   grouping_id(a),    -- GroupingIdFunctionSlot(groupingSlotIndexes=[0]), 0 is the index of 'a' in groupingSlots
 *   grouping_id(b, a)  -- GroupingIdFunctionSlot(groupingSlotIndexes=[1, 0]), 1 is the index of 'b'
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
public class GroupingIdFunctionSlot implements ComputeByGroupingSetIdSlot {
    // this field is only used to print the used grouping slots for explain. e.g. 'a' and 'b' in the grouping_id(a, b)
    private final List<Slot> groupingSlots;

    // this field is only used for nereids planner, backend need use the literal which generate by toLongValue.
    private final List<Integer> groupingSlotIndexes;

    public GroupingIdFunctionSlot(List<Slot> groupingSlots, List<Integer> groupingSlotIndexes) {
        this.groupingSlots = ImmutableList.copyOf(
                Objects.requireNonNull(groupingSlots, "groupingSlots can not be null"));
        this.groupingSlotIndexes = ImmutableList.copyOf(
                Objects.requireNonNull(groupingSlotIndexes, "groupingSlotIndexes can not be null"));
    }

    /**
     * Compute result of 'grouping(column)' by a grouping set to long value for backend fill the result.
     *
     * Same example as class comment, there are 3 GroupingSetIdSlots:
     * 1. GroupingSetIdSlot(shouldBeErasedToNull=[false, false])   to bits: [0, 0] means [a=0, b=0]
     * 2. GroupingSetIdSlot(shouldBeErasedToNull=[true, false])    to bits: [1, 0] means [a=1, b=0]
     * 3. GroupingSetIdSlot(shouldBeErasedToNull=[true, true])     to bits: [1, 1] means [a=1, b=1]
     *
     * If we want to compute grouping_id(b, a), we will generate 3 rows bits:
     * 1. [0, 0] means [b=0, a=0]
     * 2. [0, 1] means [b=0, a=1]
     * 3. [1, 1] means [b=1, a=1]
     *
     * Then we combine the big endian bits to long: [0, 1, 3], one row mapping to one grouping set:
     *
     * grouping sets
     * (
     *      (a, b)       -- grouping(b, a) = 0
     *      (   b)       -- grouping(b, a) = 1
     *      (    )       -- grouping(b, a) = 3
     * )
     */
    @Override
    public long toLongValue(GroupingSetIdSlot groupingSetIdSlot) {
        List<Boolean> bits = groupingSlotIndexes.stream()
                .map(index -> groupingSetIdSlot.shouldBeErasedToNull(index))
                .collect(Collectors.toList());
        return BitUtils.bigEndianBitsToLong(bits);
    }

    @Override
    public String toString() {
        String slots = StringUtils.join(groupingSlots, ", ");
        String indexes = StringUtils.join(groupingSlotIndexes, ", ");
        return "GroupingIdFunctionSlot(groupingSlots=[" + slots + "], groupingSlotIndexes=[" + indexes + "])";
    }
}
