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

import org.apache.doris.clone.TabletScheduler.Slot;
import org.apache.doris.nereids.util.BitUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * GroupingSetIdSlot is used to compute which group column should be erased to null,
 * and as the computation source of grouping() / grouping_id() function,
 * which grouping function denote by GroupingFunctionSlot and grouping_id function denote by GroupingIdFunctionSlot.
 *
 * for example: this grouping sets will create 3 group sets
 * <pre>
 * select b, a
 * from tbl
 * group by
 * grouping sets
 * (
 *      (a, b)              -- GroupingSetSlot(shouldBeErasedToNull=[false, false])
 *      (   b)              -- GroupingSetSlot(shouldBeErasedToNull=[true, false])
 *      (    )              -- GroupingSetSlot(shouldBeErasedToNull=[true, true])
 * )
 * </pre>
 */
public class GroupingSetIdSlot {
    // this field is only used to print the used grouping slots for explain. e.g. 'a', 'b'
    // in the example of class comment
    private final Set<Slot> groupingSlots;

    // this field is only used for nereids planner, backend need use the literal  which generate by toLongValue.
    private final List<Boolean> shouldBeErasedToNull;

    public GroupingSetIdSlot(Set<Slot> groupingSlots, List<Boolean> shouldBeErasedToNull) {
        this.groupingSlots = ImmutableSet.copyOf(
                Objects.requireNonNull(groupingSlots, "groupingSlots can not be null"));
        this.shouldBeErasedToNull = ImmutableList.copyOf(
                Objects.requireNonNull(shouldBeErasedToNull, "shouldBeErasedToNull can not be null"));
    }

    /**
     * convert shouldBeErasedToNull to bits, combine the bits to long,
     * backend will set the column to null if the bit is 1.
     *
     * The compute method, e.g.
     * shouldBeErasedToNull = [false, true, true, true] means [0, 1, 1, 1],
     * we combine the bits of big endian to long value 7.
     *
     * The example in class comment:
     * grouping sets
     * (
     *      (a, b)       -- [0, 0], to long value is 0
     *      (   b)       -- [1, 0], to long value is 2
     *      (    )       -- [1, 1], to long value is 3
     * )
     */
    public long toLongValue() {
        return BitUtils.bigEndianBitsToLong(shouldBeErasedToNull);
    }

    public boolean shouldBeErasedToNull(int index) {
        return shouldBeErasedToNull.get(index);
    }

    @Override
    public String toString() {
        String slots = StringUtils.join(groupingSlots, ", ");
        String shouldBeErasedToNull = StringUtils.join(this.shouldBeErasedToNull, ", ");
        return "GroupingSetIdSlot(groupingSlots=[" + slots + "], shouldBeErasedToNull=[" + shouldBeErasedToNull + "])";
    }
}
