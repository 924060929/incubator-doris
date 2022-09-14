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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.BitUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
 *
 * The function of toLongValues is to display the virtual value corresponding to virtualSlot.
 * For extra generated columns:
 *  Rule: The column corresponding to groupingIdList is a non-null column,
 *        and the value is 0, and the rest are null columns, and the value is 1.
 *
 * For groupingFunc columns:
 *  Rules:
 *      grouping: Only one parameter is allowed,
 *                when the column is an aggregation column, it is set to 1, otherwise it is 0.
 *      grouping_id: Multiple columns are allowed,
 *                   and the decimal result is returned based on the bitmap of the aggregated column.
 *
 * eg:
 * select k1, grouping(k1), grouping(k1, k2) from t1 group by grouping sets ((k1), (k1, k2), (k3) ());
 * originalGroupingExprs: [k1, k2, k3]
 * virtualSlotRefs: GROUPING_ID(), GROUPING_PREFIX_K1(k1), GROUPING_PREFIX_K1_K2(k1, k2)
 * bitSetAll: {0, 1, 2}
 * groupingIdList: [{0}, {0, 1}, {2}, {}]
 *
 * GROUPING_ID():
 * For {0}: k1 is set to 0, k2 and k3 are set to 1.
 * +----+----+----+
 * | k1 | k2 | k3 |
 * +----+----+----+
 * | 0  | 1  | 1  |
 * +----+----+----+
 * convert binary to decimal, the result is 3.
 *
 * And so on, The corresponding list of GROUPING_ID() is [3, 1, 6, 7]
 *
 * GROUPING_PREFIX_K1(k1):
 * For {0}:
 * +----+
 * | k1 |
 * +----+
 * | 0  |
 * +----+
 * And so on, The corresponding list GROUPING_PREFIX_K1(k1) is [0, 0, 1, 1]
 *
 * GROUPING_PREFIX_K1_K2(k1, k2):
 * For {0}:
 * +----+----+
 * | k1 | k2 |
 * +----+----+
 * | 0  | 1  |
 * +----+----+
 * convert binary to decimal, the result is 1.
 *
 * And so on, The corresponding list GROUPING_PREFIX_K1_K2(k1, k2) is [1, 0, 3, 3]
 */
public class GroupingSetIdSlot {
    // this field is only used to print the used grouping slots for explain. e.g. 'a', 'b'
    // in the example of class comment
    protected final Set<Expression> groupingSlots;

    // this field is only used for nereids planner, backend need use the literal which generate by toLongValue.
    protected final List<List<Boolean>> shouldBeErasedToNull;

    public GroupingSetIdSlot(Set<Expression> groupingSlots, List<List<Boolean>> shouldBeErasedToNull) {
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
    public List<Long> toLongValue() {
        return this.shouldBeErasedToNull.stream()
                .map(BitUtils::bigEndianBitsToLong)
                .collect(Collectors.toList());
    }

    public List<List<Boolean>> getShouldBeErasedToNull() {
        return shouldBeErasedToNull;
    }

    /**
     * Displays the columns of groupingSets in the form of a bitset.
     * Because the column of false is the actual column, the result shows the column corresponding to false.
     *
     * For additional virtual columns, it represents the column information of groupingSets.
     * eg: groupingSets: ((k1, k2), (k1, v1), (v2, k2))
     *     groupingSlots: [k1, k2, v1, v2]
     *     shouldBeErasedToNull:[[false, false, true, true], [false, true, false, true], [true, false, true, false]]
     *     output:[{0, 1}, {0, 2}, {3, 2}]
     */
    public List<BitSet> genBitSetsForNullSlot() {
        List<BitSet> groupingIdList = new ArrayList<>();
        for (List<Boolean> eachGroup : shouldBeErasedToNull) {
            BitSet bitSet = new BitSet();
            for (int i = 0; i < eachGroup.size(); ++i) {
                bitSet.set(i, !eachGroup.get(i));
            }
            if (!groupingIdList.contains(bitSet)) {
                groupingIdList.add(bitSet);
            }
        }
        return groupingIdList;
    }

    @Override
    public String toString() {
        String slots = StringUtils.join(this.groupingSlots, ", ");
        String shouldBeErasedToNull = StringUtils.join(this.shouldBeErasedToNull, ", ");
        return "GroupingSetIdSlot(groupingSlots=[" + slots + "], shouldBeErasedToNull=[" + shouldBeErasedToNull + "])";
    }
}
