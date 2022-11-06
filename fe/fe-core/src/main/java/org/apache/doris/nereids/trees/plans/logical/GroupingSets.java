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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Grouping;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/** GroupingSets */
public class GroupingSets {
    // output = groupByExpressions + aggregateFunctions.

    // Id and groupingSet are one-to-one correspondence.
    private List<GroupingSetIdSlot> ids;
    private List<Expression> groupByExpressions;
    private List<AggregateFunction> aggregateFunctions;

    public List<Integer> findIndexInGroupByExpressions(GroupingId groupingId) {
        Builder<Integer> indexesInGroupByExpressions = ImmutableList.builder();
        for (Expression theExprWantToGetId : groupingId.children()) {
            for (int i = 0; i < groupByExpressions.size(); i++) {
                if (groupByExpressions.get(i) == theExprWantToGetId) {
                    indexesInGroupByExpressions.add(i);
                }
            }
            throw new AnalysisException("Can not find the groupingId by the expression: " + theExprWantToGetId);
        }
        return indexesInGroupByExpressions.build();
    }

    public int findIndexInGroupByExpressions(Grouping grouping) {
        Expression theExprWantToKnowWhetherIsEraseToNull = grouping.child();
        for (int i = 0; i < groupByExpressions.size(); i++) {
            if (groupByExpressions.get(i).equals(theExprWantToKnowWhetherIsEraseToNull)) {
                return i;
            }
        }
        throw new AnalysisException("Can not find the groupingId by the expression: "
                + theExprWantToKnowWhetherIsEraseToNull);
    }
}
