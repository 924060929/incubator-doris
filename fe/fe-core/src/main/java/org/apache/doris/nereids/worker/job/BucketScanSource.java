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

import org.apache.doris.planner.ScanNode;

import java.util.Map;
import java.util.Map.Entry;

/** BucketScanSource */
public class BucketScanSource extends ScanSource {
    // for example:
    //   1. bucket 0 use OlapScanNode(tableName=`tbl`) to scan with tablet: [tablet 10001, tablet 10003]
    //   2. bucket 1 use OlapScanNode(tableName=`tbl`) to scan with tablet: [tablet 10002, tablet 10004]
    public final Map<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToTablets;

    public BucketScanSource(Map<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToTablets) {
        this.bucketIndexToScanNodeToTablets = bucketIndexToScanNodeToTablets;
    }

    /** toString */
    public void toString(StringBuilder str, String prefix) {
        int i = 0;
        String nextIndent = prefix + "  ";
        str.append("[\n");
        for (Entry<Integer, Map<ScanNode, ScanRanges>> entry : bucketIndexToScanNodeToTablets.entrySet()) {
            Integer bucketId = entry.getKey();
            Map<ScanNode, ScanRanges> scanNodeToScanRanges = entry.getValue();
            str.append(prefix).append("  bucket ").append(bucketId).append(": ");
            DefaultScanSource.toString(scanNodeToScanRanges, str, nextIndent);
            if (++i < bucketIndexToScanNodeToTablets.size()) {
                str.append(",\n");
            }
        }
        str.append("\n").append(prefix).append("]");
    }
}
