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

import org.apache.doris.common.Pair;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TTabletCommitInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class LoadContext {
    private final Map<String, String> loadCounters = Maps.newLinkedHashMap();
    private final List<String> deltaUrls = Lists.newCopyOnWriteArrayList();

    // in pipelinex, the commit info may be duplicate, so we remove the duplicate ones
    private final Map<Pair<Long, Long>, TTabletCommitInfo> commitInfoMap = Maps.newLinkedHashMap();

    public synchronized Map<String, String> getLoadCounters() {
        return ImmutableMap.copyOf(loadCounters);
    }

    public synchronized void updateLoadCounters(Map<String, String> newLoadCounters) {
        long numRowsNormal = 0L;
        String value = this.loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
        if (value != null) {
            numRowsNormal = Long.parseLong(value);
        }
        long numRowsAbnormal = 0L;
        value = this.loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
        if (value != null) {
            numRowsAbnormal = Long.parseLong(value);
        }
        long numRowsUnselected = 0L;
        value = this.loadCounters.get(LoadJob.UNSELECTED_ROWS);
        if (value != null) {
            numRowsUnselected = Long.parseLong(value);
        }

        // new load counters
        value = newLoadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
        if (value != null) {
            numRowsNormal += Long.parseLong(value);
        }
        value = newLoadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
        if (value != null) {
            numRowsAbnormal += Long.parseLong(value);
        }
        value = newLoadCounters.get(LoadJob.UNSELECTED_ROWS);
        if (value != null) {
            numRowsUnselected += Long.parseLong(value);
        }

        this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "" + numRowsNormal);
        this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "" + numRowsAbnormal);
        this.loadCounters.put(LoadJob.UNSELECTED_ROWS, "" + numRowsUnselected);
    }

    public List<String> getDeltaUrls() {
        return Utils.fastToImmutableList(deltaUrls);
    }

    public void updateDeltas(List<String> deltaUrls) {
        if (!deltaUrls.isEmpty()) {
            this.deltaUrls.addAll(deltaUrls);
        }
    }

    public synchronized void updateCommitInfos(List<TTabletCommitInfo> commitInfos) {
        // distinct commit info in the set
        for (TTabletCommitInfo commitInfo : commitInfos) {
            this.commitInfoMap.put(Pair.of(commitInfo.backendId, commitInfo.tabletId), commitInfo);
        }
    }

    public synchronized List<TTabletCommitInfo> getCommitInfos() {
        return Utils.fastToImmutableList(commitInfoMap.values());
    }
}
