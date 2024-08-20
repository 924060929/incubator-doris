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

package org.apache.doris.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TRuntimeFilterMode;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QueryCacheNormalizerTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);

        // Create database `db1`.
        createDatabase("db1");

        useDatabase("db1");

        // Create tables.
        String nonPart = "create table db1.non_part("
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(k1, k2, k3)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String part1 = "create table db1.part1("
                + "  dt date,\n"
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(dt, k1, k2, k3)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "  PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "  PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String part2 = "create table db1.part2("
                + "  dt date,\n"
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(dt, k1, k2, k3)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "  PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "  PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String multiLeveParts = "create table db1.multi_level_parts("
                + "  k1 int,\n"
                + "  dt date,\n"
                + "  hour int,\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "PARTITION BY RANGE(dt, hour)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01', '0'), ('2024-03-01', '1')),\n"
                + "  PARTITION p202404 VALUES [('2024-03-01', '1'), ('2024-03-01', '2')),\n"
                + "  PARTITION p202405 VALUES [('2024-03-01', '2'), ('2024-03-01', '3'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        createTables(nonPart, part1, part2, multiLeveParts);

        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    public void testNormalize() throws Exception {
        String digest1 = getDigest("select k1 as k, sum(v1) as v from db1.non_part group by 1");
        String digest2 = getDigest("select sum(v1) as v1, k1 as k1 from db1.non_part group by 2");
        Assertions.assertEquals(digest1, digest2);

        String digest3 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 between 1 and 10 group by 1");
        Assertions.assertNotEquals(digest1, digest3);

        String digest4 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 >= 1 and v1 <= 10 group by 1");
        Assertions.assertEquals(digest3, digest4);

        String digest5 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 >= 1 and v1 < 11 group by 1");
        Assertions.assertNotEquals(digest3, digest5);
    }

    @Test
    public void testProjectOnOlapScan() throws Exception {
        String digest1 = getDigest("select k1 + 1, k2, sum(v1), sum(v2) as v from db1.non_part group by 1, 2");
        String digest2 = getDigest("select sum(v2), k2, sum(v1), k1 + 1 from db1.non_part group by 2, 4");
        Assertions.assertEquals(digest1, digest2);

        String digest3 = getDigest("select k1 + 1, k2, sum(v1 + 1), sum(v2) as v from db1.non_part group by 1, 2");
        Assertions.assertNotEquals(digest1, digest3);
    }

    @Test
    public void testProjectOnAggregate() throws Exception {
        connectContext.getSessionVariable()
                .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT");
        try {
            String digest1 = getDigest(
                    "select k1 + 1, k2 + 2, sum(v1) + 3, sum(v2) + 4 as v from db1.non_part group by k1, k2"
            );
            String digest2 = getDigest(
                    "select sum(v2) + 4, k2 + 2, sum(v1) + 3, k1 + 1 as v from db1.non_part group by k2, k1"
            );
            Assertions.assertEquals(digest1, digest2);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    @Test
    public void testPartitionTable() throws Throwable {
        TQueryCacheParam queryCacheParam1 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 group by 1");
        TQueryCacheParam queryCacheParam2 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2025-01-01' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam2.digest);
        Assertions.assertEquals(queryCacheParam1.partition_filter_key, queryCacheParam2.partition_filter_key);
        Assertions.assertEquals(queryCacheParam1.partition_to_tablets, queryCacheParam2.partition_to_tablets);

        TQueryCacheParam queryCacheParam3 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2024-05-20' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam3.digest);
        Assertions.assertNotEquals(queryCacheParam1.partition_filter_key, queryCacheParam3.partition_filter_key);
        Assertions.assertEquals(queryCacheParam1.partition_to_tablets, queryCacheParam3.partition_to_tablets);

        TQueryCacheParam queryCacheParam4 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2024-04-20' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam4.digest);
        Assertions.assertNotEquals(queryCacheParam1.partition_filter_key, queryCacheParam4.partition_filter_key);
        Assertions.assertNotEquals(queryCacheParam1.partition_to_tablets, queryCacheParam4.partition_to_tablets);
    }

    @Test
    public void testMultiLevelPartitionTable() throws Throwable {
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select k1, sum(v1) as v from db1.multi_level_parts group by 1");
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    @Test
    public void testHaving() throws Throwable {
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select k1, sum(v1) as v from db1.part1 group by 1 having v > 10");
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    @Test
    public void testRuntimeFilter() throws Throwable {
        connectContext.getSessionVariable().setRuntimeFilterMode(TRuntimeFilterMode.GLOBAL.toString());
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.IN_OR_BLOOM.getValue());
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select * from (select k1, count(*) from db1.part1 where k1 < 15 group by k1)a\n"
                        + "join (select k1, count(*) from db1.part1 where k1 < 10 group by k1)b\n"
                        + "on a.k1 = b.k1");

        // only non target side can use query cache
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    private String getDigest(String sql) throws Exception {
        return Hex.encodeHexString(getQueryCacheParam(sql).digest);
    }

    private TQueryCacheParam getQueryCacheParam(String sql) throws Exception {
        List<TQueryCacheParam> queryCaches = normalize(sql);
        Assertions.assertEquals(1, queryCaches.size());
        return queryCaches.get(0);
    }

    private List<TQueryCacheParam> normalize(String sql) throws Exception {
        Planner planner = getSqlStmtExecutor(sql).planner();
        DescriptorTable descTable = planner.getDescTable();
        List<PlanFragment> fragments = planner.getFragments();
        List<TQueryCacheParam> queryCacheParams = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
            Optional<TQueryCacheParam> queryCacheParam = normalizer.normalize(connectContext);
            if (queryCacheParam.isPresent()) {
                queryCacheParams.add(queryCacheParam.get());
            }
        }
        return queryCacheParams;
    }

    private List<TNormalizedPlanNode> normalizePlans(String sql) throws Exception {
        Planner planner = getSqlStmtExecutor(sql).planner();
        DescriptorTable descTable = planner.getDescTable();
        List<PlanFragment> fragments = planner.getFragments();
        List<TNormalizedPlanNode> normalizedPlans = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
            normalizedPlans.addAll(normalizer.normalizePlans(connectContext));
        }
        return normalizedPlans;
    }
}
