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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPaths;
import org.apache.doris.thrift.TColumnNameAccessPath;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PruneNestedColumn extends TestWithFeService {
    @BeforeAll
    public void createTable() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("create table tbl(\n"
                + "  id int,\n"
                + "  s struct<\n"
                + "    city: string,\n"
                + "    data: array<map<\n"
                + "      int,\n"
                + "      struct<a: int, b: double>\n"
                + "    >>\n"
                + ">)\n"
                + "properties ('replication_num'='1')");

        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
    }

    @Test
    public void testProject() throws Exception {
        assertColumn("select 100 from tbl", null, null);
        assertColumn("select s from tbl", "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>", path());
        assertColumn("select struct_element(s, 'city') from tbl", "struct<city:text>", path("city"));
        assertColumn("select struct_element(s, 'data') from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data"));
        assertColumn("select struct_element(s, 'data')[1] from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data"));
        assertColumn("select map_keys(struct_element(s, 'data')[1]) from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data", "*", "KEYS"));
        assertColumn("select map_values(struct_element(s, 'data')[1]) from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data", "*", "VALUES"));
        assertColumn("select struct_element(map_values(struct_element(s, 'data')[1])[1], 'a') from tbl", "struct<data:array<map<int,struct<a:int>>>>", path("data", "*", "VALUES", "a"));
        assertColumn("select struct_element(s, 'data')[1][1] from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data"));
        assertColumn("select struct_element(struct_element(s, 'data')[1][1], 'a') from tbl", "struct<data:array<map<int,struct<a:int>>>>", path("data", "*", "*", "a"));
        assertColumn("select struct_element(struct_element(s, 'data')[1][1], 'b') from tbl", "struct<data:array<map<int,struct<b:double>>>>", path("data", "*", "*", "b"));
        assertColumn("select array_map(x -> x[2], struct_element(s, 'data')) from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data"));
        assertColumn("select array_map(x -> struct_element(x[2], 'b'), struct_element(s, 'data')) from tbl", "struct<data:array<map<int,struct<b:double>>>>", path("data", "*", "*", "b"));
    }

    private void assertColumn(String sql, String expectType, TColumnNameAccessPath expectAccessPath) throws Exception {
        List<SlotDescriptor> slotDescriptors = collectComplexSlots(sql);
        if (expectType == null) {
            Assertions.assertEquals(0, slotDescriptors.size());
            return;
        }

        Assertions.assertEquals(1, slotDescriptors.size());
        Assertions.assertEquals(expectType, slotDescriptors.get(0).getType().toString());
        TColumnAccessPaths accessPaths =
                new TColumnAccessPaths(TAccessPathType.NAME).setNameAccessPaths(ImmutableList.of(expectAccessPath));
        Assertions.assertEquals(accessPaths, slotDescriptors.get(0).getAccessPaths());
        System.out.println("ok");
    }

    private List<SlotDescriptor> collectComplexSlots(String sql) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) getSqlStmtExecutor(sql).planner();
        List<SlotDescriptor> complexSlots = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            List<OlapScanNode> olapScanNodes = fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance);
            for (OlapScanNode olapScanNode : olapScanNodes) {
                List<SlotDescriptor> slots = olapScanNode.getTupleDesc().getSlots();
                for (SlotDescriptor slot : slots) {
                    Type type = slot.getType();
                    if (type.isComplexType() || type.isVariantType()) {
                        complexSlots.add(slot);
                    }
                }
            }
        }
        return complexSlots;
    }

    private TColumnNameAccessPath path(String... path) {
        return path(false, path);
    }

    private TColumnNameAccessPath filterPath(String... path) {
        return path(true, path);
    }

    private TColumnNameAccessPath path(boolean predicate, String... path) {
        return new TColumnNameAccessPath(ImmutableList.copyOf(path), predicate);
    }
}
