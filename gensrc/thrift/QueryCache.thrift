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

namespace cpp doris
namespace java org.apache.doris.thrift

struct TQueryCacheParam {
  1: required i32 node_id

  2: required binary digest

  // the query slots order can different to the query cache slots order,
  // so we should mapping cache slot id to current slot id
  3: required map<i32, i32> output_slot_mapping

  // mapping partition id to tablet id,
  // if the table is non-partition table, the partition id is -1
  4: required map<i64, set<i64>> partition_to_tablets

  // mapping partition id to filter range,
  // if the table is non-partition table, the partition id is -1.
  // BE will use <digest, tablet id, filter range> as the key to search query cache.
  // note that, BE not care what the filter range content is, just use as the part of the key.
  5: required map<i64, string> partition_filter_key

  6: optional bool force_refresh_query_cache

  7: optional i64 entry_max_bytes

  8: optional i64 entry_max_rows
}