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

package org.apache.doris.nereids.worker;

import java.util.List;

/** WorkerGroup */
public interface WorkerGroup {
    AllWorkers ALL_WORKERS = new AllWorkers();
    AllAliveWorkers ALL_ALIVE_WORKERS = new AllAliveWorkers();

    List<Worker> filterWorkers(WorkerPool workerPool);

    /** AllWorkers */
    class AllWorkers implements WorkerGroup {
        private AllWorkers() {}

        @Override
        public List<Worker> filterWorkers(WorkerPool workerPool) {
            return workerPool.getAvailableWorkers();
        }
    }

    /** AllAliveWorkers */
    class AllAliveWorkers implements WorkerGroup {
        private AllAliveWorkers() {}

        public List<Worker> filterWorkers(WorkerPool workerPool) {
            return workerPool.getAvailableWorkers();
        }
    }
}
