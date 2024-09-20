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

package org.apache.doris.qe.runtime;

import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TReportExecStatusParams;

import java.util.concurrent.atomic.AtomicBoolean;

public class SingleFragmentPipelineTask extends LeafRuntimeTask {
    // immutable parameters
    private final Backend backend;
    private final int fragmentId;

    // mutate states
    private final AtomicBoolean done = new AtomicBoolean();

    public SingleFragmentPipelineTask(Backend backend, int fragmentId) {
        this.backend = backend;
        this.fragmentId = fragmentId;
    }

    // update profile.
    // return true if profile is updated. Otherwise, return false.
    // Has to use synchronized to ensure there are not concurrent update threads. Or the done
    // state maybe update wrong and will lose data. see https://github.com/apache/doris/pull/29802/files.
    public boolean processReportExecStatus(TReportExecStatusParams reportExecStatus) {
        // The fragment or instance is not finished, not need update
        if (!reportExecStatus.done) {
            return false;
        }
        return this.done.compareAndSet(false, true);
    }

    public boolean isDone() {
        return done.get();
    }

    public Backend getBackend() {
        return backend;
    }

    public int getFragmentId() {
        return fragmentId;
    }
}
