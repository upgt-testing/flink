/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.restarttest;

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.restarttest.state.AbstractStateCapture;
import org.restarttest.state.ClusterState;
import org.restarttest.state.DefaultClusterState;

import java.util.HashMap;

/**
 * State capture implementation for Flink MiniCluster.
 *
 * <p>No-op implementation - state capture is disabled.
 */
public class FlinkStateCapture extends AbstractStateCapture<MiniClusterWithClientResource> {

    @Override
    public ClusterState captureState(MiniClusterWithClientResource cluster) throws Exception {
        // No-op: return empty state
        return new DefaultClusterState(new HashMap<>());
    }

    @Override
    protected void verifyCustomInvariants(MiniClusterWithClientResource cluster,
                                         ClusterState before,
                                         ClusterState after) throws Exception {
        // No-op: skip custom invariant verification
    }
}
