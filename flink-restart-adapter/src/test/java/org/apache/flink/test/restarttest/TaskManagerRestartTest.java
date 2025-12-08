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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restarttest.core.RestartMode;
import org.restarttest.state.ClusterState;

import static org.junit.Assert.*;

/**
 * Tests for TaskManager restart functionality.
 */
public class TaskManagerRestartTest {

    private MiniClusterWithClientResource miniClusterResource;
    private FlinkClusterAdapter adapter;

    @Before
    public void setUp() throws Exception {
        miniClusterResource = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(3)
                .setNumberSlotsPerTaskManager(2)
                .build());
        miniClusterResource.before();

        adapter = new FlinkClusterAdapter();

        // Wait for cluster to be fully ready with TaskManagers registered
        adapter.waitActive(miniClusterResource);
    }

    @After
    public void tearDown() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    @Test
    public void testSingleTaskManagerGracefulRestart() throws Exception {
        // Capture state before restart
        ClusterState beforeState = adapter.getStateCapture()
            .captureState(miniClusterResource);

        // Restart TaskManager at index 0 with graceful mode
        adapter.restartNode(miniClusterResource, "taskmanager", 0, RestartMode.GRACEFUL);

        // Verify cluster still has 3 TMs
        int tmCount = adapter.getNodeCount(miniClusterResource, "taskmanager");
        assertEquals("Should still have 3 TaskManagers after restart", 3, tmCount);

        // Capture state after restart
        ClusterState afterState = adapter.getStateCapture()
            .captureState(miniClusterResource);

        // Verify state is consistent
        adapter.getStateCapture().verifyState(miniClusterResource, beforeState, afterState);
    }

    @Test
    public void testTaskManagerCrashRestart() throws Exception {
        adapter.restartNode(miniClusterResource, "taskmanager", 1, RestartMode.CRASH);

        // Verify cluster recovered
        adapter.waitActive(miniClusterResource);

        int tmCount = adapter.getNodeCount(miniClusterResource, "taskmanager");
        assertEquals("Should have 3 TaskManagers after crash restart", 3, tmCount);
    }

    @Test
    public void testTaskManagerDelayedCrashRestart() throws Exception {
        adapter.restartNode(miniClusterResource, "taskmanager", 2, RestartMode.DELAYED_CRASH);

        // Verify state preserved
        ClusterState state = adapter.getStateCapture()
            .captureState(miniClusterResource);
        assertEquals("Should have 3 TaskManagers", 3, state.getStateMap().get("taskmanager_count"));
        assertEquals("Should have 6 slots", 6, state.getStateMap().get("registered_slots"));
    }

    @Test
    public void testAllTaskManagersRestart() throws Exception {
        // Restart all TaskManagers
        adapter.restartAllNodes(miniClusterResource, "taskmanager", RestartMode.GRACEFUL);

        // Verify all TMs restarted successfully
        int tmCount = adapter.getNodeCount(miniClusterResource, "taskmanager");
        assertEquals("Should still have 3 TaskManagers after restarting all", 3, tmCount);

        // Verify cluster is healthy
        assertTrue("Cluster should be healthy",
                  adapter.getHealthCheck().checkHealth(miniClusterResource).isPassed());
    }

    @Test
    public void testAlternativeRoleName() throws Exception {
        // Test that "worker" role name works
        adapter.restartNode(miniClusterResource, "worker", 0, RestartMode.GRACEFUL);

        int workerCount = adapter.getNodeCount(miniClusterResource, "worker");
        assertEquals("Should still have 3 workers", 3, workerCount);
    }
}
