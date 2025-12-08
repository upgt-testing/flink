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
 * Tests for JobManager component restart functionality.
 */
public class JobManagerRestartTest {

    private MiniClusterWithClientResource miniClusterResource;
    private FlinkClusterAdapter adapter;

    @Before
    public void setUp() throws Exception {
        miniClusterResource = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(2)
                .setNumberSlotsPerTaskManager(2)
                .withHaLeadershipControl()  // CRITICAL: Enable HA control
                .build());
        miniClusterResource.before();

        adapter = new FlinkClusterAdapter();
    }

    @After
    public void tearDown() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    @Test
    public void testJobManagerComponentGracefulRestart() throws Exception {
        // Verify HA control is available
        assertTrue("HA leadership control should be present",
                  miniClusterResource.getMiniCluster()
                      .getHaLeadershipControl().isPresent());

        // Restart JobManager components
        adapter.restartNode(miniClusterResource, "jobmanager", 0, RestartMode.GRACEFUL);

        // Verify cluster still active
        adapter.waitActive(miniClusterResource);
        assertTrue("Cluster should be active",
                  miniClusterResource.getMiniCluster().isRunning());
    }

    @Test
    public void testJobManagerCrashRestart() throws Exception {
        adapter.restartNode(miniClusterResource, "jobmanager", 0, RestartMode.CRASH);

        // Verify recovery
        ClusterState state = adapter.getStateCapture()
            .captureState(miniClusterResource);
        assertEquals("Should have 2 TaskManagers", 2, state.getStateMap().get("taskmanager_count"));
        assertTrue("Cluster should be running", (Boolean) state.getStateMap().get("is_running"));
    }

    @Test
    public void testJobManagerDelayedCrashRestart() throws Exception {
        adapter.restartNode(miniClusterResource, "jobmanager", 0, RestartMode.DELAYED_CRASH);

        // Verify cluster recovered
        assertTrue("Health check should pass",
                  adapter.getHealthCheck().checkHealth(miniClusterResource).isPassed());
    }

    @Test
    public void testAlternativeRoleName() throws Exception {
        // Test that "master" role name works
        adapter.restartNode(miniClusterResource, "master", 0, RestartMode.GRACEFUL);

        adapter.waitActive(miniClusterResource);
        assertTrue("Cluster should be active", miniClusterResource.getMiniCluster().isRunning());
    }

    @Test(expected = IllegalStateException.class)
    public void testJobManagerRestartWithoutHaControl() throws Exception {
        // Create cluster WITHOUT HA control
        MiniClusterWithClientResource clusterNoHa =
            new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(1)
                    // NO .withHaLeadershipControl()
                    .build());

        try {
            clusterNoHa.before();

            // Should throw IllegalStateException
            adapter.restartNode(clusterNoHa, "jobmanager", 0, RestartMode.GRACEFUL);

            fail("Should have thrown IllegalStateException");
        } finally {
            clusterNoHa.after();
        }
    }
}
