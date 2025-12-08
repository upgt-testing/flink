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
import org.restarttest.health.HealthCheckResult;
import org.restarttest.state.ClusterState;

import static org.junit.Assert.*;

/**
 * Integration tests for the Flink Restart Testing Framework adapter.
 *
 * <p>These tests verify basic adapter functionality including:
 * <ul>
 *     <li>Adapter discovery and registration</li>
 *     <li>Node counting</li>
 *     <li>Health checks</li>
 *     <li>State capture</li>
 * </ul>
 */
public class FlinkAdapterIntegrationTest {

    private MiniClusterWithClientResource miniClusterResource;
    private FlinkClusterAdapter adapter;

    @Before
    public void setUp() throws Exception {
        miniClusterResource = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(2)
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
    public void testAdapterDiscovery() {
        // Verify adapter is properly configured
        assertNotNull("Adapter should not be null", adapter);
        assertEquals("Adapter should target MiniClusterWithClientResource",
                    MiniClusterWithClientResource.class,
                    adapter.getClusterType());
    }

    @Test
    public void testGetNodeCount() throws Exception {
        // Test TaskManager count
        int tmCount = adapter.getNodeCount(miniClusterResource, "taskmanager");
        assertEquals("Should have 2 TaskManagers", 2, tmCount);

        // Test with alternative role name "worker"
        int workerCount = adapter.getNodeCount(miniClusterResource, "worker");
        assertEquals("Worker should map to TaskManager", 2, workerCount);

        // Test JobManager count
        int jmCount = adapter.getNodeCount(miniClusterResource, "jobmanager");
        assertEquals("Should have 1 JobManager", 1, jmCount);

        // Test with alternative role name "master"
        int masterCount = adapter.getNodeCount(miniClusterResource, "master");
        assertEquals("Master should map to JobManager", 1, masterCount);

        // Test "all" count
        int allCount = adapter.getNodeCount(miniClusterResource, "all");
        assertEquals("All should count TMs + JM", 3, allCount);
    }

    @Test
    public void testHealthCheck() throws Exception {
        HealthCheckResult result = adapter.getHealthCheck()
            .checkHealth(miniClusterResource);

        assertTrue("Health check should pass for running cluster", result.isPassed());
        assertNotNull("Should have taskmanagers metric",
                     result.getMetrics().get("resource-overview.taskmanagers"));
        assertEquals("Should report 2 taskmanagers",
                    2, result.getMetrics().get("resource-overview.taskmanagers"));
    }

    @Test
    public void testStateCapture() throws Exception {
        ClusterState state = adapter.getStateCapture()
            .captureState(miniClusterResource);

        assertNotNull("State should not be null", state);
        assertEquals("Should capture 2 TaskManagers",
                    2, state.getStateMap().get("taskmanager_count"));
        assertEquals("Should capture 4 slots (2 TMs * 2 slots each)",
                    4, state.getStateMap().get("registered_slots"));
        assertEquals("Running status should be true",
                    true, state.getStateMap().get("is_running"));
    }

    @Test
    public void testWaitActive() throws Exception {
        // Should complete without error for a running cluster
        adapter.waitActive(miniClusterResource);
        // If we get here, waitActive succeeded
        assertTrue("Cluster should be active", miniClusterResource.getMiniCluster().isRunning());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRoleName() throws Exception {
        // Should throw for unknown role
        adapter.getNodeCount(miniClusterResource, "invalid_role");
    }
}
