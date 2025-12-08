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

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.restarttest.core.ClusterAdapter;
import org.restarttest.core.RestartMode;
import org.restarttest.health.CompositeHealthCheck;
import org.restarttest.health.HealthCheck;
import org.restarttest.state.StateCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Restart Testing Framework adapter for Apache Flink MiniClusterWithClientResource.
 *
 * <p>This adapter enables systematic restart testing of Flink clusters with support for:
 * <ul>
 *     <li>Individual TaskManager restart</li>
 *     <li>JobManager component restart (Dispatcher/ResourceManager via HA leadership control)</li>
 *     <li>All nodes restart</li>
 * </ul>
 *
 * <p><b>Limitations:</b>
 * <ul>
 *     <li>Full cluster restart is NOT supported for MiniClusterWithClientResource due to lifecycle constraints</li>
 *     <li>JobManager restart requires cluster configured with .withHaLeadershipControl()</li>
 *     <li>TaskManager indices are not reused after termination</li>
 *     <li>Jobs may fail during restart (recovery depends on Flink's configured restart strategy)</li>
 * </ul>
 */
public class FlinkClusterAdapter implements ClusterAdapter<MiniClusterWithClientResource> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkClusterAdapter.class);

    private final FlinkStateCapture stateCapture;
    private final CompositeHealthCheck<MiniClusterWithClientResource> healthCheck;

    public FlinkClusterAdapter() {
        this.stateCapture = new FlinkStateCapture();
        this.healthCheck = new CompositeHealthCheck<>("flink-health");

        // Add default health checks
        this.healthCheck.addCheck(new ClusterRunningCheck());
        this.healthCheck.addCheck(new ResourceOverviewCheck());
    }

    @Override
    public Class<MiniClusterWithClientResource> getClusterType() {
        return MiniClusterWithClientResource.class;
    }

    @Override
    public StateCapture<MiniClusterWithClientResource> getStateCapture() {
        return stateCapture;
    }

    @Override
    public HealthCheck<MiniClusterWithClientResource> getHealthCheck() {
        return healthCheck;
    }

    @Override
    public void restartNode(MiniClusterWithClientResource cluster, String nodeRole,
                           int nodeIndex, RestartMode mode) throws Exception {
        String normalizedRole = normalizeRole(nodeRole);

        LOG.info("Restarting {} node at index {} with mode {}", normalizedRole, nodeIndex, mode);

        if ("taskmanager".equals(normalizedRole)) {
            restartTaskManager(cluster, nodeIndex, mode);
        } else if ("jobmanager".equals(normalizedRole)) {
            restartJobManagerComponent(cluster, nodeIndex, mode);
        } else if ("cluster".equals(normalizedRole)) {
            restartAllComponents(cluster, mode);
        } else {
            throw new IllegalArgumentException(
                "Unknown node role: " + nodeRole +
                ". Supported: taskmanager, jobmanager, master, worker, all");
        }

        LOG.info("Successfully restarted {} node", normalizedRole);
    }

    @Override
    public void restartAllNodes(MiniClusterWithClientResource cluster, String nodeRole,
                               RestartMode mode) throws Exception {
        String normalizedRole = normalizeRole(nodeRole);

        LOG.info("Restarting all {} nodes with mode {}", normalizedRole, mode);

        if ("taskmanager".equals(normalizedRole)) {
            int tmCount = getNodeCount(cluster, "taskmanager");
            for (int i = 0; i < tmCount; i++) {
                restartTaskManager(cluster, i, mode);
            }
        } else if ("jobmanager".equals(normalizedRole)) {
            restartJobManagerComponent(cluster, 0, mode);
        } else if ("cluster".equals(normalizedRole) || "all".equals(normalizedRole)) {
            restartAllComponents(cluster, mode);
        } else {
            throw new IllegalArgumentException("Unknown node role: " + nodeRole);
        }

        LOG.info("Successfully restarted all {} nodes", normalizedRole);
    }

    @Override
    public void waitActive(MiniClusterWithClientResource cluster) throws Exception {
        LOG.info("Waiting for Flink cluster to become active");

        MiniCluster miniCluster = cluster.getMiniCluster();

        // Check 1: MiniCluster is running
        if (!miniCluster.isRunning()) {
            throw new IllegalStateException("MiniCluster is not running");
        }

        // Check 2: Wait for TaskManagers to register
        // Get expected count from current ResourceOverview
        final ResourceOverview initialOverview = miniCluster.getResourceOverview().get();
        final int initialTMs = initialOverview.getNumberTaskManagers();
        final int initialSlots = initialOverview.getNumberRegisteredSlots();

        // If starting from 0, wait for at least 1 TaskManager to appear
        if (initialTMs == 0) {
            LOG.debug("Cluster has 0 TaskManagers, waiting for TaskManagers to register");
            org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(
                () -> {
                    ResourceOverview overview = miniCluster.getResourceOverview().get();
                    return overview.getNumberTaskManagers() > 0;
                },
                100L, // retry interval: 100ms
                300    // retry attempts: 300 * 100ms = 30 seconds
            );

            // After TMs have registered, wait for all slots to be available
            ResourceOverview updatedOverview = miniCluster.getResourceOverview().get();
            final int expectedSlots = updatedOverview.getNumberRegisteredSlots();

            if (expectedSlots > 0) {
                org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(
                    () -> {
                        ResourceOverview overview = miniCluster.getResourceOverview().get();
                        return overview.getNumberRegisteredSlots() >= expectedSlots;
                    },
                    100L,
                    300
                );
            }

            LOG.info("Flink cluster is active: {} TMs, {} slots",
                     updatedOverview.getNumberTaskManagers(), expectedSlots);
        } else {
            // Wait for expected count to be reached (restart scenario)
            org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(
                () -> {
                    ResourceOverview overview = miniCluster.getResourceOverview().get();
                    return overview.getNumberTaskManagers() >= initialTMs;
                },
                100L,
                300
            );

            // Check 3: Wait for slots to be available
            if (initialSlots > 0) {
                org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(
                    () -> {
                        ResourceOverview overview = miniCluster.getResourceOverview().get();
                        return overview.getNumberRegisteredSlots() >= initialSlots;
                    },
                    100L,
                    300
                );
            }

            LOG.info("Flink cluster is active: {} TMs, {} slots", initialTMs, initialSlots);
        }
    }

    @Override
    public int getNodeCount(MiniClusterWithClientResource cluster, String nodeRole) throws Exception {
        String normalizedRole = normalizeRole(nodeRole);

        if ("taskmanager".equals(normalizedRole)) {
            ResourceOverview overview = cluster.getMiniCluster()
                .getResourceOverview().get();
            return overview.getNumberTaskManagers();
        } else if ("jobmanager".equals(normalizedRole)) {
            return 1; // Single JM in MiniCluster
        } else if ("cluster".equals(normalizedRole) || "all".equals(normalizedRole)) {
            ResourceOverview overview = cluster.getMiniCluster()
                .getResourceOverview().get();
            return overview.getNumberTaskManagers() + 1; // TMs + 1 JM
        } else {
            throw new IllegalArgumentException("Unknown node role: " + nodeRole);
        }
    }

    /**
     * Normalize generic role names to Flink-specific names.
     *
     * @param role The role name (master, worker, jobmanager, taskmanager, etc.)
     * @return Normalized role name
     */
    private String normalizeRole(String role) {
        String lower = role.toLowerCase();
        if ("master".equals(lower)) {
            return "jobmanager";
        } else if ("worker".equals(lower)) {
            return "taskmanager";
        }
        return lower;
    }

    /**
     * Restart a single TaskManager.
     *
     * <p><b>Important:</b> After terminateTaskManager(i), that index is NOT reused.
     * The new TaskManager gets the next available index. This method verifies restart
     * by checking the total TaskManager count, not the specific index.
     *
     * @param cluster The cluster resource
     * @param index The TaskManager index
     * @param mode The restart mode
     * @throws Exception if restart fails
     */
    private void restartTaskManager(MiniClusterWithClientResource cluster,
                                    int index, RestartMode mode) throws Exception {
        LOG.info("Restarting TaskManager {} with mode {}", index, mode);

        MiniCluster miniCluster = cluster.getMiniCluster();

        // Capture state before restart
        ResourceOverview beforeRestart = miniCluster.getResourceOverview().get();
        int expectedTMCount = beforeRestart.getNumberTaskManagers();
        int expectedSlotCount = beforeRestart.getNumberRegisteredSlots();

        // Phase 1: Terminate TaskManager
        switch (mode) {
            case GRACEFUL:
                // Normal termination
                miniCluster.terminateTaskManager(index).get();
                break;

            case CRASH:
                // Immediate termination (same as GRACEFUL for TaskManager)
                miniCluster.terminateTaskManager(index).get();
                break;

            case DELAYED_CRASH:
                // Terminate, wait, then restart
                miniCluster.terminateTaskManager(index).get();
                Thread.sleep(500); // Allow partial state propagation
                break;

            default:
                throw new IllegalArgumentException("Unknown restart mode: " + mode);
        }

        LOG.debug("TaskManager {} terminated", index);

        // Phase 2: Start new TaskManager
        miniCluster.startTaskManager();

        LOG.debug("New TaskManager started");

        // Phase 3: Wait for new TaskManager to register
        final int finalExpectedTMCount = expectedTMCount;
        final int finalExpectedSlotCount = expectedSlotCount;

        org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(
            () -> {
                ResourceOverview overview = miniCluster.getResourceOverview().get();
                boolean tmsRegistered = overview.getNumberTaskManagers() >= finalExpectedTMCount;
                boolean slotsRegistered = overview.getNumberRegisteredSlots() >= finalExpectedSlotCount;
                return tmsRegistered && slotsRegistered;
            },
            100L, // retry interval: 100ms
            300   // retry attempts: 300 * 100ms = 30 seconds
        );

        LOG.info("TaskManager restart completed successfully. Cluster now has {} TMs with {} slots",
                finalExpectedTMCount, finalExpectedSlotCount);
    }

    /**
     * Restart JobManager components (Dispatcher and ResourceManager) via HA leadership control.
     *
     * <p><b>Requires:</b> Cluster configured with .withHaLeadershipControl()
     *
     * <p>This method simulates JobManager component failure/recovery by revoking and
     * re-granting leadership to both the Dispatcher and ResourceManager components.
     * This does not restart the actual process, but triggers the same recovery logic
     * that would occur if the JobManager process failed and restarted.
     *
     * @param cluster The cluster resource
     * @param componentIndex The component index (unused, always 0 for single JM)
     * @param mode The restart mode
     * @throws Exception if restart fails
     */
    private void restartJobManagerComponent(MiniClusterWithClientResource cluster,
                                           int componentIndex, RestartMode mode) throws Exception {
        LOG.info("Restarting JobManager component with mode {}", mode);

        MiniCluster miniCluster = cluster.getMiniCluster();
        Optional<HaLeadershipControl> haControl = miniCluster.getHaLeadershipControl();

        if (!haControl.isPresent()) {
            throw new IllegalStateException(
                "JobManager restart requires HA leadership control. " +
                "Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()");
        }

        HaLeadershipControl control = haControl.get();

        // Phase 1: Revoke leadership (simulates component failure)
        LOG.debug("Revoking Dispatcher and ResourceManager leadership");
        control.revokeDispatcherLeadership().get();
        control.revokeResourceManagerLeadership().get();

        // Phase 2: For DELAYED_CRASH, wait before granting leadership back
        switch (mode) {
            case GRACEFUL:
                // No delay
                break;

            case CRASH:
                // No delay (immediate recovery)
                break;

            case DELAYED_CRASH:
                // Wait to simulate partial state propagation
                LOG.debug("Waiting 500ms before granting leadership back (DELAYED_CRASH mode)");
                Thread.sleep(500);
                break;

            default:
                throw new IllegalArgumentException("Unknown restart mode: " + mode);
        }

        // Phase 3: Grant leadership back (simulates component recovery)
        LOG.debug("Granting Dispatcher and ResourceManager leadership back");
        control.grantDispatcherLeadership().get();
        control.grantResourceManagerLeadership().get();

        // Phase 4: Wait for components to become active
        waitActive(cluster);

        LOG.info("JobManager components restarted successfully");
    }

    /**
     * Restart all cluster components sequentially.
     *
     * <p>This is a workaround for full cluster restart, which is not supported
     * for MiniClusterWithClientResource due to lifecycle constraints.
     *
     * @param cluster The cluster resource
     * @param mode The restart mode
     * @throws Exception if restart fails
     */
    private void restartAllComponents(MiniClusterWithClientResource cluster,
                                     RestartMode mode) throws Exception {
        LOG.info("Restarting all cluster components (workaround for full cluster restart)");

        // Step 1: Restart JobManager components
        restartJobManagerComponent(cluster, 0, mode);

        // Step 2: Restart all TaskManagers
        int tmCount = getNodeCount(cluster, "taskmanager");
        for (int i = 0; i < tmCount; i++) {
            restartTaskManager(cluster, i, mode);
        }

        // Step 3: Wait for full cluster active
        waitActive(cluster);

        LOG.info("All cluster components restarted successfully");
    }
}
