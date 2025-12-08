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

import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.restarttest.state.AbstractStateCapture;
import org.restarttest.state.ClusterState;
import org.restarttest.state.DefaultClusterState;
import org.restarttest.state.StateVerificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * State capture implementation for Flink MiniCluster.
 *
 * <p>Captures cluster topology state including:
 * <ul>
 *     <li>TaskManager count</li>
 *     <li>Registered slot count</li>
 *     <li>Free slot count</li>
 *     <li>Cluster running status</li>
 *     <li>Job counts (optional)</li>
 * </ul>
 */
public class FlinkStateCapture extends AbstractStateCapture<MiniClusterWithClientResource> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStateCapture.class);

    @Override
    public ClusterState captureState(MiniClusterWithClientResource cluster) throws Exception {
        LOG.info("Capturing Flink cluster state");

        Map<String, Object> state = new HashMap<>();

        MiniCluster miniCluster = cluster.getMiniCluster();

        // Capture running status
        boolean isRunning = miniCluster.isRunning();
        state.put("is_running", isRunning);

        if (isRunning) {
            // Capture resource overview
            ResourceOverview overview = miniCluster.getResourceOverview().get();

            state.put("taskmanager_count", overview.getNumberTaskManagers());
            state.put("registered_slots", overview.getNumberRegisteredSlots());
            state.put("free_slots", overview.getNumberFreeSlots());
            state.put("blocked_taskmanagers", overview.getNumberBlockedTaskManagers());
            state.put("blocked_free_slots", overview.getNumberBlockedFreeSlots());

            // Capture cluster overview (job info)
            try {
                ClusterOverview clusterOverview = miniCluster.requestClusterOverview().get();
                state.put("jobs_running", clusterOverview.getNumJobsRunningOrPending());
                state.put("jobs_finished", clusterOverview.getNumJobsFinished());
                state.put("jobs_cancelled", clusterOverview.getNumJobsCancelled());
                state.put("jobs_failed", clusterOverview.getNumJobsFailed());
            } catch (Exception e) {
                LOG.warn("Failed to capture cluster overview (job info): {}", e.getMessage());
                // Job info is optional, continue without it
            }

            LOG.info("Captured state: {} TMs, {} slots ({} free), running={}",
                    overview.getNumberTaskManagers(),
                    overview.getNumberRegisteredSlots(),
                    overview.getNumberFreeSlots(),
                    isRunning);
        } else {
            LOG.warn("Cluster is not running, captured minimal state");
        }

        return new DefaultClusterState(state);
    }

    @Override
    protected void verifyCustomInvariants(MiniClusterWithClientResource cluster,
                                         ClusterState before,
                                         ClusterState after) throws Exception {
        LOG.info("Verifying Flink cluster invariants");

        Map<String, Object> beforeMap = before.getStateMap();
        Map<String, Object> afterMap = after.getStateMap();

        // Invariant 1: Cluster must be running
        Boolean wasRunning = (Boolean) beforeMap.get("is_running");
        Boolean isRunning = (Boolean) afterMap.get("is_running");

        if (wasRunning != null && wasRunning && (isRunning == null || !isRunning)) {
            throw new StateVerificationException(
                "Cluster is not running after restart (was running before)");
        }

        // Invariant 2: TaskManager count preserved
        compareStateValue("taskmanager_count", before, after, false);

        // Invariant 3: Registered slots preserved
        compareStateValue("registered_slots", before, after, false);

        // Invariant 4: Free slots should be >= before (allow increase, jobs may have completed)
        // This uses allowIncrease=true
        compareStateValue("free_slots", before, after, true);

        // Informational: Log job count changes
        Integer beforeRunning = (Integer) beforeMap.get("jobs_running");
        Integer afterRunning = (Integer) afterMap.get("jobs_running");

        if (beforeRunning != null && afterRunning != null &&
            !beforeRunning.equals(afterRunning)) {
            LOG.info("Running job count changed: {} -> {} (informational only)",
                    beforeRunning, afterRunning);
        }

        LOG.info("Flink cluster invariants verified successfully");
    }
}
