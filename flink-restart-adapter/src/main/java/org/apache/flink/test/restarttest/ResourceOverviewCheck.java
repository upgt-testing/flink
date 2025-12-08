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

import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.restarttest.health.HealthCheck;
import org.restarttest.health.HealthCheckResult;

/**
 * Health check to verify Flink cluster resource availability.
 */
public class ResourceOverviewCheck implements HealthCheck<MiniClusterWithClientResource> {

    @Override
    public HealthCheckResult checkHealth(MiniClusterWithClientResource cluster) throws Exception {
        HealthCheckResult result = new HealthCheckResult(true, getName());

        ResourceOverview overview = cluster.getMiniCluster()
            .getResourceOverview().get();

        // Collect all metrics
        result.addMetric("taskmanagers", overview.getNumberTaskManagers());
        result.addMetric("registered_slots", overview.getNumberRegisteredSlots());
        result.addMetric("free_slots", overview.getNumberFreeSlots());
        result.addMetric("blocked_taskmanagers", overview.getNumberBlockedTaskManagers());
        result.addMetric("blocked_free_slots", overview.getNumberBlockedFreeSlots());

        // Basic sanity checks
        if (overview.getNumberTaskManagers() == 0) {
            result.addFailure("No TaskManagers registered");
        }

        if (overview.getNumberRegisteredSlots() == 0) {
            result.addFailure("No slots registered");
        }

        return result;
    }

    @Override
    public String getName() {
        return "resource-overview";
    }
}
