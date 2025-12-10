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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * An integration test for the sources based on iterators.
 *
 * <p>This test uses the {@link NumberSequenceSource} as a concrete iterator source implementation,
 * but covers all runtime-related aspects for all the iterator-based sources together.
 */
public class NumberSequenceSourceITCase_RestartInjected extends TestLogger {

    private static final List<Long> COLLECTED_RESULTS = Collections.synchronizedList(new ArrayList<>());

    private static class CollectSink implements SinkFunction<Long> {
        @Override
        public void invoke(Long value, Context context) {
            COLLECTED_RESULTS.add(value);
        }
    }

    private static final int PARALLELISM = 4;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    public void testParallelSourceExecution() throws Exception {
        COLLECTED_RESULTS.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        final DataStream<Long> stream =
                env.fromSource(
                        new NumberSequenceSource(1L, 1_000L),
                        WatermarkStrategy.noWatermarks(),
                        "iterator source");

        stream.addSink(new CollectSink());

        JobClient jobClient = env.executeAsync("testParallelSourceExecution");

        RestartFramework.at("after_job_submit")
            .on(MINI_CLUSTER)
            .restart("jobmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        Thread.sleep(500);

        RestartFramework.at("during_source_ingestion")
            .on(MINI_CLUSTER)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        jobClient.getJobExecutionResult().get();

        final List<Long> result = new ArrayList<>(COLLECTED_RESULTS);
        assertThat(result, containsInAnyOrder(LongStream.rangeClosed(1, 1000).boxed().toArray()));
    }

    @Test
    public void testCheckpointingWithDelayedAssignment() throws Exception {
        COLLECTED_RESULTS.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        final SingleOutputStreamOperator<Long> stream =
                env.fromSequence(0, 100)
                        .map(
                                x -> {
                                    if (x == 0) {
                                        Thread.sleep(50);
                                    }
                                    return x;
                                });
        stream.addSink(new CollectSink());

        JobClient jobClient = env.executeAsync("testCheckpointingWithDelayedAssignment");

        RestartFramework.at("after_job_submit")
            .on(MINI_CLUSTER)
            .restart("jobmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        Thread.sleep(100);

        RestartFramework.at("after_checkpoint")
            .on(MINI_CLUSTER)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        RestartFramework.at("during_job_running")
            .on(MINI_CLUSTER)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        jobClient.getJobExecutionResult().get();

        List<Long> result = new ArrayList<>(COLLECTED_RESULTS);
        assertThat(result, contains(LongStream.rangeClosed(0, 100).boxed().toArray()));
    }

    @Test
    public void testLessSplitsThanParallelism() throws Exception {
        COLLECTED_RESULTS.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        int n = PARALLELISM - 2;
        DataStream<Long> stream = env.fromSequence(0, n).map(l -> l);
        stream.addSink(new CollectSink());

        JobClient jobClient = env.executeAsync("testLessSplitsThanParallelism");

        RestartFramework.at("after_job_submit")
            .on(MINI_CLUSTER)
            .restart("jobmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        RestartFramework.at("during_task_execution")
            .on(MINI_CLUSTER)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        jobClient.getJobExecutionResult().get();

        List<Long> result = new ArrayList<>(COLLECTED_RESULTS);
        assertThat(result, containsInAnyOrder(LongStream.rangeClosed(0, n).boxed().toArray()));
    }
}
