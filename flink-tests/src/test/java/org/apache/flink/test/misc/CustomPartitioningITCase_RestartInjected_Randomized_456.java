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
package org.apache.flink.test.misc;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.JavaProgramTestBaseJUnit4;
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;
import org.junit.Assert;

/**
 * Integration tests for custom {@link Partitioner}.
 */
@SuppressWarnings("serial")
public class CustomPartitioningITCase_RestartInjected_Randomized_456 extends JavaProgramTestBaseJUnit4 {

    @Override
    protected void testProgram() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Assert.assertTrue(env.getParallelism() > 1);
        env.fromSequence(1, 1000).partitionCustom(new AllZeroPartitioner(), new IdKeySelector<Long>()).map(new FailExceptInPartitionZeroMapper()).sinkTo(new DiscardingSink<>());
        env.execute();
    }

    @Override
    public void testJobWithObjectReuse() throws Exception {
        try {
            preSubmit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            org.junit.Assert.fail("Pre-submit work caused an error: " + e.getMessage());
        }
        org.apache.flink.streaming.util.TestStreamEnvironment env = MINI_CLUSTER_RESOURCE.getTestStreamEnvironment();
        env.getConfig().enableObjectReuse();
        final Thread testThread = new Thread(() -> {
            try {
                testProgram();
                this.latestExecutionResult = env.getLastJobExecutionResult();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        testThread.start();
        Thread.sleep(500);
        RestartFramework.at("after_job_submit").on(MINI_CLUSTER_RESOURCE).restart("jobmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
        testThread.join();
        org.junit.Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
        RestartFramework.at("during_job_running").on(MINI_CLUSTER_RESOURCE).restart("taskmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
        try {
            postSubmit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            org.junit.Assert.fail("Post-submit work caused an error: " + e.getMessage());
        }
    }

    @Override
    public void testJobWithoutObjectReuse() throws Exception {
        try {
            preSubmit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            org.junit.Assert.fail("Pre-submit work caused an error: " + e.getMessage());
        }
        org.apache.flink.streaming.util.TestStreamEnvironment env = MINI_CLUSTER_RESOURCE.getTestStreamEnvironment();
        env.getConfig().disableObjectReuse();
        final Thread testThread = new Thread(() -> {
            try {
                testProgram();
                this.latestExecutionResult = env.getLastJobExecutionResult();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        RestartFramework.at("after_job_submit").on(MINI_CLUSTER_RESOURCE).restart("jobmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
        testThread.start();
        Thread.sleep(500);
        testThread.join();
        org.junit.Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
        RestartFramework.at("during_job_running").on(MINI_CLUSTER_RESOURCE).restart("taskmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
        try {
            postSubmit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            org.junit.Assert.fail("Post-submit work caused an error: " + e.getMessage());
        }
    }

    private static class FailExceptInPartitionZeroMapper extends RichMapFunction<Long, Long> {

        @Override
        public Long map(Long value) throws Exception {
            if (getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0) {
                return value;
            } else {
                throw new Exception("Received data in a partition other than partition 0");
            }
        }
    }

    private static class AllZeroPartitioner implements Partitioner<Long> {

        @Override
        public int partition(Long key, int numPartitions) {
            return 0;
        }
    }

    private static class IdKeySelector<T> implements KeySelector<T, T> {

        @Override
        public T getKey(T value) {
            return value;
        }
    }
}
