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
package org.apache.flink.test.io;

import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.common.operators.util.TestNonRichOutputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.OutputFormatSinkFunction;
import org.apache.flink.test.util.JavaProgramTestBaseJUnit4;
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

/**
 * Tests for non rich DataSource and DataSink input output formats being correctly used at runtime.
 */
public class InputOutputITCase_RestartInjected_Randomized_123 extends JavaProgramTestBaseJUnit4 {

    @Override
    protected void testProgram() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TestNonRichOutputFormat output = new TestNonRichOutputFormat();
        env.createInput(new TestNonRichInputFormat()).addSink(new OutputFormatSinkFunction<>(output));
        env.execute();
        // we didn't break anything by making everything rich.
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
        testThread.join();
        org.junit.Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
        RestartFramework.at("after_job_submit").on(MINI_CLUSTER_RESOURCE).restart("jobmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
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
        RestartFramework.at("during_job_running").on(MINI_CLUSTER_RESOURCE).restart("taskmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
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
        testThread.start();
        Thread.sleep(500);
        testThread.join();
        RestartFramework.at("after_job_submit").on(MINI_CLUSTER_RESOURCE).restart("jobmanager").withIndex(0).withMode(RestartMode.GRACEFUL).execute();
        org.junit.Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
        try {
            postSubmit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            org.junit.Assert.fail("Post-submit work caused an error: " + e.getMessage());
        }
    }
}
