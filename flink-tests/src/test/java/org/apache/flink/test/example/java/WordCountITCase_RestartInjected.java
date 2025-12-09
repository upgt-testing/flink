/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.java;

import org.apache.flink.streaming.examples.wordcount.WordCount;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBaseJUnit4;
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;

/** Test {@link WordCount}. */
public class WordCountITCase_RestartInjected extends JavaProgramTestBaseJUnit4 {

    protected String textPath;
    protected String resultPath;

    @Override
    protected void preSubmit() throws Exception {
        textPath = createTempFile("text.txt", WordCountData.TEXT);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(WordCountData.COUNTS_AS_TUPLES, resultPath);
    }

    @Override
    protected void testProgram() throws Exception {
        WordCount.main(
                new String[] {
                    "--input", textPath,
                    "--output", resultPath,
                    "--execution-mode", "BATCH"
                });
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

        RestartFramework.at("after_job_submit")
            .on(MINI_CLUSTER_RESOURCE)
            .restart("jobmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        Thread.sleep(500);

        RestartFramework.at("during_job_running")
            .on(MINI_CLUSTER_RESOURCE)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        testThread.join();

        org.junit.Assert.assertNotNull(
                "The test program never triggered an execution.", this.latestExecutionResult);

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
        testThread.start();

        RestartFramework.at("after_job_submit_no_reuse")
            .on(MINI_CLUSTER_RESOURCE)
            .restart("jobmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        Thread.sleep(500);

        RestartFramework.at("during_job_running_no_reuse")
            .on(MINI_CLUSTER_RESOURCE)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        testThread.join();

        org.junit.Assert.assertNotNull(
                "The test program never triggered an execution.", this.latestExecutionResult);

        try {
            postSubmit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            org.junit.Assert.fail("Post-submit work caused an error: " + e.getMessage());
        }
    }

    private org.apache.flink.api.common.JobExecutionResult latestExecutionResult;
}
