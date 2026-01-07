# DEBUG TRACKER - Flink Dec20 Grouped Failures

This document tracks 12 failure groups ordered by likelihood of being an actual bug (highest priority first).

**Priority Guidelines:**
- HIGHEST PRIORITY: NPE/IndexOutOfBounds from NON-test code (excluding restarttest modules)
- LOWER PRIORITY: Failures from restarttest modules, timeouts, test infrastructure issues

---

## HIGH PRIORITY - Likely Actual Bugs

### [ ] Group 3: NoResourceAvailableException
**Group ID:** 3

**Raw Stacktrace Sample:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase_RestartInjected.testSlidingTimeWindow(EventTimeWindowCheckpointingITCase_RestartInjected.java:634)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:144)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.lambda$getJobExecutionResult$3(MiniClusterJobClient.java:140)
...
Caused by: org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase_RestartInjected.testSlidingTimeWindow`
   - "position": "after_checkpoint"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-0268cb32"

**Analysis:** NoResourceAvailableException from core Flink scheduler - indicates possible resource management bug after restart.

---

### [ ] Group 10: TaskNotRunningException
**Group ID:** 10

**Raw Stacktrace Sample:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.streaming.runtime.SideOutputITCase_RestartInjected.testWatermarkForwarding(SideOutputITCase_RestartInjected.java:185)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:144)
...
Caused by: org.apache.flink.runtime.operators.coordination.TaskNotRunningException
	at org.apache.flink.runtime.taskmanager.Task.deliverOperatorEvent(Task.java)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.streaming.runtime.SideOutputITCase_RestartInjected.testWatermarkForwarding`
   - "position": "during_job_running"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "006-6c971db6"

**Analysis:** TaskNotRunningException from runtime code - possible race condition in task coordination after restart.

---

### [ ] Group 7: FlinkJobNotFoundException
**Group ID:** 7

**Raw Stacktrace Sample:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.messages.FlinkJobNotFoundException: Could not find Flink job (d5b138cda77b2cb55f24979502d5e282)
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.runtime.operators.lifecycle.TestJobExecutor.stopWithSavepoint(TestJobExecutor.java:108)
	at org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected.test(StopWithSavepointITCase_RestartInjected.java:138)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.messages.FlinkJobNotFoundException: Could not find Flink job (d5b138cda77b2cb55f24979502d5e282)
	at org.apache.flink.runtime.dispatcher.Dispatcher.getJobMasterGateway(Dispatcher.java:1529)
	at org.apache.flink.runtime.dispatcher.Dispatcher.performOperationOnJobMasterGateway(Dispatcher.java:1544)
	at org.apache.flink.runtime.dispatcher.Dispatcher.stopWithSavepointAndGetLocation(Dispatcher.java:1090)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected.test`
   - "position": "during_stop_with_savepoint_execute"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-31659634"

**Analysis:** Job lost after restart - possible issue with job recovery or dispatcher state management.

---

### [ ] Group 11: FlinkJobTerminatedWithoutCancellationException
**Group ID:** 11

**Raw Stacktrace Sample:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException: Flink job (20177d5060263fde929cd5aba0155f1c) was not canceled, but instead FAILED.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.execution.JobStatusChangedListenerITCase_RestartInjected.testJobStatusChangedForCancelledApplication(JobStatusChangedListenerITCase_RestartInjected.java:227)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException: Flink job (20177d5060263fde929cd5aba0155f1c) was not canceled, but instead FAILED.
	at org.apache.flink.runtime.dispatcher.Dispatcher.cancelJob(Dispatcher.java:824)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.execution.JobStatusChangedListenerITCase_RestartInjected.testJobStatusChangedForCancelledApplication`
   - "position": "during_job_running"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-b4214a75"

**Analysis:** Job failed instead of being properly cancelled - possible state management issue after restart.

---

### [ ] Group 4: JobExecutionException (UNKNOWN status)
**Group ID:** 4

**Raw Stacktrace Sample:**
```
org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
	at org.apache.flink.test.checkpointing.RegionFailoverITCase_RestartInjected.testMultiRegionFailover(RegionFailoverITCase_RestartInjected.java:179)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.checkpointing.RegionFailoverITCase_RestartInjected.testMultiRegionFailover`
   - "position": "after_job_submit"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-97d01b8b"

**Analysis:** Job status corrupted to UNKNOWN - indicates serious state tracking issue after restart.

---

### [ ] Group 5: JobExecutionException (UNKNOWN status - variant)
**Group ID:** 5

**Raw Stacktrace Sample:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected.executeAndVerifyResult(CacheITCase_RestartInjected.java:297)
	at org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected.testCacheProduceAndConsumeWithDifferentPartitioner(CacheITCase_RestartInjected.java:186)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
...
Caused by: org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected.testCacheProduceAndConsumeWithDifferentPartitioner`
   - "position": "after_job_submit"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "005-e42bc23c"

**Analysis:** Same as Group 4 - job status corrupted to UNKNOWN after restart.

---

### [ ] Group 2: FlinkExpectedException (Job FAILED instead of RUNNING)
**Group ID:** 2

**Raw Stacktrace Sample:**
```
java.lang.IllegalStateException: Job has entered FAILED state, but expecting [RUNNING]
	at org.apache.flink.runtime.testutils.CommonTestUtils.lambda$waitForJobStatus$7(CommonTestUtils.java:286)
	at org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(CommonTestUtils.java:152)
	at org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition(CommonTestUtils.java:146)
	at org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus(CommonTestUtils.java:270)
	at org.apache.flink.test.checkpointing.ManualCheckpointITCase_RestartInjected.testTriggeringWhenPeriodicDisabled(ManualCheckpointITCase_RestartInjected.java:116)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
...
Caused by: org.apache.flink.util.FlinkExpectedException
	at org.apache.flink.runtime.taskexecutor.TaskExecutor.onStop(TaskExecutor.java)
```

**Test Executions (6 total):**
1. Test: `org.apache.flink.test.checkpointing.ManualCheckpointITCase_RestartInjected.testTriggeringWhenPeriodicDisabled`
   - "position": "during_keyed_state_processing"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "004-65516fc0"
2. Test: `org.apache.flink.api.connector.source.lib.NumberSequenceSourceITCase_RestartInjected.testParallelSourceExecution`
   - "position": "during_source_ingestion"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-d246ed9f"
3. Test: `org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected.testCommitterMetrics`
   - "position": "during_committer_execution"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "006-c17626f9"
4. Test: `org.apache.flink.test.streaming.runtime.TimestampITCase_RestartInjected.testWatermarkPropagation`
   - "position": "during_watermark_propagation"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "006-112a917b"
5. Test: `org.apache.flink.test.streaming.runtime.BroadcastStateITCase_RestartInjected.testKeyedWithBroadcastTranslation`
   - "position": "during_broadcast_processing"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-916dd98b"
6. Test: `org.apache.flink.test.streaming.runtime.BroadcastStateITCase_RestartInjected.testBroadcastTranslation`
   - "position": "during_broadcast_processing"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-4dffaabf"

**Analysis:** Jobs unexpectedly failing after taskmanager restart - FlinkExpectedException suggests expected behavior, but jobs should recover.

---

### [ ] Group 6: RuntimeException (Failover timeout)
**Group ID:** 6

**Raw Stacktrace Sample:**
```
java.lang.RuntimeException: Unable to failover the job: No subtask restarted in 10000ms; job status: FAILED
	at org.apache.flink.runtime.operators.lifecycle.TestJobExecutor.handleFailoverTimeout(TestJobExecutor.java:180)
	at org.apache.flink.runtime.operators.lifecycle.TestJobExecutor.triggerFailover(TestJobExecutor.java:129)
	at org.apache.flink.runtime.operators.lifecycle.PartiallyFinishedSourcesITCase_RestartInjected.test(PartiallyFinishedSourcesITCase_RestartInjected.java:166)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.JobException: org.apache.flink.runtime.JobException: Recovery is suppressed by FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=1, backoffTimeMS=0)
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:213)
...
Caused by: java.lang.RuntimeException
	at org.apache.flink.runtime.operators.lifecycle.graph.TestEventSource.run(TestEventSource.java)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.runtime.operators.lifecycle.PartiallyFinishedSourcesITCase_RestartInjected.test`
   - "position": "during_partially_finished_execute"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-691e7a02"

**Analysis:** Failover not recovering - RuntimeException from TestEventSource suggests test code, but failover timeout indicates recovery issue.

---

## MEDIUM PRIORITY - Test Infrastructure or Test Code Issues

### [ ] Group 12: BrokenBarrierException
**Group ID:** 12

**Raw Stacktrace Sample:**
```
java.util.concurrent.BrokenBarrierException
	at java.base/java.util.concurrent.CyclicBarrier.dowait(CyclicBarrier.java:210)
	at java.base/java.util.concurrent.CyclicBarrier.await(CyclicBarrier.java:364)
	at org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected.testMetrics(SinkV2MetricsITCase_RestartInjected.java:137)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected.testMetrics`
   - "position": "during_sink_metrics_check"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "006-1a7c65e6"

**Analysis:** BrokenBarrierException from test synchronization code - likely test infrastructure issue.

---

## LOW PRIORITY - RestartTest Infrastructure Issues

### [ ] Group 8: NullPointerException (restartJobManagerComponent)
**Group ID:** 8

**Raw Stacktrace Sample:**
```
org.restarttest.core.RestartException: Restart failed at position after_job_submit
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing(CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:100)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
...
Caused by: java.lang.NullPointerException: Cannot invoke "org.apache.flink.runtime.minicluster.MiniCluster.getHaLeadershipControl()" because "miniCluster" is null
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java:328)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:94)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing`
   - "position": "after_job_submit"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-7f92108e"

**Analysis:** NPE from restarttest infrastructure - miniCluster is null. RestartTest framework issue.

---

### [ ] Group 9: NullPointerException (restartTaskManager)
**Group ID:** 9

**Raw Stacktrace Sample:**
```
org.restarttest.core.RestartException: Restart failed at position during_job_running
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing(CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:109)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
...
Caused by: java.lang.NullPointerException: Cannot invoke "org.apache.flink.runtime.minicluster.MiniCluster.getResourceOverview()" because "miniCluster" is null
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartTaskManager(FlinkClusterAdapter.java:256)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:92)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
```

**Test Executions (1 total):**
1. Test: `org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing`
   - "position": "during_job_running"
   - "target": "taskmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "003-c0b59e24"

**Analysis:** NPE from restarttest infrastructure - miniCluster is null. RestartTest framework issue.

---

### [ ] Group 1: IllegalStateException (HA leadership control required)
**Group ID:** 1

**Raw Stacktrace Sample:**
```
org.restarttest.core.RestartException: Restart failed at position before_avro_specific_execute
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroSpecific(AvroStreamingFileSinkITCase_RestartInjected.java:96)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: java.lang.IllegalStateException: JobManager restart requires HA leadership control. Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java:331)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:94)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java:170)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java:112)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:83)
```

**Test Executions (100 total):**
1. Test: `org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroSpecific`
   - "position": "before_avro_specific_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-b4caf15a"
2. Test: `org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroGeneric`
   - "position": "before_avro_generic_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-24d6598c"
3. Test: `org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroReflect`
   - "position": "before_avro_reflect_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-a7c22dd8"
4. Test: `org.apache.flink.hdfstests.ContinuousFileProcessingITCase_RestartInjected.testProgram`
   - "position": "before_file_processing_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-c7ef6824"
5. Test: `org.apache.flink.hdfstests.DistributedCacheDfsTest_RestartInjected.testDistributedFileViaDFS`
   - "position": "before_dfs_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-c708c58f"
6. Test: `org.apache.flink.hdfstests.DistributedCacheDfsTest_RestartInjected.testSubmittingJobViaRestClusterClient`
   - "position": "before_rest_client_submit"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-dd333458"
7. Test: `org.apache.flink.cep.CEPITCase_RestartInjected.testSimplePatternCEP`
   - "position": "before_cep_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-1e85c829"
8. Test: `org.apache.flink.cep.CEPITCase_RestartInjected.testProcessingTimeWithinPreviousAndCurrent`
   - "position": "before_window_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "005-d46d649e"
9. Test: `org.apache.flink.cep.CEPITCase_RestartInjected.testProcessingTimeWithinBetweenFirstAndLast`
   - "position": "before_window_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "002-afc0a4f0"
10. Test: `org.apache.flink.cep.CEPITCase_RestartInjected.testSimpleKeyedPatternCEP`
   - "position": "before_keyed_cep_execute"
   - "target": "jobmanager"
   - "mode": "GRACEFUL"
   - "index": "0"
   - "executionDir": "005-48d76e63"
... (90 more tests)

**Analysis:** RestartTest configuration issue - 100 tests failed due to missing HA leadership control configuration. RestartTest framework setup issue.

---

## Summary Statistics

- **Total Groups:** 12
- **High Priority (Likely Bugs):** 8 groups
- **Medium Priority (Test Infrastructure):** 1 group
- **Low Priority (RestartTest Issues):** 3 groups

**Key Findings:**
- Groups 3, 10, 7, 11, 4, 5: Core runtime issues with resource management, job state, and coordination
- Group 2: Jobs failing unexpectedly after taskmanager restarts (6 occurrences)
- Group 6: Failover recovery timeout issue
- Group 1: Configuration issue affecting 100 test executions (most common failure)
- Groups 8, 9: RestartTest infrastructure NPEs (same test, different restart points)
- Group 12: Test synchronization issue
