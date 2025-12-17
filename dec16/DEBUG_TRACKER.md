# DEBUG_TRACKER - FLINK

This tracker lists failure groups ordered by likelihood of being actual bugs.
Groups are prioritized from most likely to be bugs to most likely to be false positives.

Total failure groups: 11

---

## Group 6 (Priority 2)

**Status:** [x] TEST-BUG - JUnit 4/5 incompatibility

**Execution Count:** 1

**Priority Reason:** Test bug - Caused by thrown from test code

**Root Cause Analysis:**
The test `CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing()` is a **JUnit 5** test (uses `@Test` from `org.junit.jupiter.api.Test`) but it extends `AbstractTestBaseJUnit4` which has `MINI_CLUSTER_RESOURCE` defined as a JUnit 4 `@ClassRule`.

JUnit 5 does NOT automatically execute JUnit 4 rules, so the `MINI_CLUSTER_RESOURCE.before()` method is never called, leaving the internal `miniCluster` field as `null`. When the test tries to restart using `RestartFramework.at("after_job_submit").on(MINI_CLUSTER_RESOURCE)`, it calls `cluster.getMiniCluster()` which returns `null`, causing the `NullPointerException`.

**Evidence:**
1. Test file: `CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:86-100`
2. Uses JUnit 5 imports: `org.junit.jupiter.api.Test`, `@BeforeEach`
3. Parent class `AbstractTestBaseJUnit4` uses JUnit 4 `@ClassRule` for `MINI_CLUSTER_RESOURCE`
4. Other test methods in the same class (`testRestoreAfterSomeTasksFinished`, `testFailoverAfterSomeTasksFinished`) correctly create their own `MiniCluster` instances instead of relying on the uninitialized `MINI_CLUSTER_RESOURCE`

**Fix Required:**
The test method `testImmediateCheckpointing()` should create and manage its own `MiniCluster` instance like the other test methods in the class, rather than trying to use `MINI_CLUSTER_RESOURCE` which is not initialized in JUnit 5 context.

**Location:** `flink-tests/src/test/java/org/apache/flink/test/checkpointing/CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:86-114`

### Generalized Stack Trace
```
Caused by: java.lang.NullPointerException
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java)
	... more
```

### Raw Stack Trace Sample
```
org.restarttest.core.RestartException: Restart failed at position after_job_submit
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing(CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:100)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)
Caused by: java.lang.NullPointerException: Cannot invoke "org.apache.flink.runtime.minicluster.MiniCluster.getHaLeadershipControl()" because "miniCluster" is null
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java:328)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:94)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java:170)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java:112)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:83)
	... 8 more

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected`
- Test Method: `testImmediateCheckpointing`
- Position: `after_job_submit`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `006-7f92108e`

---

## Group 7 (Priority 2)

**Status:** [x] TEST-BUG - JUnit 4/5 incompatibility (same as Group 6)

**Execution Count:** 1

**Priority Reason:** Test bug - Caused by thrown from test code

**Root Cause Analysis:**
The test `CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing()` has the **exact same root cause as Group 6** - it is a **JUnit 5** test (uses `@Test` from `org.junit.jupiter.api.Test`) but extends `AbstractTestBaseJUnit4` which has `MINI_CLUSTER_RESOURCE` defined as a JUnit 4 `@ClassRule`.

JUnit 5 does NOT automatically execute JUnit 4 rules, so the `MINI_CLUSTER_RESOURCE.before()` method is never called, leaving the internal `miniCluster` field as `null`. When the test tries to restart at position `during_job_running` using `RestartFramework.at("during_job_running").on(MINI_CLUSTER_RESOURCE)`, it attempts to call `miniCluster.isRunning()` (in FlinkStateCapture) or `miniCluster.getResourceOverview()` (in FlinkClusterAdapter) on the null miniCluster, causing the `NullPointerException`.

**Evidence:**
1. Test file: `CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:86-114`
2. Uses JUnit 5 imports: `org.junit.jupiter.api.Test`, `@BeforeEach`
3. Parent class `AbstractTestBaseJUnit4` uses JUnit 4 `@ClassRule` for `MINI_CLUSTER_RESOURCE`
4. Other test methods in the same class (`testRestoreAfterSomeTasksFinished`, `testFailoverAfterSomeTasksFinished`) correctly create their own `MiniCluster` instances instead of relying on the uninitialized `MINI_CLUSTER_RESOURCE`
5. Reproduced error: "Cannot invoke org.apache.flink.runtime.minicluster.MiniCluster.isRunning() because miniCluster is null"

**Fix Required:**
The test method `testImmediateCheckpointing()` should create and manage its own `MiniCluster` instance like the other test methods in the class, rather than trying to use `MINI_CLUSTER_RESOURCE` which is not initialized in JUnit 5 context.

**Location:** `flink-tests/src/test/java/org/apache/flink/test/checkpointing/CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:86-114`

### Generalized Stack Trace
```
Caused by: java.lang.NullPointerException
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartTaskManager(FlinkClusterAdapter.java)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java)
	... more
```

### Raw Stack Trace Sample
```
org.restarttest.core.RestartException: Restart failed at position during_job_running
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected.testImmediateCheckpointing(CheckpointAfterAllTasksFinishedITCase_RestartInjected.java:109)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)
Caused by: java.lang.NullPointerException: Cannot invoke "org.apache.flink.runtime.minicluster.MiniCluster.getResourceOverview()" because "miniCluster" is null
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartTaskManager(FlinkClusterAdapter.java:256)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:92)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java:170)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java:112)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:83)
	... 8 more

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.checkpointing.CheckpointAfterAllTasksFinishedITCase_RestartInjected`
- Test Method: `testImmediateCheckpointing`
- Position: `during_job_running`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `007-c0b59e24`

---

## Group 11 (Priority 2)

**Status:** [x] BUG - Job status corruption after early jobmanager restart

**Execution Count:** 1

**Priority Reason:** Application exception - org.apache.flink.runtime.client.JobExecutionException

**Root Cause Analysis:**
The test `RegionFailoverITCase_RestartInjected.testMultiRegionFailover()` triggers a **BUG in Flink's job status tracking/recovery mechanism** when the jobmanager is restarted shortly (2 seconds) after job submission.

**What Happens:**
1. The test submits a streaming job with region failover configuration
2. After 2 seconds, it restarts the jobmanager via HA leadership revocation/re-granting (line 154-159)
3. The jobmanager restart succeeds (HA leadership control is properly configured via TestingHAFactory with EmbeddedHaServicesWithLeadershipControl)
4. When the client requests the job result (line 179), the job has `ApplicationStatus.UNKNOWN`
5. This is an **illegal final status** - jobs should end with FINISHED, FAILED, or CANCELED, never UNKNOWN

**Why This is a BUG:**
- The test has proper HA configuration (TestingHAFactory with EmbeddedHaServicesWithLeadershipControl at lines 501-511)
- The jobmanager restart works correctly (no "requires HA leadership control" error like in Group 1)
- When a jobmanager is restarted (even early after submission), the job should be:
  - Properly recovered from HA services
  - Continue execution or fail gracefully
  - End with a **valid** final status (FINISHED/FAILED/CANCELED), not UNKNOWN
- The UNKNOWN status indicates the job recovery mechanism or status tracking has a defect

**Evidence:**
1. Test file: `RegionFailoverITCase_RestartInjected.java:147-180`
2. HA configuration: TestingHAFactory provides EmbeddedHaServicesWithLeadershipControl (lines 501-511)
3. Error: "Job completed with illegal application status: UNKNOWN" at line 179
4. The jobmanager restart at "after_job_submit" (2 seconds after submission) triggers the issue

**Affected Component:**
Flink's job recovery and/or application status tracking mechanism when jobmanager restarts occur early in the job lifecycle.

**Location:** `flink-tests/src/test/java/org/apache/flink/test/checkpointing/RegionFailoverITCase_RestartInjected.java:179`

### Generalized Stack Trace
```
org.apache.flink.runtime.client.JobExecutionException
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java)
```

### Raw Stack Trace Sample
```
org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
	at org.apache.flink.test.checkpointing.RegionFailoverITCase_RestartInjected.testMultiRegionFailover(RegionFailoverITCase_RestartInjected.java:179)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.lang.Thread.run(Thread.java:840)

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.checkpointing.RegionFailoverITCase_RestartInjected`
- Test Method: `testMultiRegionFailover`
- Position: `after_job_submit`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `002-97d01b8b`

---

## Group 1 (Priority 3)

**Status:** [x] TEST-BUG - Missing HA leadership control configuration

**Execution Count:** 95

**Priority Reason:** Exception with restart framework involvement

**Root Cause Analysis:**
Tests that attempt to restart the JobManager component require the MiniCluster to be configured with HA (High Availability) leadership control, but the affected tests do not have this configuration enabled.

**What Happens:**
1. Tests like `AvroStreamingFileSinkITCase_RestartInjected` extend `AbstractTestBaseJUnit4` which provides a `MINI_CLUSTER_RESOURCE`
2. The `MINI_CLUSTER_RESOURCE` is created without `.withHaLeadershipControl()` (see `AbstractTestBaseJUnit4.java:70-75`)
3. The restart-config.json explicitly configures jobmanager restart points (e.g., `before_avro_specific_execute`)
4. When the restart framework attempts to restart the jobmanager, the `FlinkClusterAdapter.restartJobManagerComponent()` method checks if HA leadership control is available
5. Since it's not configured, the method throws: `IllegalStateException: JobManager restart requires HA leadership control`

**Why This is a TEST-BUG:**
1. These are restart-injected tests (indicated by `_RestartInjected` suffix) specifically created for restart testing
2. The restart-config.json intentionally specifies jobmanager restart targets for these tests
3. The test infrastructure should have been updated to support jobmanager restarts by enabling HA leadership control
4. Other restart-injected tests in the codebase (e.g., `CacheITCase_RestartInjected.java`) correctly configure `.withHaLeadershipControl()`
5. Tests that inherit from `AbstractTestBaseJUnit4` cannot support jobmanager restarts because this base class uses a simple MiniCluster configuration without HA features

**Fix Required:**
Tests that need to support jobmanager restarts must create their own MiniClusterResource with HA leadership control enabled:
```java
new MiniClusterResourceConfiguration.Builder()
    .setNumberTaskManagers(1)
    .setNumberSlotsPerTaskManager(4)
    .withHaLeadershipControl()  // REQUIRED for jobmanager restarts
    .build()
```

Alternatively, these tests should be configured to only test taskmanager restarts, not jobmanager restarts.

**Affected Tests:**
- `AvroStreamingFileSinkITCase_RestartInjected` (all tests extending `AbstractTestBaseJUnit4`)
- `BoundedSourceITCase_RestartInjected` and other lifecycle tests
- Any test attempting jobmanager restarts without HA leadership control

**Location:**
- Base class: `flink-test-utils-parent/flink-test-utils/src/main/java/org/apache/flink/test/util/AbstractTestBaseJUnit4.java:70-75`
- Adapter check: `flink-restart-adapter/src/main/java/org/apache/flink/test/restarttest/FlinkClusterAdapter.java:331`

### Generalized Stack Trace
```
Caused by: java.lang.IllegalStateException
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java)
	... more
```

### Raw Stack Trace Sample
```
org.restarttest.core.RestartException: Restart failed at position before_avro_specific_execute
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroSpecific(AvroStreamingFileSinkITCase_RestartInjected.java:99)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: java.lang.IllegalStateException: JobManager restart requires HA leadership control. Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java:331)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:94)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java:170)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java:112)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:83)
	... 5 more

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected`
- Test Method: `testWriteAvroSpecific`
- Position: `before_avro_specific_execute`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `002-b4caf15a`

**Test 2:**
- Test Class: `org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected`
- Test Method: `testWriteAvroReflect`
- Position: `before_avro_reflect_execute`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `005-a7c22dd8`

**Test 3:**
- Test Class: `org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected`
- Test Method: `testWriteAvroGeneric`
- Position: `before_avro_generic_execute`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `002-24d6598c`

---

## Group 3 (Priority 3)

**Status:** [x] TEST-BUG - Missing HA leadership control configuration (same as Group 1)

**Execution Count:** 5

**Priority Reason:** Exception with restart framework involvement

**Root Cause Analysis:**
Tests in this group attempt to restart the JobManager component but the MiniCluster is not configured with HA (High Availability) leadership control. This is **identical to Group 1**.

**What Happens:**
1. Tests like `BoundedSourceITCase_RestartInjected`, `PartiallyFinishedSourcesITCase_RestartInjected`, and `StopWithSavepointITCase_RestartInjected` create a `MiniClusterWithClientResource` without `.withHaLeadershipControl()`
2. The restart-config.json explicitly configures jobmanager restart points (e.g., `before_bounded_source_execute`, `before_partially_finished_execute`, `before_stop_with_savepoint_execute`)
3. When the restart framework attempts to restart the jobmanager, the `FlinkClusterAdapter.restartJobManagerComponent()` method checks if HA leadership control is available (line 328-334)
4. Since it's not configured, the method throws: `IllegalStateException: JobManager restart requires HA leadership control`

**Why This is a TEST-BUG:**
1. These are restart-injected tests (indicated by `_RestartInjected` suffix) specifically created for restart testing
2. The restart-config.json intentionally specifies jobmanager restart targets for these tests
3. The test infrastructure should have been updated to support jobmanager restarts by enabling HA leadership control
4. Unlike tests that properly configure HA (e.g., `CacheITCase_RestartInjected.java`), these tests use a basic MiniClusterResourceConfiguration without `.withHaLeadershipControl()`
5. All affected tests create their MiniCluster configuration at test setup time and include jobmanager restart points in restart-config.json

**Fix Required:**
Tests that need to support jobmanager restarts must create their MiniClusterResource with HA leadership control enabled:
```java
new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(configuration())
    .setNumberTaskManagers(1)
    .setNumberSlotsPerTaskManager(4)
    .withHaLeadershipControl()  // REQUIRED for jobmanager restarts
    .build()
```

Alternatively, these tests should be configured to only test taskmanager restarts, not jobmanager restarts.

**Affected Tests:**
- `BoundedSourceITCase_RestartInjected`
- `PartiallyFinishedSourcesITCase_RestartInjected`
- `StopWithSavepointITCase_RestartInjected`

**Location:**
- Test files: `flink-tests/src/test/java/org/apache/flink/runtime/operators/lifecycle/*_RestartInjected.java`
- Adapter check: `flink-restart-adapter/src/main/java/org/apache/flink/test/restarttest/FlinkClusterAdapter.java:331`

**Reproduced:** Yes, using test `BoundedSourceITCase_RestartInjected#test` with restart parameters: position=`before_bounded_source_execute`, target=`jobmanager`, mode=`GRACEFUL`

### Generalized Stack Trace
```
Caused by: java.lang.IllegalStateException
```

### Raw Stack Trace Sample
```
org.restarttest.core.RestartException: Restart failed at position before_bounded_source_execute
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.runtime.operators.lifecycle.BoundedSourceITCase_RestartInjected.test(BoundedSourceITCase_RestartInjected.java:115)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: java.lang.IllegalStateException: JobManager restart requires HA leadership control. Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java:331)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:94)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
	at org.restarttest.core.RestartExecutor.performRestart(RestartExecutor.java:170)
	at org.restarttest.core.RestartExecutor.executeRestart(RestartExecutor.java:112)
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:83)
	... 5 more

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.runtime.operators.lifecycle.BoundedSourceITCase_RestartInjected`
- Test Method: `test`
- Position: `before_bounded_source_execute`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `006-42e17d33`

**Test 2:**
- Test Class: `org.apache.flink.runtime.operators.lifecycle.PartiallyFinishedSourcesITCase_RestartInjected`
- Test Method: `test`
- Position: `before_partially_finished_execute`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `006-52debbbc`

**Test 3:**
- Test Class: `org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected`
- Test Method: `test`
- Position: `before_stop_with_savepoint_execute`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `006-795228d7`

---

## Group 2 (Priority 5)

**Status:** [x] TEST-BUG - Missing restart strategy configuration

**Execution Count:** 6

**Priority Reason:** Test code issue - exception from test

**Root Cause Analysis:**
Tests in this group inject taskmanager restarts during job execution using the RestartFramework, but fail to configure the jobs with a restart strategy. When the taskmanager restarts gracefully, it shuts down and throws `FlinkExpectedException: The TaskExecutor is shutting down`, causing all tasks running on that taskmanager to fail.

**What Happens:**
1. Tests like `SinkV2MetricsITCase_RestartInjected.testCommitterMetrics()` explicitly call `RestartFramework.at("during_committer_execution").restart("taskmanager")` at line 180-185
2. The graceful taskmanager restart causes the TaskExecutor to shut down, throwing a `FlinkExpectedException`
3. This causes all tasks on that taskmanager to fail
4. The job has **NO restart strategy configured** (defaults to `NoRestartBackoffTimeStrategy`)
5. Flink's scheduler cannot recover: `"Recovery is suppressed by NoRestartBackoffTimeStrategy"`
6. The job fails with `JobExecutionException: Job execution failed`

**Why This is a TEST-BUG:**
1. The test developer added RestartFramework calls to inject taskmanager restarts for resilience testing
2. However, they forgot to configure the job with a restart strategy that would allow recovery from task failures
3. This is an incomplete test implementation - you cannot test resilience to taskmanager restarts if the job isn't configured to recover from task failures
4. The original non-restart-injected test (`SinkV2MetricsITCase.java`) doesn't restart taskmanagers during execution, so it doesn't need a restart strategy

**Fix Required:**
Tests that inject taskmanager restarts during job execution must configure the job with a restart strategy **before** job submission:
```java
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(1)));
```

**Evidence:**
1. Test file: `SinkV2MetricsITCase_RestartInjected.java:180-185` - explicit taskmanager restart call
2. Test file: `SinkV2MetricsITCase_RestartInjected.java:147-208` - no restart strategy configured
3. Error: `"Recovery is suppressed by NoRestartBackoffTimeStrategy"` at line 472 in stack trace
4. Reproduced with: `SinkV2MetricsITCase_RestartInjected#testCommitterMetrics` with restart parameters: position=`during_committer_execution`, target=`taskmanager`, mode=`GRACEFUL`

**Affected Tests (all similar pattern):**
- `SinkV2MetricsITCase_RestartInjected.testCommitterMetrics` (Test 1)
- `TimestampITCase_RestartInjected.testWatermarkPropagation` (Test 2)
- `StreamingOperatorsITCase_RestartInjected.testAsyncWaitOperator` (Test 3)
- Other tests in this group following the same pattern

**Location:** Multiple test files that use RestartFramework for taskmanager restarts without configuring restart strategies

### Generalized Stack Trace
```
Caused by: org.apache.flink.util.FlinkExpectedException
	at org.apache.flink.runtime.taskexecutor.TaskExecutor.onStop(TaskExecutor.java)
	at org.apache.flink.runtime.rpc.RpcEndpoint.internalCallOnStop(RpcEndpoint.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor$StartedState.lambda$terminate$0(PekkoRpcActor.java)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor$StartedState.terminate(PekkoRpcActor.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleControlMessage(PekkoRpcActor.java)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:29)
	at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)
```

### Raw Stack Trace Sample
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected.testCommitterMetrics(SinkV2MetricsITCase_RestartInjected.java:198)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:144)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.lambda$getJobExecutionResult$3(MiniClusterJobClient.java:140)
	at java.base/java.util.concurrent.CompletableFuture.uniApplyNow(CompletableFuture.java:684)
	at java.base/java.util.concurrent.CompletableFuture.uniApplyStage(CompletableFuture.java:662)
	at java.base/java.util.concurrent.CompletableFuture.thenApply(CompletableFuture.java:2168)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.getJobExecutionResult(MiniClusterJobClient.java:137)
	... 3 more
Caused by: org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:213)
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailureAndReport(ExecutionFailureHandler.java:163)
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.getFailureHandlingResult(ExecutionFailureHandler.java:118)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.recordTaskFailure(DefaultScheduler.java:294)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.handleTaskFailure(DefaultScheduler.java:285)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.onTaskFailed(DefaultScheduler.java:278)
	at org.apache.flink.runtime.scheduler.SchedulerBase.onTaskExecutionStateUpdate(SchedulerBase.java:836)
	at org.apache.flink.runtime.scheduler.SchedulerBase.updateTaskExecutionState(SchedulerBase.java:813)
	at org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener.notifyTaskFailure(UpdateSchedulerNgOnInternalFailuresListener.java:51)
	at org.apache.flink.runtime.executiongraph.DefaultExecutionGraph.notifySchedulerNgAboutInternalTaskFailure(DefaultExecutionGraph.java:1725)
	at org.apache.flink.runtime.executiongraph.Execution.processFail(Execution.java:1361)
	at org.apache.flink.runtime.executiongraph.Execution.processFail(Execution.java:1301)
	at org.apache.flink.runtime.executiongraph.Execution.fail(Execution.java:1002)
	at org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot.signalPayloadRelease(SingleLogicalSlot.java:195)
	at org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot.release(SingleLogicalSlot.java:182)
	at org.apache.flink.runtime.scheduler.SharedSlot.lambda$release$4(SharedSlot.java:270)
	at java.base/java.util.concurrent.CompletableFuture.uniAcceptNow(CompletableFuture.java:757)
	at java.base/java.util.concurrent.CompletableFuture.uniAcceptStage(CompletableFuture.java:735)
	at java.base/java.util.concurrent.CompletableFuture.thenAccept(CompletableFuture.java:2182)
	at org.apache.flink.runtime.scheduler.SharedSlot.release(SharedSlot.java:270)
	at org.apache.flink.runtime.jobmaster.slotpool.AllocatedSlot.releasePayload(AllocatedSlot.java:152)
	at org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool.releasePayload(DefaultDeclarativeSlotPool.java:549)
	at org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool.freeAndReleaseSlots(DefaultDeclarativeSlotPool.java:541)
	at org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool.releaseSlots(DefaultDeclarativeSlotPool.java:512)
	at org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolService.internalReleaseTaskManager(DeclarativeSlotPoolService.java:281)
	at org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolService.releaseTaskManager(DeclarativeSlotPoolService.java:237)
	at org.apache.flink.runtime.jobmaster.JobMaster.disconnectTaskManager(JobMaster.java:587)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.lambda$handleRpcInvocation$1(PekkoRpcActor.java:318)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:83)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcInvocation(PekkoRpcActor.java:316)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcMessage(PekkoRpcActor.java:229)
	at org.apache.flink.runtime.rpc.pekko.FencedPekkoRpcActor.handleRpcMessage(FencedPekkoRpcActor.java:88)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleMessage(PekkoRpcActor.java:174)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:29)
	at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)
	at scala.PartialFunction.applyOrElse$(PartialFunction.scala:126)
	at org.apache.pekko.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:29)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:175)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at org.apache.pekko.actor.Actor.aroundReceive(Actor.scala:547)
	at org.apache.pekko.actor.Actor.aroundReceive$(Actor.scala:545)
	at org.apache.pekko.actor.AbstractActor.aroundReceive(AbstractActor.scala:229)
	at org.apache.pekko.actor.ActorCell.receiveMessage(ActorCell.scala:590)
	at org.apache.pekko.actor.ActorCell.invoke(ActorCell.scala:557)
	at org.apache.pekko.dispatch.Mailbox.processMailbox(Mailbox.scala:272)
	at org.apache.pekko.dispatch.Mailbox.run(Mailbox.scala:233)
	at org.apache.pekko.dispatch.Mailbox.exec(Mailbox.scala:245)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)
Caused by: org.apache.flink.util.FlinkExpectedException: The TaskExecutor is shutting down.
	at org.apache.flink.runtime.taskexecutor.TaskExecutor.onStop(TaskExecutor.java:523)
	at org.apache.flink.runtime.rpc.RpcEndpoint.internalCallOnStop(RpcEndpoint.java:255)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor$StartedState.lambda$terminate$0(PekkoRpcActor.java:583)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:83)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor$StartedState.terminate(PekkoRpcActor.java:582)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleControlMessage(PekkoRpcActor.java:203)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:29)
	at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)
	at scala.PartialFunction.applyOrElse$(PartialFunction.scala:126)
	at org.apache.pekko.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:29)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:175)
	... 14 more

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected`
- Test Method: `testCommitterMetrics`
- Position: `during_committer_execution`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `010-c17626f9`

**Test 2:**
- Test Class: `org.apache.flink.test.streaming.runtime.TimestampITCase_RestartInjected`
- Test Method: `testWatermarkPropagation`
- Position: `during_watermark_propagation`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `010-112a917b`

**Test 3:**
- Test Class: `org.apache.flink.test.streaming.api.StreamingOperatorsITCase_RestartInjected`
- Test Method: `testAsyncWaitOperator`
- Position: `during_async_processing`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `007-ebb04f78`

---

## Group 4 (Priority 5)

**Status:** [x] TEST-BUG - Insufficient restart attempts for restart injection

**Execution Count:** 1

**Priority Reason:** Test code issue - exception from test

**Root Cause Analysis:**
The test `PartiallyFinishedSourcesITCase_RestartInjected.test()` configures a job with a restart strategy that allows only **1 restart attempt** (line 186), but when restart injection is enabled, the test experiences **2 failures** that both require restarts:

1. **First failure** (lines 151-156): The RestartFramework injects a taskmanager restart at position `during_partially_finished_execute`. When the taskmanager shuts down gracefully, all tasks running on it fail. The job uses its **1 allowed restart** to recover.

2. **Second failure** (lines 170-171): The test calls `executor.triggerFailover()` which sends a FAIL command to intentionally fail a task for testing purposes. The task fails with `"requested to fail"`, but the restart strategy has already been exhausted from the first failure.

When the second failure occurs, the restart strategy cannot recover: `"Recovery is suppressed by FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=1, backoffTimeMS=0)"`. The job enters FAILED state. The test's `triggerFailover()` method waits 10 seconds for a subtask to restart (indicated by an `OperatorStartedEvent`), but it never happens, causing the timeout error.

**Why This is a TEST-BUG:**
1. The original test (without restart injection) only had one intentional failure via `triggerFailover()`, so 1 restart attempt was sufficient
2. The restart injection adds an **unaccounted** failure when the taskmanager restarts gracefully
3. The test parameters specify `failover: true` (line 227-229), meaning the test explicitly tests failover scenarios
4. When both restart injection and failover are enabled, the test needs at least 2 restart attempts
5. This is a test infrastructure issue - the test didn't account for the additional restart consumed by the injected taskmanager restart

**Fix Required:**
When restart injection is enabled AND the test will trigger failovers (`failover` parameter is true), the restart strategy should allow sufficient attempts to handle both:
- Failures caused by injected restarts (taskmanager shutdowns)
- Intentional failures triggered by the test for validation

The test should configure at least 2-3 restart attempts:
```java
RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 0L);
```

Alternatively, if restart injection is deemed incompatible with this specific test's failover scenarios, the restart configuration should exclude positions that occur before the intentional failover is triggered.

**Location:** `flink-tests/src/test/java/org/apache/flink/runtime/operators/lifecycle/PartiallyFinishedSourcesITCase_RestartInjected.java:186`

**Reproduced:** Yes, using test `PartiallyFinishedSourcesITCase_RestartInjected#test[simple graph SINGLE_SUBTASK, failover: true, strategy: full]` with restart parameters: position=`during_partially_finished_execute`, target=`taskmanager`, mode=`GRACEFUL`

### Generalized Stack Trace
```
Caused by: java.lang.RuntimeException
	at org.apache.flink.runtime.operators.lifecycle.graph.TestEventSource.run(TestEventSource.java)
	at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java)
	at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java)
	at org.apache.flink.streaming.runtime.tasks.SourceStreamTask$LegacySourceFunctionThread.run(SourceStreamTask.java)
```

### Raw Stack Trace Sample
```
java.lang.RuntimeException: Unable to failover the job: No subtask restarted in 10000ms; job status: FAILED
	at org.apache.flink.runtime.operators.lifecycle.TestJobExecutor.handleFailoverTimeout(TestJobExecutor.java:180)
	at org.apache.flink.runtime.operators.lifecycle.TestJobExecutor.triggerFailover(TestJobExecutor.java:129)
	at org.apache.flink.runtime.operators.lifecycle.PartiallyFinishedSourcesITCase_RestartInjected.test(PartiallyFinishedSourcesITCase_RestartInjected.java:170)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: org.apache.flink.runtime.JobException: org.apache.flink.runtime.JobException: Recovery is suppressed by FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=1, backoffTimeMS=0)
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:213)
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailureAndReport(ExecutionFailureHandler.java:163)
	at org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.getFailureHandlingResult(ExecutionFailureHandler.java:118)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.recordTaskFailure(DefaultScheduler.java:294)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.handleTaskFailure(DefaultScheduler.java:285)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.onTaskFailed(DefaultScheduler.java:278)
	at org.apache.flink.runtime.scheduler.SchedulerBase.onTaskExecutionStateUpdate(SchedulerBase.java:836)
	at org.apache.flink.runtime.scheduler.SchedulerBase.updateTaskExecutionState(SchedulerBase.java:813)
	at org.apache.flink.runtime.scheduler.SchedulerNG.updateTaskExecutionState(SchedulerNG.java:83)
	at org.apache.flink.runtime.jobmaster.JobMaster.updateTaskExecutionState(JobMaster.java:532)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.lambda$handleRpcInvocation$1(PekkoRpcActor.java:318)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:83)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcInvocation(PekkoRpcActor.java:316)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcMessage(PekkoRpcActor.java:229)
	at org.apache.flink.runtime.rpc.pekko.FencedPekkoRpcActor.handleRpcMessage(FencedPekkoRpcActor.java:88)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleMessage(PekkoRpcActor.java:174)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:29)
	at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)
	at scala.PartialFunction.applyOrElse$(PartialFunction.scala:126)
	at org.apache.pekko.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:29)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:175)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at org.apache.pekko.actor.Actor.aroundReceive(Actor.scala:547)
	at org.apache.pekko.actor.Actor.aroundReceive$(Actor.scala:545)
	at org.apache.pekko.actor.AbstractActor.aroundReceive(AbstractActor.scala:229)
	at org.apache.pekko.actor.ActorCell.receiveMessage(ActorCell.scala:590)
	at org.apache.pekko.actor.ActorCell.invoke(ActorCell.scala:557)
	at org.apache.pekko.dispatch.Mailbox.processMailbox(Mailbox.scala:272)
	at org.apache.pekko.dispatch.Mailbox.run(Mailbox.scala:233)
	at org.apache.pekko.dispatch.Mailbox.exec(Mailbox.scala:245)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)
Caused by: java.lang.RuntimeException: java.lang.RuntimeException: requested to fail
	at org.apache.flink.runtime.operators.lifecycle.graph.TestEventSource.run(TestEventSource.java:87)
	at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:107)
	at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:68)
	at org.apache.flink.streaming.runtime.tasks.SourceStreamTask$LegacySourceFunctionThread.run(SourceStreamTask.java:346)

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.runtime.operators.lifecycle.PartiallyFinishedSourcesITCase_RestartInjected`
- Test Method: `test`
- Position: `during_partially_finished_execute`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `007-691e7a02`

---

## Group 5 (Priority 5)

**Status:** [x] TEST-BUG - Missing restart strategy configuration (same as Group 2)

**Execution Count:** 1

**Priority Reason:** Test code issue - exception from test

**Root Cause Analysis:**
The test `StopWithSavepointITCase_RestartInjected.test()` is designed to test the "stop with savepoint" feature, but when restart injection is enabled, it fails because the test does not configure a restart strategy.

**What Happens:**
1. The test creates a streaming job with `NoRestartStrategy` configured (TestJobBuilders.java:289)
2. The job starts and runs successfully, waiting for a WatermarkReceivedEvent (line 132)
3. At position `during_stop_with_savepoint_execute`, the RestartFramework injects a taskmanager restart (lines 139-144)
4. When the taskmanager shuts down gracefully, all tasks running on it fail
5. Since the job has `NoRestartStrategy` configured, Flink cannot recover from these failures
6. The job enters FAILED state and is eventually cleaned up/removed from the dispatcher
7. When the test tries to call `stopWithSavepoint()` at line 141, the job is no longer found in the dispatcher, resulting in `FlinkJobNotFoundException`

**Evidence:**
1. Debug output shows: Job status BEFORE taskmanager restart: RUNNING → Job status AFTER taskmanager restart: FAILED
2. Test file: `StopWithSavepointITCase_RestartInjected.java:139-141`
3. No restart strategy configured: `TestJobBuilders.java:289` explicitly sets `RestartStrategyUtils.configureNoRestartStrategy(env)`
4. Error: `FlinkJobNotFoundException: Could not find Flink job` when calling `stopWithSavepoint()`

**Why This is a TEST-BUG:**
1. This is identical to Group 2 - the test injects taskmanager restarts during job execution but fails to configure the job with a restart strategy
2. Without a restart strategy, the job cannot recover from task failures caused by the injected taskmanager restart
3. The original test (without restart injection) doesn't need a restart strategy because it doesn't inject any failures
4. However, when restart injection is enabled via restart-config.json, the test infrastructure should have been updated to support taskmanager restarts by configuring an appropriate restart strategy

**Fix Required:**
Tests that inject taskmanager restarts during job execution must configure the job with a restart strategy **before** job submission:
```java
RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 10, 1000L);
```

Additionally, after the taskmanager restart, the test should wait for the job to recover to RUNNING state before calling `stopWithSavepoint()`.

**Location:** `flink-tests/src/test/java/org/apache/flink/runtime/operators/lifecycle/graph/TestJobBuilders.java:289`

**Reproduced:** Yes, using test `StopWithSavepointITCase_RestartInjected#test[withDrain: true, simple graph]` with restart parameters: position=`during_stop_with_savepoint_execute`, target=`taskmanager`, mode=`GRACEFUL`

### Generalized Stack Trace
```
Caused by: org.apache.flink.runtime.messages.FlinkJobNotFoundException
	at org.apache.flink.runtime.dispatcher.Dispatcher.getJobMasterGateway(Dispatcher.java)
	at org.apache.flink.runtime.dispatcher.Dispatcher.performOperationOnJobMasterGateway(Dispatcher.java)
	at org.apache.flink.runtime.dispatcher.Dispatcher.stopWithSavepointAndGetLocation(Dispatcher.java)
	at java.base/java.lang.reflect.Method.invoke(Method.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.lambda$handleRpcInvocation$1(PekkoRpcActor.java)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcInvocation(PekkoRpcActor.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcMessage(PekkoRpcActor.java)
	at org.apache.flink.runtime.rpc.pekko.FencedPekkoRpcActor.handleRpcMessage(FencedPekkoRpcActor.java)
```

### Raw Stack Trace Sample
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.messages.FlinkJobNotFoundException: Could not find Flink job (4b06395d40c91929cdbdcc719716df14)
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.runtime.operators.lifecycle.TestJobExecutor.stopWithSavepoint(TestJobExecutor.java:108)
	at org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected.test(StopWithSavepointITCase_RestartInjected.java:141)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: org.apache.flink.runtime.messages.FlinkJobNotFoundException: Could not find Flink job (4b06395d40c91929cdbdcc719716df14)
	at org.apache.flink.runtime.dispatcher.Dispatcher.getJobMasterGateway(Dispatcher.java:1529)
	at org.apache.flink.runtime.dispatcher.Dispatcher.performOperationOnJobMasterGateway(Dispatcher.java:1544)
	at org.apache.flink.runtime.dispatcher.Dispatcher.stopWithSavepointAndGetLocation(Dispatcher.java:1090)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.lambda$handleRpcInvocation$1(PekkoRpcActor.java:318)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:83)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcInvocation(PekkoRpcActor.java:316)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcMessage(PekkoRpcActor.java:229)
	at org.apache.flink.runtime.rpc.pekko.FencedPekkoRpcActor.handleRpcMessage(FencedPekkoRpcActor.java:88)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleMessage(PekkoRpcActor.java:174)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:29)
	at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)
	at scala.PartialFunction.applyOrElse$(PartialFunction.scala:126)
	at org.apache.pekko.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:29)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:175)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at org.apache.pekko.actor.Actor.aroundReceive(Actor.scala:547)
	at org.apache.pekko.actor.Actor.aroundReceive$(Actor.scala:545)
	at org.apache.pekko.actor.AbstractActor.aroundReceive(AbstractActor.scala:229)
	at org.apache.pekko.actor.ActorCell.receiveMessage(ActorCell.scala:590)
	at org.apache.pekko.actor.ActorCell.invoke(ActorCell.scala:557)
	at org.apache.pekko.dispatch.Mailbox.processMailbox(Mailbox.scala:272)
	at org.apache.pekko.dispatch.Mailbox.run(Mailbox.scala:233)
	at org.apache.pekko.dispatch.Mailbox.exec(Mailbox.scala:245)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected`
- Test Method: `test`
- Position: `during_stop_with_savepoint_execute`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `007-31659634`

---

## Group 8 (Priority 5)

**Status:** [x] TEST-BUG - Missing restart strategy configuration (same pattern as Group 2)

**Execution Count:** 1

**Priority Reason:** Test code issue - exception from test

**Root Cause Analysis:**
The test `JobStatusChangedListenerITCase_RestartInjected.testJobStatusChangedForCancelledApplication()` injects a taskmanager restart during job execution but fails to configure the job with a restart strategy. When the taskmanager restarts gracefully, it causes all tasks running on it to fail. Without a restart strategy, the job cannot recover and enters FAILED state. The test then tries to cancel the job, but you cannot cancel a FAILED job, resulting in the `FlinkJobTerminatedWithoutCancellationException`.

**What Happens:**
1. The test creates a streaming job with an infinite source and sleeping sink (lines 191-192)
2. The job is submitted and starts running (lines 199, 208)
3. At position `during_job_running`, the RestartFramework injects a taskmanager restart (lines 210-215)
4. When the taskmanager shuts down gracefully, all tasks running on it fail
5. Since NO restart strategy is configured for the job, Flink cannot recover from these failures
6. The job enters FAILED state
7. When the test tries to cancel the job at line 218, it gets the error: "Flink job was not canceled, but instead FAILED"

**Why This is a TEST-BUG:**
1. The test injects a taskmanager restart during job execution via RestartFramework
2. However, the test does NOT configure any restart strategy in the job (no `setRestartStrategy()` call in the test method)
3. This is an incomplete test implementation - you cannot test resilience to taskmanager restarts if the job isn't configured to recover from task failures
4. The original non-restart-injected test doesn't need a restart strategy because it doesn't inject taskmanager restarts
5. This is the same pattern as Group 2 - taskmanager restart injection without restart strategy configuration

**Fix Required:**
The test must configure a restart strategy **before** job submission to handle task failures caused by the injected taskmanager restart:
```java
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(1)));
```

Additionally, after the taskmanager restart, the test should wait for the job to recover to RUNNING state before attempting to cancel it.

**Evidence:**
1. Test file: `JobStatusChangedListenerITCase_RestartInjected.java:210-218` - taskmanager restart injection followed by cancel attempt
2. Test file: Lines 189-196 - no restart strategy configuration anywhere in the test method
3. Error: `FlinkJobTerminatedWithoutCancellationException: Flink job was not canceled, but instead FAILED`
4. Reproduced successfully with the specified restart parameters

**Location:** `flink-tests/src/test/java/org/apache/flink/test/execution/JobStatusChangedListenerITCase_RestartInjected.java:189-220`

### Generalized Stack Trace
```
Caused by: org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException
	at org.apache.flink.runtime.dispatcher.Dispatcher.cancelJob(Dispatcher.java)
	at java.base/java.lang.reflect.Method.invoke(Method.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.lambda$handleRpcInvocation$1(PekkoRpcActor.java)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcInvocation(PekkoRpcActor.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcMessage(PekkoRpcActor.java)
	at org.apache.flink.runtime.rpc.pekko.FencedPekkoRpcActor.handleRpcMessage(FencedPekkoRpcActor.java)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleMessage(PekkoRpcActor.java)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
```

### Raw Stack Trace Sample
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException: Flink job (d9343ffceb55cd35eeaf90f405458ac4) was not canceled, but instead FAILED.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.execution.JobStatusChangedListenerITCase_RestartInjected.testJobStatusChangedForCancelledApplication(JobStatusChangedListenerITCase_RestartInjected.java:218)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)
Caused by: org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException: Flink job (d9343ffceb55cd35eeaf90f405458ac4) was not canceled, but instead FAILED.
	at org.apache.flink.runtime.dispatcher.Dispatcher.cancelJob(Dispatcher.java:824)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.lambda$handleRpcInvocation$1(PekkoRpcActor.java:318)
	at org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:83)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcInvocation(PekkoRpcActor.java:316)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleRpcMessage(PekkoRpcActor.java:229)
	at org.apache.flink.runtime.rpc.pekko.FencedPekkoRpcActor.handleRpcMessage(FencedPekkoRpcActor.java:88)
	at org.apache.flink.runtime.rpc.pekko.PekkoRpcActor.handleMessage(PekkoRpcActor.java:174)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:33)
	at org.apache.pekko.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:29)
	at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)
	at scala.PartialFunction.applyOrElse$(PartialFunction.scala:126)
	at org.apache.pekko.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:29)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:175)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)
	at org.apache.pekko.actor.Actor.aroundReceive(Actor.scala:547)
	at org.apache.pekko.actor.Actor.aroundReceive$(Actor.scala:545)
	at org.apache.pekko.actor.AbstractActor.aroundReceive(AbstractActor.scala:229)
	at org.apache.pekko.actor.ActorCell.receiveMessage(ActorCell.scala:590)
	at org.apache.pekko.actor.ActorCell.invoke(ActorCell.scala:557)
	at org.apache.pekko.dispatch.Mailbox.processMailbox(Mailbox.scala:272)
	at org.apache.pekko.dispatch.Mailbox.run(Mailbox.scala:233)
	at org.apache.pekko.dispatch.Mailbox.exec(Mailbox.scala:245)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.execution.JobStatusChangedListenerITCase_RestartInjected`
- Test Method: `testJobStatusChangedForCancelledApplication`
- Position: `during_job_running`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `007-b4214a75`

---

## Group 9 (Priority 5)

**Status:** [x] TEST-BUG - CyclicBarrier synchronization incompatible with restart injection

**Execution Count:** 1

**Priority Reason:** Test code issue - exception from test

**Root Cause Analysis:**
The test `SinkV2MetricsITCase_RestartInjected.testMetrics()` uses `CyclicBarrier` for synchronization between the test thread and worker threads in the Flink job. When the taskmanager is restarted during the test, the barrier coordination is broken because:

1. **Missing restart strategy**: The test does NOT configure any restart strategy for the job (no `env.setRestartStrategy()` call anywhere in the test method)
2. **Job fails after taskmanager restart**: When the taskmanager restarts gracefully at position `during_sink_metrics_check` (lines 130-137), all tasks running on it are killed. Without a restart strategy, the job cannot recover and enters FAILED state
3. **Barrier synchronization breaks**: The test uses CyclicBarrier with 3 parties (2 worker threads + 1 test thread) at lines 96-99. After the first barrier synchronization succeeds (line 129), the test restarts the taskmanager. Worker threads that were supposed to reach the second barrier (`afterBarrier.get().await()` at line 113) are killed when the taskmanager shuts down. The test thread waits at line 137 for parties that will never arrive, causing `BrokenBarrierException`

**What Happens:**
1. Job starts with parallelism 4, `numSplits = 2` (line 90)
2. Barriers are created with 3 parties: 2 worker threads + 1 test thread (lines 96-99)
3. Workers process records and wait at `beforeBarrier` when reaching record 4 (lines 110-113)
4. Test thread also waits at `beforeBarrier` (line 129) - all 3 parties arrive and are released
5. Test restarts taskmanager (lines 130-137)
6. **Debug output shows**: Job status changes from RUNNING to FAILED after taskmanager restart
7. Workers are killed and cannot reach `afterBarrier.get().await()` at line 113
8. Test thread waits at line 137, but workers never arrive - barrier breaks
9. `BrokenBarrierException` is thrown

**Why This is a TEST-BUG:**
1. **Missing restart strategy** (same as Groups 2, 5, 8): The test injects a taskmanager restart during job execution but fails to configure the job with a restart strategy to handle task failures
2. **Incompatible synchronization mechanism**: The test uses CyclicBarrier for synchronization between test thread and worker threads. This synchronization mechanism is fundamentally incompatible with restart injection because:
   - CyclicBarrier expects a specific number of parties to arrive
   - When the taskmanager restarts, worker threads are killed mid-execution
   - Without a restart strategy, the job cannot recover and workers never reach the barrier
   - Even with a restart strategy, the barrier state would be disrupted by task restarts and the coordination timing would be completely broken

**Evidence:**
1. Test file: `SinkV2MetricsITCase_RestartInjected.java:87-144`
2. No restart strategy configured (verified with grep - no `setRestartStrategy()` call)
3. Debug output added to the test shows: Job status RUNNING → taskmanager restart → Job status FAILED
4. Error occurs at line 137 when test thread tries to await on barrier that will never complete
5. Reproduced successfully with the specified restart parameters

**Fix Required:**
This test's design is fundamentally incompatible with restart injection at the `during_sink_metrics_check` position. Two possible fixes:

1. **Remove restart injection at this position**: Configure the restart-config.json to exclude the `during_sink_metrics_check` position for this test, as the CyclicBarrier synchronization mechanism cannot survive taskmanager restarts

2. **Redesign the test synchronization**: Replace CyclicBarrier with a more resilient synchronization mechanism that can survive job restarts (e.g., external coordination via shared state, or polling-based synchronization)

Additionally, if taskmanager restart injection is kept, the test must configure a restart strategy:
```java
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(1)));
```

**Location:** `flink-tests/src/test/java/org/apache/flink/test/streaming/runtime/SinkV2MetricsITCase_RestartInjected.java:87-144`

**Reproduced:** Yes, using test `SinkV2MetricsITCase_RestartInjected#testMetrics` with restart parameters: position=`during_sink_metrics_check`, target=`taskmanager`, mode=`GRACEFUL`

### Generalized Stack Trace
```
java.util.concurrent.BrokenBarrierException
	at java.base/java.util.concurrent.CyclicBarrier.dowait(CyclicBarrier.java)
	at java.base/java.util.concurrent.CyclicBarrier.await(CyclicBarrier.java)
```

### Raw Stack Trace Sample
```
java.util.concurrent.BrokenBarrierException
	at java.base/java.util.concurrent.CyclicBarrier.dowait(CyclicBarrier.java:210)
	at java.base/java.util.concurrent.CyclicBarrier.await(CyclicBarrier.java:364)
	at org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected.testMetrics(SinkV2MetricsITCase_RestartInjected.java:137)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.flink.util.TestNameProvider$1.evaluate(TestNameProvider.java:45)

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.streaming.runtime.SinkV2MetricsITCase_RestartInjected`
- Test Method: `testMetrics`
- Position: `during_sink_metrics_check`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Execution Dir: `010-1a7c65e6`

---

## Group 10 (Priority 5)

**Status:** [x] BUG - BATCH job remains SUSPENDED after early jobmanager restart

**Execution Count:** 1

**Priority Reason:** Test code issue - exception from test

**Root Cause Analysis:**
The test `CacheITCase_RestartInjected.testCacheProduceAndConsumeWithDifferentPartitioner()` exposes a **BUG in Flink's job recovery mechanism for BATCH jobs** when the jobmanager is restarted very early in the job lifecycle (during INITIALIZING phase).

**What Happens:**
1. The test is properly configured with HA leadership control (line 86: `.withHaLeadershipControl()`)
2. A BATCH job is submitted (line 98: `env.setRuntimeMode(RuntimeExecutionMode.BATCH)`)
3. Job starts in INITIALIZING state
4. JobManager is restarted at position "after_job_submit" (immediately after submission)
5. The jobmanager restart succeeds (no "requires HA leadership control" error)
6. **BUG**: Job transitions to SUSPENDED state and never recovers
7. Job remains SUSPENDED even after waiting and after taskmanager restart
8. When `jobClient.getJobExecutionResult().get()` is called, the job has ApplicationStatus.UNKNOWN
9. This causes the error: "Job completed with illegal application status: UNKNOWN"

**Debug Evidence:**
```
DEBUG: Job status BEFORE jobmanager restart: INITIALIZING
DEBUG: Job status AFTER jobmanager restart: SUSPENDED
DEBUG: Job status AFTER taskmanager restart: SUSPENDED
```

**Why This is a BUG:**
1. The test has proper HA configuration with leadership control, so jobmanager restarts should be supported
2. According to `JobStatus.java`, SUSPENDED state is entered when "the executing JobManager loses its leader status" (lines 98-99)
3. SUSPENDED is a LOCALLY terminal state but NOT a GLOBALLY terminal state
4. When a BATCH job enters SUSPENDED state due to JobManager leadership loss, it should be:
   - Recovered from the HA job store when JobManager regains leadership
   - Resumed and transitioned back to RUNNING state to complete execution
5. **However**, the job remains in SUSPENDED state indefinitely and never recovers
6. ApplicationStatus.fromJobStatus() returns UNKNOWN for any non-globally-terminal JobStatus (including SUSPENDED)
7. The error "Job completed with illegal application status: UNKNOWN" occurs because SUSPENDED is not a valid final state

**Expected Behavior:**
When a BATCH job is suspended due to early JobManager restart:
- **Option 1 (Preferred)**: Job should be automatically recovered from HA store and resumed when JobManager regains leadership
- **Option 2**: Job should fail gracefully with FAILED status if BATCH jobs don't support recovery from this state
- **NOT acceptable**: Job remaining in SUSPENDED state indefinitely with UNKNOWN application status

**Affected Component:**
Flink's job recovery and scheduling mechanism for BATCH jobs when JobManager restart occurs during INITIALIZING phase.

**Location:**
- Test: `flink-tests/src/test/java/org/apache/flink/test/streaming/runtime/CacheITCase_RestartInjected.java:186,289`
- Error source: `flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobResult.java:148`
- JobStatus definition: `flink-core/src/main/java/org/apache/flink/api/common/JobStatus.java:60`

**Reproduced:** Yes, using test parameters: position=`after_job_submit`, target=`jobmanager`, mode=`GRACEFUL`

### Generalized Stack Trace
```
Caused by: org.apache.flink.runtime.client.JobExecutionException
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.lambda$getJobExecutionResult$3(MiniClusterJobClient.java)
	at java.base/java.util.concurrent.CompletableFuture.uniApplyNow(CompletableFuture.java)
	at java.base/java.util.concurrent.CompletableFuture.uniApplyStage(CompletableFuture.java)
	at java.base/java.util.concurrent.CompletableFuture.thenApply(CompletableFuture.java)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.getJobExecutionResult(MiniClusterJobClient.java)
	... more
```

### Raw Stack Trace Sample
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
	at org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected.executeAndVerifyResult(CacheITCase_RestartInjected.java:289)
	at org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected.testCacheProduceAndConsumeWithDifferentPartitioner(CacheITCase_RestartInjected.java:186)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:373)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(ForkJoinPool.java:1182)
	at java.base/java.util.concurrent.ForkJoinPool.scan(ForkJoinPool.java:1655)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1622)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)
Caused by: org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
	at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.lambda$getJobExecutionResult$3(MiniClusterJobClient.java:140)
	at java.base/java.util.concurrent.CompletableFuture.uniApplyNow(CompletableFuture.java:684)
	at java.base/java.util.concurrent.CompletableFuture.uniApplyStage(CompletableFuture.java:662)
	at java.base/java.util.concurrent.CompletableFuture.thenApply(CompletableFuture.java:2168)
	at org.apache.flink.runtime.minicluster.MiniClusterJobClient.getJobExecutionResult(MiniClusterJobClient.java:137)
	... 8 more

```

### Sample Test Executions

**Test 1:**
- Test Class: `org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected`
- Test Method: `testCacheProduceAndConsumeWithDifferentPartitioner`
- Position: `after_job_submit`
- Target: `jobmanager`
- Mode: `GRACEFUL`
- Execution Dir: `006-e42bc23c`

---
