# FP-GROUP-2: FlinkExpectedException (Job FAILED instead of RUNNING)

## Classification: FALSE POSITIVE

## Summary
The failure in Group 2 is a false positive because the restart was injected into a test that was explicitly designed WITHOUT a restart strategy. When the TaskManager is killed, Flink correctly fails the job because no restart strategy is configured to handle the failure.

## Test Information
- **Test**: `org.apache.flink.test.checkpointing.ManualCheckpointITCase_RestartInjected.testTriggeringWhenPeriodicDisabled`
- **Restart Position**: `during_keyed_state_processing`
- **Target**: `taskmanager`
- **Mode**: `GRACEFUL`

## Error Analysis

### Stack Trace
```
java.lang.IllegalStateException: Job has entered FAILED state, but expecting [RUNNING]
...
Caused by: org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy
...
Caused by: org.apache.flink.util.FlinkExpectedException: The TaskExecutor is shutting down.
    at org.apache.flink.runtime.taskexecutor.TaskExecutor.onStop(TaskExecutor.java:523)
```

### Root Cause
1. The test `testTriggeringWhenPeriodicDisabled` does NOT call `env.enableCheckpointing()` - this is intentional as the test is specifically designed to test manual checkpoint triggering without periodic checkpoints

2. Without enabling checkpointing, no restart strategy is configured (uses `NoRestartBackoffTimeStrategy`)

3. When the TaskManager is killed at position `during_keyed_state_processing`:
   - All tasks on the TaskManager fail with `FlinkExpectedException: The TaskExecutor is shutting down.`
   - Flink checks restart strategy and finds `NoRestartBackoffTimeStrategy`
   - `NoRestartBackoffTimeStrategy.canRestart()` returns `false`
   - The entire job fails with `Recovery is suppressed by NoRestartBackoffTimeStrategy`

4. The test then calls `CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.RUNNING))` which throws the exception because the job is in FAILED state

## Why This is NOT a Bug

### 1. Correct Flink Behavior
The `NoRestartBackoffTimeStrategy` is specifically designed to NOT restart tasks:

```java
// NoRestartBackoffTimeStrategy.java
@Override
public boolean canRestart() {
    return false;  // Never restart
}
```

When a task fails and `canRestart()` returns `false`, Flink correctly fails the entire job. This is the expected and documented behavior.

### 2. Test Design Incompatibility
The original test was designed to verify manual checkpoint triggering functionality, NOT failover recovery. From the original test `ManualCheckpointITCase.java`:
- Line 70-96: `testTriggeringWhenPeriodicDisabled()` does NOT call `env.enableCheckpointing()`
- The test's purpose is to verify that manual checkpoints can be triggered even when periodic checkpointing is disabled

### 3. Inappropriate Restart Position
Injecting a TaskManager restart at `during_keyed_state_processing` in a test without a restart strategy is not meaningful because:
- The test was never designed to handle component failures
- There's no mechanism (restart strategy) to recover from the failure
- The failure is a direct consequence of the test configuration, not a bug

## Evidence

### Original Test (ManualCheckpointITCase.java)
```java
@Test
public void testTriggeringWhenPeriodicDisabled() throws Exception {
    int parallelism = MINI_CLUSTER_RESOURCE.getNumberSlots();
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    // NOTE: No env.enableCheckpointing() call - no restart strategy configured
    storageConfigurer.accept(temporaryFolder.newFolder().toURI().toString(), env);
    // ... rest of test
}
```

### Contrast with testTriggeringWhenPeriodicEnabled
```java
@Test
public void testTriggeringWhenPeriodicEnabled() throws Exception {
    // ...
    env.enableCheckpointing(checkpointingInterval);  // Enables restart strategy
    // ...
}
```

## Conclusion
This failure is a **False Positive** because:
1. The test explicitly does NOT configure a restart strategy
2. Flink correctly fails the job when there's no restart strategy to handle task failures
3. The restart injection position is inappropriate for this test's design and purpose
4. The behavior observed is expected Flink behavior, not a bug

The 6 test executions in this group all share the same characteristic: they inject TaskManager restarts into tests that don't have restart strategies configured, leading to expected job failures.
