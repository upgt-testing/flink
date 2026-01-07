# FP-GROUP-11: FlinkJobTerminatedWithoutCancellationException

## Summary
**Classification:** FALSE POSITIVE
**Group ID:** 11
**Root Cause:** Test job is not configured with a restart strategy and checkpointing is disabled, so the job fails permanently when TaskManager restarts.

## Failure Details

**Test:** `org.apache.flink.test.execution.JobStatusChangedListenerITCase_RestartInjected.testJobStatusChangedForCancelledApplication`

**Restart Configuration:**
- Position: `during_job_running`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Index: `0`

**Error:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException: Flink job (20177d5060263fde929cd5aba0155f1c) was not canceled, but instead FAILED.
```

## Reproduction Results

Successfully reproduced the failure. Debug output confirms:
```
DEBUG: Job status BEFORE taskmanager restart: RUNNING
DEBUG: Job status AFTER taskmanager restart: FAILED
```

## Root Cause Analysis

### Test Code Analysis
The test method `testJobStatusChangedForCancelledApplication` (lines 186-250):
1. Creates a streaming job with `InfiniteLongSourceFunction` and `SleepingSink`
2. Submits the job to the cluster
3. Waits for all tasks to be running
4. Expects to cancel the job and verify status transitions

**Critical Finding:** The test does NOT:
- Enable checkpointing (`env.enableCheckpointing(...)`)
- Configure any restart strategy (`RestartStrategyUtils.configureNoRestartStrategy(env)` or similar)

### Flink's Default Behavior
From `RestartBackoffTimeStrategyFactoryLoader.java` (lines 94-121):
```java
private static RestartBackoffTimeStrategy.Factory getDefaultRestartStrategyFactory(
        final boolean isCheckpointingEnabled) {

    if (isCheckpointingEnabled) {
        // exponential delay restart strategy with default params
        return new ExponentialDelayRestartBackoffTimeStrategy...
    } else {
        return NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE;
    }
}
```

**Default restart strategy when checkpointing is NOT enabled = NO RESTART**

### Failure Sequence
1. Job is submitted and running with `RUNNING` status
2. TaskManager restart is injected at `during_job_running` position
3. TaskManager process restarts (GRACEFUL mode)
4. Tasks on the TaskManager fail due to TaskManager unavailability
5. **Since no restart strategy is configured and checkpointing is disabled**, Flink uses `NoRestartBackoffTimeStrategy`
6. Job immediately transitions to `FAILED` state without attempting recovery
7. Test calls `client.cancel(jobID).get()` on a FAILED job
8. `FlinkJobTerminatedWithoutCancellationException` is thrown because job is already in terminal FAILED state

## Why This is a False Positive

1. **Expected Flink Behavior:** Jobs without a restart strategy (and without checkpointing) fail permanently on any task failure. This is documented Flink behavior.

2. **Test Design Issue:** The test was designed to test job cancellation flow, not recovery from TaskManager failures. It assumes the job remains RUNNING, but this assumption only holds when no external failures occur.

3. **Not a Source Code Bug:** The Flink source code behaves exactly as designed. The `Dispatcher.cancelJob()` correctly throws `FlinkJobTerminatedWithoutCancellationException` when attempting to cancel a job that has already terminated.

4. **Comparison with Group 7:** Similar to Group 7 (FlinkJobNotFoundException), which was also classified as FP due to `noRestartStrategy` configuration. The only difference is:
   - Group 7: Test explicitly configured `noRestartStrategy`
   - Group 11: Test implicitly gets `noRestartStrategy` via default (no checkpointing)

## Relevant Code References

- Test file: `flink-tests/src/test/java/org/apache/flink/test/execution/JobStatusChangedListenerITCase_RestartInjected.java:186-250`
- Default restart strategy logic: `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/failover/RestartBackoffTimeStrategyFactoryLoader.java:94-121`
- Dispatcher cancel logic: `flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java:824`

## Conclusion

This is a FALSE POSITIVE. The test is not designed to handle TaskManager restarts and does not configure the necessary restart strategy for fault tolerance. The observed failure is expected Flink behavior given the test's configuration.
