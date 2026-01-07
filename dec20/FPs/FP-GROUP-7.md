# False Positive Analysis: Group 7 - FlinkJobNotFoundException

## Summary

**Verdict: FALSE POSITIVE**

The `FlinkJobNotFoundException` is expected behavior when a TaskManager is restarted while a job is configured with **no restart strategy**. This is not a bug in Flink's source code.

## Failure Details

**Test:** `org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected.test`

**Restart Configuration:**
- Position: `during_stop_with_savepoint_execute`
- Target: `taskmanager`
- Mode: `GRACEFUL`

**Error:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.messages.FlinkJobNotFoundException: Could not find Flink job (d5b138cda77b2cb55f24979502d5e282)
    at org.apache.flink.runtime.dispatcher.Dispatcher.getJobMasterGateway(Dispatcher.java:1529)
    at org.apache.flink.runtime.dispatcher.Dispatcher.stopWithSavepointAndGetLocation(Dispatcher.java:1090)
```

## Root Cause Analysis

### Job Configuration

The test job is explicitly configured with **no restart strategy** in `TestJobBuilders.java:289`:

```java
private static StreamExecutionEnvironment prepareEnv(...) throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(EXECUTION_FAILOVER_STRATEGY, "full");
    // ...
    RestartStrategyUtils.configureNoRestartStrategy(env);  // <-- No restart strategy
    // ...
}
```

`RestartStrategyUtils.configureNoRestartStrategy()` sets:
```java
env.configure(new Configuration().set(RestartStrategyOptions.RESTART_STRATEGY, "none"));
```

### Sequence of Events

1. **Job Starts:** The test submits a job and waits for watermark events
2. **TaskManager Restart:** The restart framework gracefully restarts TaskManager at position `during_stop_with_savepoint_execute`
3. **Tasks Fail:** Tasks running on the restarted TaskManager fail
4. **Job Fails Permanently:** Without a restart strategy, the job cannot recover and transitions to `FAILED` state
5. **JobManagerRunner Unregistered:** When the job reaches terminal state, the `JobManagerRunner` is unregistered from the Dispatcher's registry
6. **stopWithSavepoint Fails:** The test tries to call `stopWithSavepoint`, but `Dispatcher.getJobMasterGateway()` throws `FlinkJobNotFoundException` because the job is no longer registered

### Evidence from Debug Output

Added debug logging confirmed the job status after TaskManager restart:
```
DEBUG: Jobs after TaskManager restart: 1
DEBUG: Job 157507ce2e7e524e25d0261068743a6a - status: FAILED
```

### Dispatcher Code Analysis

From `Dispatcher.java:1527-1530`:
```java
private CompletableFuture<JobMasterGateway> getJobMasterGateway(JobID jobId) {
    if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
        return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
    }
    // ...
}
```

The `FlinkJobNotFoundException` is thrown because the job (in FAILED state) has been unregistered from the dispatcher.

## Why This Is a False Positive

1. **Intentional Test Configuration:** The test explicitly configures no restart strategy, which means any task failure will cause permanent job failure

2. **Expected Flink Behavior:** When a TaskManager is restarted, its tasks fail. Without a restart strategy, the job fails permanently and is cleaned up from the dispatcher

3. **Improper Restart Position:** The restart point `during_stop_with_savepoint_execute` is placed right before `stopWithSavepoint()` is called. Restarting a TaskManager at this point will cause the job to fail before the savepoint operation can be executed

4. **No Bug in Flink Runtime:** The `FlinkJobNotFoundException` correctly indicates that the job is no longer available for operations because it has failed and been cleaned up

## Reproduction Steps

1. Run the test with:
```bash
MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" mvn surefire:test \
  -Dtest=org.apache.flink.runtime.operators.lifecycle.StopWithSavepointITCase_RestartInjected \
  -Drestart.position=during_stop_with_savepoint_execute \
  -Drestart.target=taskmanager \
  -Drestart.mode=GRACEFUL \
  -pl flink-tests
```

2. The failure is consistently reproducible because the job always fails after TaskManager restart (no restart strategy)

## Conclusion

This failure is a **FALSE POSITIVE** caused by the restart framework injecting a TaskManager restart at a position that causes the job to fail, while the test is configured to not allow any job recovery. The test was designed to test `stopWithSavepoint` functionality, not TaskManager failure recovery.

To make this test compatible with TaskManager restarts, the job would need to be configured with a restart strategy that allows recovery from task failures.
