# FP-GROUP-6: RuntimeException (Failover timeout)

## Classification: FALSE POSITIVE

## Summary
This failure is a false positive caused by the restart injection interfering with the test's intentional failover mechanism that has a strictly limited restart budget.

## Failure Details
- **Group ID:** 6
- **Test:** `org.apache.flink.runtime.operators.lifecycle.PartiallyFinishedSourcesITCase_RestartInjected.test`
- **Restart Position:** `during_partially_finished_execute`
- **Target:** `taskmanager`
- **Mode:** `GRACEFUL`

## Error Message
```
java.lang.RuntimeException: Unable to failover the job: No subtask restarted in 10000ms; job status: FAILED
Caused by: org.apache.flink.runtime.JobException: Recovery is suppressed by FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=1, backoffTimeMS=0)
```

## Root Cause Analysis

### Test Design
The test is designed to verify operator lifecycle behavior when sources finish partially. It explicitly:

1. **Configures a strict restart budget** (line 182):
   ```java
   RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
   ```
   This allows only **1 restart attempt** with 0ms backoff.

2. **Intentionally triggers failover** when `failover=true` (line 166-167):
   ```java
   if (failover) {
       executor.triggerFailover(
               subtaskScope == ALL_SUBTASKS ? iterator.next() : finishingOperatorID);
   }
   ```
   This intentional failover expects to consume the single restart attempt.

### How Restart Injection Causes Failure
1. The restart framework injects a taskmanager restart at `during_partially_finished_execute`
2. This causes task failure and triggers a restart attempt
3. The single allowed restart attempt is consumed
4. Later, the test calls `triggerFailover()` expecting the job to recover
5. But the restart budget is exhausted, so the job fails instead
6. The test fails with "Unable to failover the job"

### Why This Is Not a Bug
- **Flink is working as designed**: The restart strategy correctly limits restarts
- **Test logic depends on restart budget**: The test intentionally reserves the restart attempt for its own failover verification
- **Improper restart position**: Injecting a restart at this position interferes with test-specific logic that has no tolerance for additional failures

## Verdict
**FALSE POSITIVE** - The failure is caused by the restart injection consuming the test's limited restart budget before the test's own intentional failover logic can use it. This is not a bug in Flink source code; it's an incompatibility between the restart injection point and the test's strict restart budget configuration.

## Recommendation
Tests that configure strict restart budgets (`maxNumberRestartAttempts=1`) should be excluded from restart injection testing at positions that may trigger task failures, as these tests rely on their specific restart budget for their test logic.
