# FP-GROUP-10: TaskNotRunningException

## Summary
**Classification:** FALSE POSITIVE
**Root Cause:** Race condition between fast-finishing bounded source and coordinator event delivery during restart injection

## Original Failure

**Test:** `org.apache.flink.test.streaming.runtime.SideOutputITCase_RestartInjected.testWatermarkForwarding`

**Restart Parameters:**
- Position: `during_job_running`
- Target: `taskmanager`
- Mode: `GRACEFUL`
- Index: `0`

**Stacktrace:**
```
java.util.concurrent.ExecutionException: org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
Caused by: org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy
Caused by: org.apache.flink.util.FlinkException: An OperatorEvent from an OperatorCoordinator to a task was lost.
  Triggering task failover to ensure consistency. Event: '[NoMoreSplitEvent]', targetTask: Source: timestamped-source (1/1)
Caused by: org.apache.flink.runtime.operators.coordination.TaskNotRunningException: Task is not running, but in state FINISHED
  at org.apache.flink.runtime.taskmanager.Task.deliverOperatorEvent(Task.java:1542)
```

## Analysis

### Why This Is a False Positive

1. **Bounded Source Finishes Almost Instantly:**
   The test uses a custom bounded source that emits only 5 elements and immediately returns `END_OF_INPUT`:
   ```java
   public InputStatus pollNext(ReaderOutput<Integer> out) {
       if (!emitted) {
           out.collect(1, 0);
           out.emitWatermark(new Watermark(0));
           out.collect(2, 1);
           out.collect(5, 2);
           out.emitWatermark(new Watermark(2));
           out.collect(3, 3);
           out.collect(4, 4);
           emitted = true;
           return InputStatus.END_OF_INPUT;  // Finishes immediately
       }
       return InputStatus.END_OF_INPUT;
   }
   ```
   This source completes in milliseconds.

2. **Improper Restart Position:**
   The restart is injected at `during_job_running` position, but this position is not appropriate for a job that finishes almost immediately. By the time the restart is triggered (after 1 second sleep), the source task is already in FINISHED state.

3. **Expected Flink Behavior:**
   The `TaskNotRunningException` is Flink's expected response when the OperatorCoordinator tries to send an event (NoMoreSplitEvent) to a task that has already finished. Flink correctly triggers a failover to maintain coordinator-task consistency.

4. **No Restart Strategy Configured:**
   The test doesn't configure a restart strategy for the job. When Flink triggers failover for consistency, the job fails because `NoRestartBackoffTimeStrategy` suppresses recovery.

5. **Race Condition Timing:**
   This failure requires a very specific timing alignment where:
   - The source task has just transitioned to FINISHED state
   - The coordinator is simultaneously trying to send NoMoreSplitEvent
   - The TaskManager restart happens at exactly this moment

### Reproduction Attempts

- **15+ runs performed:** All passed successfully
- **Conclusion:** The race condition is extremely rare and timing-dependent

### Why This Is Not a Bug in Flink

1. The `TaskNotRunningException` is working as designed - it correctly reports that the task is not running
2. The coordinator event delivery mechanism correctly handles this by triggering failover
3. The job only fails because no restart strategy is configured
4. The restart injection position is inappropriate for this type of short-lived bounded job

## Conclusion

This failure is a **FALSE POSITIVE** caused by:
1. Injecting a restart at `during_job_running` on a job that finishes almost instantly
2. The test not configuring a restart strategy to handle expected failover scenarios
3. An extremely rare timing condition that cannot be reliably reproduced

The restart framework's `during_job_running` position is not suitable for jobs with bounded sources that complete in milliseconds. The failure does not indicate any bug in Flink's source code.
