# [FLINK-XXXXX] Uninformative error message when job is suspended due to JobManager leadership loss

## Summary

When the JobManager loses leadership, running jobs transition to `SUSPENDED` state. However, `ApplicationStatus.fromJobStatus()` has no mapping for `SUSPENDED`, causing it to return `UNKNOWN`. This results in an uninformative error message: "Job completed with illegal application status: UNKNOWN" - providing no diagnostic value to users.

## Component/s

- Runtime / Coordination

## Affects Version/s

- 2.2.0 (and likely earlier versions)

## Priority

- Major

## Description

### Problem

When a JobManager loses leadership (e.g., in HA scenarios), running jobs are suspended. The job's `ExecutionGraph` transitions to `JobStatus.SUSPENDED` and is archived. When a client later queries the job result via `Dispatcher.requestJobResult()`, the system attempts to create a `JobResult` from the archived execution graph.

The issue is that `ApplicationStatus.fromJobStatus()` only maps **globally terminal** states:
- `FINISHED` → `SUCCEEDED`
- `FAILED` → `FAILED`
- `CANCELED` → `CANCELED`

But `SUSPENDED` is a **locally terminal** state (not globally terminal), so it has no mapping and defaults to `UNKNOWN`:

```java
// ApplicationStatus.java
public static ApplicationStatus fromJobStatus(JobStatus jobStatus) {
    return JOB_STATUS_APPLICATION_STATUS_BI_MAP.getOrDefault(jobStatus, UNKNOWN);
}
```

This causes `JobResult.toJobExecutionResult()` to throw:
```
org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
```

### Root Cause

1. `JobStatus.SUSPENDED` is defined as `TerminalState.LOCALLY` (locally terminal, not globally terminal)
2. `isTerminalState()` returns `true` for `SUSPENDED`
3. `JobResult.createFrom()` accepts any terminal state (passes the `isTerminalState()` check)
4. `ApplicationStatus.fromJobStatus(SUSPENDED)` returns `UNKNOWN` (no mapping)
5. `JobResult` did not preserve the original `JobStatus`, so error handling cannot provide specific guidance
6. User receives an uninformative "illegal application status: UNKNOWN" error

### Code References

**JobStatus.java:60**
```java
SUSPENDED(TerminalState.LOCALLY),  // Locally terminal but NOT globally terminal
```

**ApplicationStatus.java:46-53**
```java
static {
    // only globally-terminated JobStatus have a corresponding ApplicationStatus
    JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FAILED, ApplicationStatus.FAILED);
    JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.CANCELED, ApplicationStatus.CANCELED);
    JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FINISHED, ApplicationStatus.SUCCEEDED);
    // NOTE: No mapping for SUSPENDED!
}
```

**JobResult.java:147-155** (where uninformative error is thrown)
```java
} else {
    exception =
            new JobExecutionException(
                    jobId,
                    "Job completed with illegal application status: "
                            + applicationStatus
                            + '.',
                    cause);
}
```

## Steps to Reproduce

1. Start a Flink cluster with HA enabled (but using `StandaloneExecutionPlanStore` which doesn't persist jobs)
2. Submit a long-running job
3. Trigger JobManager leadership loss (e.g., revoke leadership)
4. Wait for the new Dispatcher to start
5. Query the job result from the client

**Expected:** Clear error message explaining the job was suspended and recovery failed

**Actual:** `JobExecutionException: Job completed with illegal application status: UNKNOWN.`

## Implemented Fix

The fix preserves the original `JobStatus` in `JobResult` and uses it to provide informative error messages when `ApplicationStatus` is `UNKNOWN`.

### Changes to JobResult.java

1. **Add `jobStatus` field** to preserve the original state:
```java
@Nullable private final JobStatus jobStatus;
```

2. **Update `toJobExecutionResult()`** to handle `UNKNOWN` with the actual `JobStatus`:
```java
} else if (applicationStatus == ApplicationStatus.UNKNOWN) {
    if (jobStatus == JobStatus.SUSPENDED) {
        exception = new JobExecutionException(
                jobId,
                "Job is in state SUSPENDED. This commonly happens when the "
                        + "JobManager lost leadership. The job may recover "
                        + "automatically if High Availability and a persistent "
                        + "job store are configured. If recovery is not possible "
                        + "(e.g., non-persistent ExecutionPlanStore), the job "
                        + "needs to be resubmitted.",
                cause);
    } else {
        exception = new JobExecutionException(
                jobId,
                "Job reached a terminal state without a corresponding "
                        + "ApplicationStatus. JobStatus=" + jobStatus
                        + ", ApplicationStatus=" + applicationStatus + ".",
                cause);
    }
}
```

3. **Update Builder and `createFrom()`** to set the `jobStatus`.

### Test Results

**Before:**
```
org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
    at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
```

**After:**
```
org.apache.flink.runtime.client.JobExecutionException: Job is in state SUSPENDED. This commonly happens when the JobManager lost leadership. The job may recover automatically if High Availability and a persistent job store are configured. If recovery is not possible (e.g., non-persistent ExecutionPlanStore), the job needs to be resubmitted.
    at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:161)
```

## Why This Fix is Correct

1. **Minimal behavior change**: Only changes the exception path, no functional changes
2. **Preserves truth**: Reports actual `JobStatus=SUSPENDED` instead of hiding it behind `UNKNOWN`
3. **Provides operator guidance**: Explains leadership loss, HA, job store, and resubmission
4. **Handles other UNKNOWN cases**: Generic fallback message still includes the actual `JobStatus` for debugging
5. **No API changes**: The fix is internal to error handling

## Impact

- Users now receive actionable error messages when jobs are suspended
- Debugging HA-related job failures becomes straightforward
- Clear guidance is provided on how to configure proper HA settings for job recovery

## Attachments

- Patch file: `BUG-GROUP-4.patch`
