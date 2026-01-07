# Bug Report - Group 4: JobExecutionException (UNKNOWN status)

## Summary

**Bug Type:** Missing ApplicationStatus Mapping for SUSPENDED JobStatus
**Severity:** Medium
**Component:** `flink-runtime` - Job Result Status Handling

When the JobManager loses leadership and jobs transition to SUSPENDED state, the system returns an uninformative `UNKNOWN` application status because `ApplicationStatus.fromJobStatus()` has no mapping for the `SUSPENDED` JobStatus.

## Root Cause (Confirmed via Debug Tracing)

### Debug Output
```
[DEBUG-DISPATCHER] requestJobResult called for jobId=..., isRegistered=false
[DEBUG-DISPATCHER] Job not registered, executionGraphInfo=present
[DEBUG-DISPATCHER] Returning JobResult from executionGraphInfoStore, state=SUSPENDED
[DEBUG-JOBRESULT] createFrom called, jobId=..., jobStatus=SUSPENDED, isTerminalState=true, isGloballyTerminalState=false
[DEBUG-JOBRESULT] ApplicationStatus.fromJobStatus(SUSPENDED) = UNKNOWN
```

### The Bug

1. **JobStatus.SUSPENDED** is defined as `TerminalState.LOCALLY` (locally terminal, not globally terminal)
2. **`isTerminalState()`** returns `true` for SUSPENDED (because LOCALLY is a terminal state)
3. **`ApplicationStatus.fromJobStatus()`** only maps globally terminal states:
   - FINISHED → SUCCEEDED
   - FAILED → FAILED
   - CANCELED → CANCELED
   - **SUSPENDED → UNKNOWN** (no mapping!)
4. **`JobResult`** did not preserve the original `JobStatus`, so error handling could not provide specific guidance

### Code Evidence

**JobStatus.java:60**
```java
SUSPENDED(TerminalState.LOCALLY),  // Locally terminal but NOT globally terminal
```

**JobStatus.java:103-105**
```java
public boolean isTerminalState() {
    return terminalState != TerminalState.NON_TERMINAL;  // Returns TRUE for SUSPENDED
}
```

**ApplicationStatus.java:46-51**
```java
static {
    // only globally-terminated JobStatus have a corresponding ApplicationStatus
    JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FAILED, ApplicationStatus.FAILED);
    JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.CANCELED, ApplicationStatus.CANCELED);
    JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FINISHED, ApplicationStatus.SUCCEEDED);
    // NOTE: No mapping for SUSPENDED!
}
```

**ApplicationStatus.java:74-76**
```java
public static ApplicationStatus fromJobStatus(JobStatus jobStatus) {
    return JOB_STATUS_APPLICATION_STATUS_BI_MAP.getOrDefault(jobStatus, UNKNOWN);  // SUSPENDED → UNKNOWN
}
```

### Flow When JobManager Loses Leadership

1. JobManager leadership is revoked
2. `revokeLeadership()` is called on JobMasterServiceLeadershipRunner
3. JobMasterServiceProcess is stopped
4. `scheduler.closeAsync()` is called (SchedulerBase.java:711)
5. `executionGraph.suspend(cause)` is called (SchedulerBase.java:729)
6. **Running jobs transition to SUSPENDED state** (locally terminal)
7. Job's ExecutionGraphInfo is stored in `executionGraphInfoStore` with SUSPENDED state
8. New Dispatcher starts but doesn't recover the job (StandaloneExecutionPlanStore doesn't persist jobs)
9. Client calls `requestJobResult(jobId)`
10. Dispatcher finds ExecutionGraphInfo in store (job not registered)
11. `JobResult.createFrom()` is called with SUSPENDED state
12. `ApplicationStatus.fromJobStatus(SUSPENDED)` returns **UNKNOWN**
13. User gets uninformative error: "Job completed with illegal application status: UNKNOWN"

## Error Details

**Original Error:**
```
org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
    at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
```

## Buggy Code Location

**Primary Bug Location:**
`flink-runtime/src/main/java/org/apache/flink/runtime/clusterframework/ApplicationStatus.java:46-51`
- Missing mapping for `JobStatus.SUSPENDED`

**Secondary Issue:**
`flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobResult.java:147-155`
- UNKNOWN status handling provides no diagnostic information
- Original `JobStatus` was not preserved in `JobResult`

## Reproduction

```bash
MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" mvn surefire:test \
  -Dtest=org.apache.flink.test.checkpointing.RegionFailoverITCase_RestartInjected#testMultiRegionFailover \
  -Drestart.position=after_job_submit \
  -Drestart.target=jobmanager \
  -Drestart.mode=GRACEFUL \
  -pl flink-tests
```

---

## Implemented Patch

The fix preserves the original `JobStatus` in `JobResult` and uses it to provide informative error messages when `ApplicationStatus` is `UNKNOWN`.

### Patch Location
`flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobResult.java`

### Changes

1. **Add `jobStatus` field** to preserve the original state:
```java
/**
 * Stores the original JobStatus from the ExecutionGraph. This is needed because
 * ApplicationStatus.fromJobStatus() may return UNKNOWN for locally terminal states
 * like SUSPENDED, losing the original state information.
 */
@Nullable private final JobStatus jobStatus;
```

2. **Update `toJobExecutionResult()`** to handle `UNKNOWN` with the actual `JobStatus`:
```java
} else if (applicationStatus == ApplicationStatus.UNKNOWN) {
    // UNKNOWN status occurs when the job is in a locally terminal state (like SUSPENDED)
    // that has no mapping in ApplicationStatus. Provide informative error based on
    // the actual JobStatus if available.
    if (jobStatus == JobStatus.SUSPENDED) {
        exception =
                new JobExecutionException(
                        jobId,
                        "Job is in state SUSPENDED. This commonly happens when the "
                                + "JobManager lost leadership. The job may recover "
                                + "automatically if High Availability and a persistent "
                                + "job store are configured. If recovery is not possible "
                                + "(e.g., non-persistent ExecutionPlanStore), the job "
                                + "needs to be resubmitted.",
                        cause);
    } else {
        exception =
                new JobExecutionException(
                        jobId,
                        "Job reached a terminal state without a corresponding "
                                + "ApplicationStatus. JobStatus="
                                + jobStatus
                                + ", ApplicationStatus="
                                + applicationStatus
                                + ".",
                        cause);
    }
}
```

3. **Update Builder** to support `jobStatus` field.

4. **Update `createFrom()`** to set the `jobStatus`.

### Test Results Comparison

**Before Patch:**
```
org.apache.flink.runtime.client.JobExecutionException: Job completed with illegal application status: UNKNOWN.
    at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:148)
```

**After Patch:**
```
org.apache.flink.runtime.client.JobExecutionException: Job is in state SUSPENDED. This commonly happens when the JobManager lost leadership. The job may recover automatically if High Availability and a persistent job store are configured. If recovery is not possible (e.g., non-persistent ExecutionPlanStore), the job needs to be resubmitted.
    at org.apache.flink.runtime.jobmaster.JobResult.toJobExecutionResult(JobResult.java:161)
```

### Improvement Summary

| Aspect | Before | After |
|--------|--------|-------|
| Problem Identification | "illegal application status" | "Job is in state SUSPENDED" |
| Root Cause | None | "JobManager lost leadership" |
| Technical Explanation | None | "may recover automatically if HA configured" |
| Recovery Guidance | None | "persistent job store" / "resubmit" |
| Action Required | None | "job needs to be resubmitted" |
| Diagnostic Value | Zero | High |
| Other UNKNOWN cases | Same bad message | Shows actual JobStatus |

### Why This Fix is Correct

1. **Minimal behavior change**: Only changes the exception path, no functional changes
2. **Preserves truth**: Reports actual `JobStatus=SUSPENDED` instead of hiding it behind `UNKNOWN`
3. **Provides operator guidance**: Explains leadership loss, HA, job store, and resubmission
4. **Handles other UNKNOWN cases**: Generic fallback message still includes the actual `JobStatus` for debugging
5. **No API changes**: The fix is internal to error handling

---

## Patch File

The patch is available at: `dec20/bugs/BUG-GROUP-4.patch`

## JIRA Report

A JIRA-format report is available at: `dec20/bugs/BUG-GROUP-4-JIRA.md`
