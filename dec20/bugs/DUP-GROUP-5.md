# Analysis - Group 5: JobExecutionException (UNKNOWN status - variant)

## Status: DUPLICATE of Group 4

## Summary

Group 5 is a **duplicate** of Group 4. Both failures have the same root cause:

1. **JobManager restart** at `after_job_submit` position
2. Job transitions to **SUSPENDED** state (locally terminal, not globally terminal)
3. `ApplicationStatus.fromJobStatus(SUSPENDED)` returns **UNKNOWN** (no mapping)
4. Error message: "Job completed with illegal application status: UNKNOWN"

## Difference from Group 4

The only difference is how the exception is surfaced:

| Aspect | Group 4 | Group 5 |
|--------|---------|---------|
| Exception wrapper | Thrown directly | Wrapped in `ExecutionException` |
| Test class | `RegionFailoverITCase_RestartInjected` | `CacheITCase_RestartInjected` |
| Line in stacktrace | `JobResult.java:148` | Same (`JobResult.java:148`) |
| Root cause | Same | Same |

## Reproduction

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" mvn surefire:test \
  -Dtest=org.apache.flink.test.streaming.runtime.CacheITCase_RestartInjected#testCacheProduceAndConsumeWithDifferentPartitioner \
  -Drestart.position=after_job_submit \
  -Drestart.target=jobmanager \
  -Drestart.mode=GRACEFUL \
  -pl flink-tests
```

## Debug Output

```
DEBUG: Job submitted with ID: c21985126c414b0965861c2ab6f74f89
DEBUG: Job status BEFORE jobmanager restart: INITIALIZING
DEBUG: JobManager restarted
DEBUG: Job status AFTER jobmanager restart: SUSPENDED
DEBUG: TaskManager restarted
DEBUG: Job status AFTER taskmanager restart: SUSPENDED
DEBUG: About to call getJobExecutionResult()
```

The job enters SUSPENDED state after JobManager restart and never recovers.

## Fix

The fix for Group 4 (BUG-GROUP-4.patch) also resolves Group 5:

- Preserves original `JobStatus` in `JobResult`
- Provides informative error message for SUSPENDED state
- Explains that this happens when JobManager loses leadership and HA is not configured

## References

- **Primary bug report:** `dec20/bugs/BUG-GROUP-4.md`
- **Patch:** `dec20/bugs/BUG-GROUP-4.patch`
- **JIRA report:** `dec20/bugs/BUG-GROUP-4-JIRA.md`
