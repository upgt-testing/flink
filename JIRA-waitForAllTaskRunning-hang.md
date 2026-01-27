# JIRA Bug Report: waitForAllTaskRunning May Hang Indefinitely

| Field | Value |
|-------|-------|
| **Summary** | `CommonTestUtils.waitForAllTaskRunning()` may hang indefinitely when job is lost or in unexpected state |
| **Component/s** | flink-runtime |
| **Affects Version/s** | 2.2.0 |
| **Priority** | Minor |
| **Labels** | test-utilities, hang, robustness |

---

## Description

### Problem Statement

`CommonTestUtils.waitForAllTaskRunning()` does not have a timeout mechanism and may hang indefinitely if the job being waited on is lost, cancelled, or enters an unexpected terminal state.

### Current Implementation

Location: `flink-runtime/src/test/java/org/apache/flink/runtime/testutils/CommonTestUtils.java`

```java
public static void waitForAllTaskRunning(
        MiniCluster miniCluster, JobID jobId, boolean allowFinished) throws Exception {
    waitForAllTaskRunning(() -> getGraph(miniCluster, jobId), allowFinished);
}
```

This method calls `waitUntilCondition()` which polls indefinitely with no upper bound on wait time:

```java
public static void waitUntilCondition(
        SupplierWithException<Boolean, Exception> condition) throws Exception {
    waitUntilCondition(condition, Duration.ofMillis(1));
}
```

### Potential Issue

If the job identified by `jobId` is:
- Lost due to cluster failure or restart
- Cancelled unexpectedly
- Failed and entered a terminal state
- Never properly scheduled

The method will continue polling forever, causing the test to hang indefinitely rather than failing with a clear error message.

### Scenario Example

```java
JobClient jobClient = env.executeAsync();

// If something causes the job to be lost here (e.g., cluster issue)

// This will hang forever because the job no longer exists
CommonTestUtils.waitForAllTaskRunning(
    miniCluster, jobClient.getJobID(), false);
```

---

## Proposed Fix

Add a timeout parameter to `waitForAllTaskRunning()` to ensure tests fail fast with a clear error message rather than hanging indefinitely.

### Option 1: Add overloaded method with timeout (Recommended)

```java
/**
 * Waits for all tasks to be in RUNNING state with a specified timeout.
 *
 * @param miniCluster the mini cluster
 * @param jobId the job ID to wait for
 * @param allowFinished whether to allow FINISHED state
 * @param timeout maximum time to wait
 * @throws TimeoutException if tasks don't reach RUNNING state within timeout
 */
public static void waitForAllTaskRunning(
        MiniCluster miniCluster,
        JobID jobId,
        boolean allowFinished,
        Duration timeout) throws Exception {
    waitUntilCondition(
            () -> {
                AccessExecutionGraph graph = getGraph(miniCluster, jobId);
                return graph.getAllVertices().values().stream()
                        .allMatch(jv ->
                            Arrays.stream(jv.getTaskVertices())
                                .allMatch(task -> checkTaskState(task, allowFinished)));
            },
            timeout);
}

private static boolean checkTaskState(AccessExecutionVertex task, boolean allowFinished) {
    switch (task.getExecutionState()) {
        case RUNNING:
            return true;
        case FINISHED:
            if (allowFinished) {
                return true;
            } else {
                throw new RuntimeException("Sub-Task finished unexpectedly: " + task);
            }
        default:
            return false;
    }
}
```

### Option 2: Add default timeout to existing method

Add a sensible default timeout (e.g., 5 minutes) to prevent indefinite hangs:

```java
private static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofMinutes(5);

public static void waitForAllTaskRunning(
        MiniCluster miniCluster, JobID jobId, boolean allowFinished) throws Exception {
    waitForAllTaskRunning(miniCluster, jobId, allowFinished, DEFAULT_WAIT_TIMEOUT);
}
```

---

## Additional Consideration

Consider adding a job existence check before entering the wait loop:

```java
public static void waitForAllTaskRunning(
        MiniCluster miniCluster, JobID jobId, boolean allowFinished) throws Exception {
    // Verify job exists before waiting
    try {
        miniCluster.getJobStatus(jobId).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
        throw new IllegalStateException("Job " + jobId + " does not exist or is not accessible", e);
    }

    waitForAllTaskRunning(() -> getGraph(miniCluster, jobId), allowFinished);
}
```

---

## Acceptance Criteria

- [ ] `waitForAllTaskRunning()` has a timeout mechanism (either default or explicit parameter)
- [ ] Tests fail fast with clear error message when job is lost or unavailable
- [ ] Backward compatibility maintained for existing callers

---

## Environment

- Flink Version: 2.2.0
- Location: `flink-runtime/src/test/java/org/apache/flink/runtime/testutils/CommonTestUtils.java`
