# FP-GROUP-1: IllegalStateException (HA leadership control required)

## Classification: FALSE POSITIVE

## Summary
This failure group contains 100 test executions that all fail with the same error: the restart testing framework's `FlinkClusterAdapter` cannot perform JobManager restarts because the test's MiniCluster is not configured with HA leadership control.

## Error Details

**Exception Type:** `java.lang.IllegalStateException`

**Error Message:**
```
JobManager restart requires HA leadership control. Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()
```

**Stack Trace:**
```
org.restarttest.core.RestartException: Restart failed at position before_avro_specific_execute
	at org.restarttest.core.RestartExecutor.execute(RestartExecutor.java:85)
	at org.restarttest.api.RestartPointBuilder.execute(RestartPointBuilder.java:172)
	at org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroSpecific(AvroStreamingFileSinkITCase_RestartInjected.java:96)
	...
Caused by: java.lang.IllegalStateException: JobManager restart requires HA leadership control. Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartJobManagerComponent(FlinkClusterAdapter.java:331)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:94)
	at org.apache.flink.test.restarttest.FlinkClusterAdapter.restartNode(FlinkClusterAdapter.java:53)
	at org.restarttest.core.RestartExecutor.restartSingleNode(RestartExecutor.java:186)
```

## Root Cause Analysis

### Source of the Error

The error originates from the restart testing framework's adapter code in `FlinkClusterAdapter.java`:

```java
// flink-restart-adapter/src/main/java/org/apache/flink/test/restarttest/FlinkClusterAdapter.java
// Lines 323-334

private void restartJobManagerComponent(MiniClusterWithClientResource cluster,
                                       int componentIndex, RestartMode mode) throws Exception {
    LOG.info("Restarting JobManager component with mode {}", mode);

    MiniCluster miniCluster = cluster.getMiniCluster();
    Optional<HaLeadershipControl> haControl = miniCluster.getHaLeadershipControl();

    if (!haControl.isPresent()) {
        throw new IllegalStateException(
            "JobManager restart requires HA leadership control. " +
            "Enable with MiniClusterResourceConfiguration.Builder.withHaLeadershipControl()");
    }
    // ... rest of the method
}
```

### Why This is a False Positive

1. **Error Origin**: The error is thrown by the restart testing framework (`FlinkClusterAdapter`), NOT by Flink source code.

2. **No Flink Code Exercised**: The failure occurs **before** any actual Flink restart logic is attempted. The adapter fails at its own precondition check.

3. **Infrastructure Limitation**: The tests in this group use `MiniClusterWithClientResource` configured **without** `withHaLeadershipControl()`. This is a valid configuration for normal tests, but the restart adapter requires this option to perform JobManager restarts.

4. **Design Constraint**: To simulate JobManager restart in MiniCluster, the adapter uses HA leadership revocation/granting:
   - `control.revokeDispatcherLeadership()`
   - `control.revokeResourceManagerLeadership()`
   - `control.grantDispatcherLeadership()`
   - `control.grantResourceManagerLeadership()`

   These APIs are only available when HA leadership control is enabled.

5. **Common Across All 100 Tests**: All tests in this group share the same characteristic - they target `jobmanager` restart but don't have HA leadership control enabled.

## Reproduction

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" mvn surefire:test \
  -Dtest=org.apache.flink.formats.avro.AvroStreamingFileSinkITCase_RestartInjected#testWriteAvroSpecific \
  -Drestart.position=before_avro_specific_execute \
  -Drestart.target=jobmanager \
  -Drestart.mode=GRACEFUL \
  -pl flink-formats/flink-avro
```

## Conclusion

This is NOT a bug in Flink source code. It is a **test infrastructure limitation** where:
- The restart testing framework requires HA leadership control to be enabled for JobManager restarts
- The original tests were not designed with this requirement in mind
- The injected restart tests inherit this configuration limitation

**Recommendation**: These tests should either:
1. Be excluded from JobManager restart injection testing, OR
2. Have their test infrastructure modified to enable HA leadership control (which may not be trivial and could affect test behavior)

## Affected Tests (100 total)

All tests target `jobmanager` restart and fail at the adapter's precondition check. Examples include:
- `AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroSpecific`
- `AvroStreamingFileSinkITCase_RestartInjected.testWriteAvroGeneric`
- `CEPITCase_RestartInjected.testSimplePatternCEP`
- And 97 more...
