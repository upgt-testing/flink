# Prompt: Transform Flink MiniCluster Test with Restart Position Injection

## Objective

Transform an existing Flink mini cluster test to inject restart positions for distributed system restart testing. The transformation will generate:
1. A new test file with `_RestartInjected` suffix
2. A restart configuration file for the Maven plugin

## Input

- **Test File Path**: Path to the original test file (e.g., `/path/to/TestFlinkJob.java`)
- **Test Class**: Fully-qualified class name (e.g., `org.apache.flink.test.streaming.TestFlinkJob`)

## Output

1. **Generated Test File**: `{OriginalFileName}_RestartInjected.java` at the same directory as the input file
2. **Restart Configuration**: `restart-config.json` in the `restarts-config/` directory under the same module directory as the test file, if not exist create it.

## Transformation Instructions

### Step 1: Analyze the Original Test

Read the input test file and identify:

1. **Cluster Setup**: Find the Flink cluster instance variable (e.g., `MiniClusterWithClientResource flinkCluster` or `MiniClusterResource miniCluster`)
   - **CRITICAL**: The cluster object may be inherited from a parent base test class rather than being directly initialized in the test class itself
   - **IMPORTANT**: Carefully check the class hierarchy (look for `extends` clauses) and examine parent classes for cluster initialization
   - Common patterns:
     - Direct initialization: `public final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(...)` in the test class
     - JUnit Rule/ClassRule: `@ClassRule public static MiniClusterWithClientResource cluster = ...`
     - Inherited from parent: `extends FlinkTestBase` where parent has the cluster field
     - Field with different name: `flinkCluster`, `miniCluster`, `cluster`, `testCluster`, etc.
   - **Strategy**: If the cluster is not found in the test class, traverse up the inheritance hierarchy to locate it
   - Look for JUnit `@Rule`, `@ClassRule`, `@Before`, or `@BeforeClass` annotations that may initialize the cluster
2. **Test Methods**: Identify all `@Test` annotated methods
3. **Critical Operations**: Look for operations that involve state transitions, such as:
   - Job submission: `env.execute()`, `client.submitJob()`, job graph creation
   - Job lifecycle: Job start, job running, job completion, job cancellation
   - Checkpointing: Checkpoint triggers, checkpoint completion, savepoint creation
   - State operations: State updates, state queries, state restoration
   - Streaming operations: Source consumption, windowing, aggregations, sinks
   - Task deployment: Task submission, task execution, task completion
   - Failover: Job recovery, checkpoint recovery, task failover
   - Slot allocation: Slot requests, slot allocation, slot release
   - Resource management: Resource allocation, resource release
   - Backpressure scenarios: Slow operators, blocking operations

### Step 2: Identify Restart Points

For each test method, identify potential restart points based on these criteria:

**Good Restart Points** (inject here):
- After job submission but before completion
- During job execution (while job is running)
- After checkpoint completion
- Before savepoint creation
- After state updates (for stateful operations)
- During window processing
- After task deployment but before task completion
- Before job cancellation
- During backpressure scenarios
- After slot allocation
- During recovery from failures
- After source data ingestion
- During sink operations

**Poor Restart Points** (avoid):
- Before cluster setup (no cluster exists yet)
- After cluster teardown (cluster already destroyed)
- During trivial operations (simple utility methods with no state changes)
- Operations that are too fast to test meaningful state
- Before job submission (no job running yet)

**Naming Convention for Restart Positions**:
- Use descriptive, lowercase names with underscores
- Pattern: `{operation}_{context}`
- Examples:
  - `after_job_submit`
  - `during_job_running`
  - `after_checkpoint`
  - `before_savepoint`
  - `after_state_update`
  - `during_window_processing`
  - `before_job_cancel`
  - `after_task_deploy`
  - `during_backpressure`
  - `after_slot_allocate`
  - `during_source_ingestion`
  - `before_sink_write`
  - `after_recovery`

### Step 3: Generate the Restart-Injected Test File

Create a new test file with the following transformations:

#### 3.1 Package and Imports

```java
// Keep original package declaration
package org.apache.flink.test.streaming;

// Add these imports at the top (if not already present)
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

// Keep all original imports
```

#### 3.2 Class Declaration

```java
// Original class name: TestFlinkJob
// New class name: TestFlinkJob_RestartInjected
public class TestFlinkJob_RestartInjected {
    // Keep all original fields and variables
}
```

#### 3.3 Cluster Setup and Teardown

Keep the `@Before`, `@After`, `@BeforeClass`, `@AfterClass` methods unchanged:

```java
@Before
public void setUp() throws Exception {
    // Keep original setup code unchanged
}

@After
public void tearDown() throws Exception {
    // Keep original teardown code unchanged
}
```

**IMPORTANT**: If the cluster is declared as a JUnit `@Rule` or `@ClassRule`, keep it unchanged:

```java
// Keep as-is
@ClassRule
public static final MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(...);
```

#### 3.4 Transform Test Methods

For each `@Test` method, apply the following transformations:

**Original Test Method**:
```java
@Test
public void testStreamingJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);

    DataStream<String> input = env.fromElements("a", "b", "c");
    DataStream<String> result = input.map(x -> x.toUpperCase());
    result.addSink(new CollectingSink());

    JobClient jobClient = env.executeAsync("Test Job");
    jobClient.getJobExecutionResult().get();
}
```

**Transformed Test Method**:
```java
@Test
public void testStreamingJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);

    DataStream<String> input = env.fromElements("a", "b", "c");
    DataStream<String> result = input.map(x -> x.toUpperCase());
    result.addSink(new CollectingSink());

    JobClient jobClient = env.executeAsync("Test Job");

    // RESTART POINT 1: after_job_submit
    RestartFramework.at("after_job_submit")
        .on(flinkCluster)  // Use the cluster instance from the test class or parent
        .restart("jobmanager")
        .withIndex(0)
        .withMode(RestartMode.GRACEFUL)
        .execute();

    // Wait for job to start running
    Thread.sleep(2000);

    // RESTART POINT 2: during_job_running
    RestartFramework.at("during_job_running")
        .on(flinkCluster)
        .restart("taskmanager")
        .withIndex(0)
        .withMode(RestartMode.GRACEFUL)
        .execute();

    jobClient.getJobExecutionResult().get();
}
```

**Injection Pattern**:

1. **After Job Submission**:
   ```java
   JobClient jobClient = env.executeAsync("Job");

   // Inject restart point
   RestartFramework.at("after_job_submit")
       .on(flinkCluster)
       .restart("jobmanager")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();
   ```

2. **During Job Execution**:
   ```java
   // Wait for job to be running
   waitUntilJobIsRunning(jobClient);

   // Inject restart point during execution
   RestartFramework.at("during_job_running")
       .on(flinkCluster)
       .restart("taskmanager")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();
   ```

3. **After Checkpoint**:
   ```java
   // Trigger checkpoint
   CompletableFuture<CompletedCheckpoint> checkpoint =
       triggerCheckpoint(jobClient);
   checkpoint.get();

   // Inject restart point
   RestartFramework.at("after_checkpoint")
       .on(flinkCluster)
       .restart("taskmanager")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();
   ```

4. **Before Job Cancellation**:
   ```java
   // Inject restart point before cancel
   RestartFramework.at("before_job_cancel")
       .on(flinkCluster)
       .restart("jobmanager")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();

   jobClient.cancel().get();
   ```

#### 3.5 Flink-Specific Node Roles

Flink has two primary node types:

- **`jobmanager`** (or `"master"`): The master node that manages job lifecycle, scheduling, and coordination (includes Dispatcher and ResourceManager)
- **`taskmanager`** (or `"worker"`): Worker nodes that execute tasks and process data

**Default Restart Configuration**:
Use these defaults for all injected restart points:
- **For job/scheduling operations**: `"jobmanager"` (manages job lifecycle, checkpoint coordination)
- **For task/data operations**: `"taskmanager"` (executes tasks, processes data)
- **For cluster-wide operations**: Both `"jobmanager"` and `"taskmanager"`
- **Node Index**: `0` (first node)
- **Restart Mode**: `RestartMode.GRACEFUL` (default, safest)

#### 3.6 Node Role Selection Guidelines

| Operation Type | Primary Node Role | Secondary Node Role | Reason |
|----------------|-------------------|---------------------|--------|
| Job submission | `jobmanager` | - | JM manages job lifecycle |
| Job scheduling | `jobmanager` | - | JM schedules tasks |
| Job cancellation | `jobmanager` | - | JM controls job state |
| Task execution | `taskmanager` | - | TM executes tasks |
| Checkpointing | `taskmanager` | `jobmanager` | TM snapshots state, JM coordinates |
| Savepoints | `jobmanager` | `taskmanager` | JM triggers, TM snapshots |
| State updates | `taskmanager` | - | TM manages state |
| Failover/Recovery | `taskmanager` | `jobmanager` | Tests recovery mechanisms |
| Slot allocation | `jobmanager` | `taskmanager` | JM allocates, TM provides |
| Data processing | `taskmanager` | - | TM processes data |
| Source ingestion | `taskmanager` | - | TM reads from sources |
| Sink operations | `taskmanager` | - | TM writes to sinks |
| Backpressure | `taskmanager` | - | TM handles backpressure |

### Step 4: Generate Restart Configuration File

Create `restarts-config/restart-config.json` with the following structure:

```json
{
  "tests": [
    {
      "testClass": "org.apache.flink.test.streaming.TestFlinkJob_RestartInjected",
      "testMethod": "testStreamingJob",
      "restartPoints": [
        {
          "position": "after_job_submit",
          "targets": ["jobmanager"],
          "modes": ["GRACEFUL", "CRASH", "DELAYED_CRASH"]
        },
        {
          "position": "during_job_running",
          "targets": ["taskmanager"],
          "modes": ["GRACEFUL", "CRASH"]
        }
      ]
    }
  ]
}
```

#### Configuration Generation Rules

For each test method in the transformed test:

1. **Create a test specification** with:
   - `testClass`: The fully-qualified name of the generated test class
   - `testMethod`: The test method name (same as original)
   - `restartPoints`: Array of restart point configurations

2. **For each restart point** injected in the test method:
   - `position`: The position identifier used in `.at("...")`
   - `targets`: Array of node roles to test
   - `modes`: Array of restart modes to test

#### Target Selection for Flink Operations

**JobManager-only operations** (`["jobmanager"]`):
- Job submission, cancellation, finish
- Job scheduling and planning
- Checkpoint coordination
- Savepoint triggering
- Slot request management
- Job state tracking
- Resource management decisions

**TaskManager-only operations** (`["taskmanager"]`):
- Task execution
- Data processing (map, filter, reduce, etc.)
- State backend operations
- Source data reading
- Sink data writing
- Local checkpoint snapshots
- Backpressure handling

**Both JobManager and TaskManager** (`["jobmanager", "taskmanager"]`):
- Job recovery and failover
- Checkpoint completion (JM coordinates, TM snapshots)
- Savepoint creation (JM triggers, TM snapshots)
- Slot allocation (JM requests, TM provides)
- End-to-end job execution
- Cluster-wide consistency operations

#### Mode Selection Guidelines

- **`["GRACEFUL"]`**: Basic test, verify restart works
  - Use for: Initial testing, simple state transitions

- **`["GRACEFUL", "CRASH"]`**: Standard test, verify crash recovery
  - Use for: Job lifecycle, task execution, checkpoint operations

- **`["GRACEFUL", "CRASH", "DELAYED_CRASH"]`**: Advanced test, verify timing-sensitive operations
  - Use for: Checkpoint coordination, state persistence, failover recovery, distributed coordination

### Step 5: File Placement

1. **Generated Test File**:
   - Location: Same directory as original test file
   - Name: `{OriginalClassName}_RestartInjected.java`
   - Example: `TestFlinkJob.java` → `TestFlinkJob_RestartInjected.java`

2. **Restart Configuration**:
   - Location: `restarts-config/` directory under the same module directory as the test file
   - Name: `restart-config.json`
   - If file exists, append to the `tests` array (avoid duplicates)
   - If file doesn't exist, create new file

## Example Transformation

### Input: `TestCheckpointing.java`

```java
package org.apache.flink.streaming.tests;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCheckpointing {

    @ClassRule
    public static final MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());

    @Test
    public void testCheckpointingJob() throws Exception {
        StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();
        env.enableCheckpointing(500);

        DataStream<Long> input = env.fromSequence(1, 100);
        DataStream<Long> mapped = input.map(x -> x * 2);
        mapped.addSink(new CollectSink());

        env.execute("Checkpointing Test");
    }

    private static class CollectSink implements SinkFunction<Long> {
        @Override
        public void invoke(Long value, Context context) {
            // Collect results
        }
    }
}
```

### Output 1: `TestCheckpointing_RestartInjected.java`

```java
package org.apache.flink.streaming.tests;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

import static org.junit.Assert.*;

public class TestCheckpointing_RestartInjected {

    @ClassRule
    public static final MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());

    @Test
    public void testCheckpointingJob() throws Exception {
        StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();
        env.enableCheckpointing(500);

        DataStream<Long> input = env.fromSequence(1, 100);
        DataStream<Long> mapped = input.map(x -> x * 2);
        mapped.addSink(new CollectSink());

        // Submit job asynchronously to enable restart injection
        org.apache.flink.core.execution.JobClient jobClient =
            env.executeAsync("Checkpointing Test");

        // RESTART POINT 1: after_job_submit
        RestartFramework.at("after_job_submit")
            .on(flinkCluster)
            .restart("jobmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        // Wait for job to be running and checkpoints to occur
        Thread.sleep(1500);

        // RESTART POINT 2: after_checkpoint
        RestartFramework.at("after_checkpoint")
            .on(flinkCluster)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        // RESTART POINT 3: during_job_running
        RestartFramework.at("during_job_running")
            .on(flinkCluster)
            .restart("taskmanager")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        // Wait for job completion
        jobClient.getJobExecutionResult().get();
    }

    private static class CollectSink implements SinkFunction<Long> {
        @Override
        public void invoke(Long value, Context context) {
            // Collect results
        }
    }
}
```

### Output 2: `restarts-config/restart-config.json`

```json
{
  "tests": [
    {
      "testClass": "org.apache.flink.streaming.tests.TestCheckpointing_RestartInjected",
      "testMethod": "testCheckpointingJob",
      "restartPoints": [
        {
          "position": "after_job_submit",
          "targets": ["jobmanager"],
          "modes": ["GRACEFUL", "CRASH", "DELAYED_CRASH"]
        },
        {
          "position": "after_checkpoint",
          "targets": ["taskmanager", "jobmanager"],
          "modes": ["GRACEFUL", "CRASH", "DELAYED_CRASH"]
        },
        {
          "position": "during_job_running",
          "targets": ["taskmanager"],
          "modes": ["GRACEFUL", "CRASH"]
        }
      ]
    }
  ]
}
```

## Flink-Specific Patterns

### Pattern 1: Job Lifecycle Testing

```java
@Test
public void testJobLifecycle() throws Exception {
    StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();

    DataStream<String> stream = env.fromElements("a", "b", "c");
    stream.addSink(new TestSink());

    // Submit job
    JobClient jobClient = env.executeAsync("Lifecycle Test");

    RestartFramework.at("after_submit")
        .on(flinkCluster)
        .restart("jobmanager")
        .execute();

    // Running
    waitUntilJobIsRunning(jobClient);

    RestartFramework.at("during_running")
        .on(flinkCluster)
        .restart("taskmanager")
        .execute();

    // Complete
    jobClient.getJobExecutionResult().get();
}
```

### Pattern 2: Checkpointing and Recovery Testing

```java
@Test
public void testCheckpointRecovery() throws Exception {
    StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();
    env.enableCheckpointing(1000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

    DataStream<Long> input = env.fromSequence(1, 1000);
    DataStream<Long> stateful = input
        .keyBy(x -> x % 10)
        .map(new StatefulMapper());
    stateful.addSink(new CollectSink());

    JobClient jobClient = env.executeAsync("Checkpoint Test");

    // Wait for first checkpoint
    Thread.sleep(1500);

    RestartFramework.at("after_checkpoint")
        .on(flinkCluster)
        .restart("taskmanager")
        .withMode(RestartMode.CRASH)
        .execute();

    // Job should recover from checkpoint
    jobClient.getJobExecutionResult().get();
}
```

### Pattern 3: Stateful Processing Testing

```java
@Test
public void testStatefulProcessing() throws Exception {
    StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();
    env.enableCheckpointing(500);

    DataStream<Event> events = env.addSource(new EventSource());

    DataStream<Result> results = events
        .keyBy(Event::getKey)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .aggregate(new MyAggregateFunction());

    RestartFramework.at("after_state_update")
        .on(flinkCluster)
        .restart("taskmanager")
        .execute();

    results.addSink(new ResultSink());

    env.execute("Stateful Test");
}
```

### Pattern 4: Backpressure Testing

```java
@Test
public void testBackpressure() throws Exception {
    StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();

    DataStream<String> fast = env.addSource(new FastSource());
    DataStream<String> slow = fast.map(new SlowMapper());
    slow.addSink(new TestSink());

    JobClient jobClient = env.executeAsync("Backpressure Test");

    // Wait for backpressure to build up
    Thread.sleep(2000);

    RestartFramework.at("during_backpressure")
        .on(flinkCluster)
        .restart("taskmanager")
        .execute();

    jobClient.getJobExecutionResult().get();
}
```

### Pattern 5: Multi-TaskManager Testing

```java
@Test
public void testMultiTaskManager() throws Exception {
    StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();
    env.setParallelism(4); // Spread across multiple TaskManagers

    DataStream<Integer> data = env.fromSequence(1, 1000);
    data.rebalance()
        .map(x -> x * 2)
        .addSink(new DistributedSink());

    JobClient jobClient = env.executeAsync("Multi-TM Test");

    // Restart random TaskManager
    RestartFramework.at("during_execution")
        .on(flinkCluster)
        .restart("taskmanager")
        .withIndex("random")
        .execute();

    jobClient.getJobExecutionResult().get();
}
```

### Pattern 6: Savepoint Testing

```java
@Test
public void testSavepoint() throws Exception {
    StreamExecutionEnvironment env = flinkCluster.getTestStreamEnvironment();
    env.enableCheckpointing(1000);

    DataStream<Long> stream = env.fromSequence(1, 10000);
    stream.addSink(new TestSink());

    JobClient jobClient = env.executeAsync("Savepoint Test");

    // Trigger savepoint
    String savepointPath = jobClient
        .triggerSavepoint(savepointDir)
        .get();

    RestartFramework.at("after_savepoint")
        .on(flinkCluster)
        .restart("jobmanager")
        .execute();

    // Verify savepoint exists
    assertNotNull(savepointPath);
}
```

## Validation Checklist

After transformation, verify:

- [ ] Generated test file compiles without errors
- [ ] All original test logic is preserved
- [ ] Restart points are placed at meaningful Flink operations
- [ ] Restart position names are descriptive and Flink-specific
- [ ] Node roles (jobmanager/taskmanager) are correctly chosen
- [ ] Configuration file has correct fully-qualified class names
- [ ] Configuration file includes all restart points from the test
- [ ] Target arrays match the operation type (JM vs TM vs both)
- [ ] Mode arrays are appropriate for timing sensitivity
- [ ] Files are placed in correct locations
- [ ] Original test file is not modified (only new files created)
- [ ] Cluster instance is correctly identified (from test class or parent class)

## Advanced Scenarios

### Multiple Test Methods

If the original test has multiple `@Test` methods:

1. Transform each method independently
2. Inject restart points in each method
3. Create a separate test specification for each method in the configuration

Example configuration:
```json
{
  "tests": [
    {
      "testClass": "org.apache.flink.test.TestFlink_RestartInjected",
      "testMethod": "testJobSubmit",
      "restartPoints": [...]
    },
    {
      "testClass": "org.apache.flink.test.TestFlink_RestartInjected",
      "testMethod": "testCheckpointing",
      "restartPoints": [...]
    }
  ]
}
```

### Helper Methods

If the test has helper methods:

1. **Do not inject restart points in helper methods**
2. Only inject in `@Test` annotated methods
3. Keep helper methods unchanged

### Inherited Cluster from Parent Class

**CRITICAL**: Many Flink tests inherit the cluster from a base test class. When transforming such tests:

1. **Identify the parent class** by checking the `extends` clause
2. **Read the parent class** to find the cluster field name
3. **Use the correct cluster field name** in restart injection calls
4. **Do NOT duplicate the cluster initialization** in the child class

Example:
```java
// Parent class: FlinkTestBase.java
public abstract class FlinkTestBase {
    @ClassRule
    public static final MiniClusterWithClientResource miniCluster = ...;
}

// Child test: TestMyJob.java
public class TestMyJob extends FlinkTestBase {
    @Test
    public void testJob() {
        // Use 'miniCluster' from parent, not 'flinkCluster'
        RestartFramework.at("after_submit")
            .on(miniCluster)  // Correct: use parent's field name
            .restart("jobmanager")
            .execute();
    }
}
```

### Converting Synchronous to Asynchronous Execution

Many Flink tests use synchronous `env.execute()`. To inject restart points during job execution, convert to asynchronous:

**Before**:
```java
env.execute("Job");
```

**After**:
```java
JobClient jobClient = env.executeAsync("Job");

RestartFramework.at("after_submit")
    .on(flinkCluster)
    .restart("jobmanager")
    .execute();

jobClient.getJobExecutionResult().get();
```

### Tests Without Obvious Restart Points

If a test has no clear state transitions:

1. Inject restart points within the range of (a) after cluster setup and (b) before cluster teardown
2. Evenly distribute restart points to cover the test execution
3. You MUST Use percentage-based positions (e.g., `at_25_percent`, `at_50_percent`) to at least cover 4 points during the test execution
4. Find cluster operations to place restart points around

**IMPORTANT**: You are NOT allowed to skip any test transformation due to lack of restart points. Always inject at least one restart point per test method.

### High Availability Configuration

For tests with HA JobManager, ensure the cluster is configured with HA leadership control:

```java
@ClassRule
public static final MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(2)
            .setNumberSlotsPerTaskManager(2)
            .withHaLeadershipControl()  // Required for JM restart
            .build());
```

Then restart JobManager:
```java
RestartFramework.at("after_failover")
    .on(flinkCluster)
    .restart("jobmanager")
    .withIndex(0)
    .execute();
```

## Common Flink Operations and Suggested Restart Points

| Flink Operation | Suggested Restart Point Name | Node Role | Timing |
|-----------------|------------------------------|-----------|--------|
| `env.execute()`, `executeAsync()` | `after_job_submit` | `jobmanager` | After |
| Job running | `during_job_running` | `taskmanager` | During |
| Job cancellation | `before_job_cancel` | `jobmanager` | Before |
| Checkpoint trigger | `after_checkpoint` | `taskmanager`, `jobmanager` | After |
| Savepoint trigger | `after_savepoint` | `jobmanager` | After |
| State update (map, reduce, etc.) | `after_state_update` | `taskmanager` | After |
| Window processing | `during_window_processing` | `taskmanager` | During |
| Source ingestion | `during_source_ingestion` | `taskmanager` | During |
| Sink write | `before_sink_write` | `taskmanager` | Before |
| Task deployment | `after_task_deploy` | `taskmanager` | After |
| Slot allocation | `after_slot_allocate` | `jobmanager`, `taskmanager` | After |
| Job recovery | `after_recovery` | `jobmanager`, `taskmanager` | After |
| Backpressure scenario | `during_backpressure` | `taskmanager` | During |

## Notes

- **Non-invasive**: Original test file is never modified
- **Incremental**: Can transform tests one at a time
- **Compatible**: Generated tests can run both with and without restart injection
- **Configurable**: Configuration file allows easy adjustment of test matrix
- **Flink-aware**: Node role selection is specific to Flink architecture (JobManager vs TaskManager)
- **Parent-aware**: Handles cluster instances inherited from parent test classes

## Dependencies

Ensure the following dependencies are included in the test's module to use the Restart Testing Framework:

```xml
<dependencies>
    <!-- Existing dependencies... -->

    <!-- Restart Testing Framework - Core -->
    <dependency>
        <groupId>org.restarttest</groupId>
        <artifactId>restart-core</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <scope>test</scope>
    </dependency>

    <!-- Restart Testing Framework - Flink Adapter -->
    <dependency>
        <groupId>org.restarttest</groupId>
        <artifactId>flink-restart-adapter</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Build Requirements

**IMPORTANT**: Flink tests require Java 17 and special Maven options to build and compile.

### Prerequisites

Ensure Java 17 is installed and available:

```bash
# Check Java version
java -version  # Should show Java 17

# If using multiple Java versions (SDKMAN, update-alternatives, etc.):
# Set JAVA_HOME to Java 17
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Set required MAVEN_OPTS to allow reflection access
export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED"
```

The `--add-opens` flags are required for Flink's internal reflection operations to work properly with Java 17's module system.

### Building the Restart Testing Framework

**CRITICAL**: The `flink-restart-adapter` module is a standalone module and should be built FIRST before building any test modules that depend on it.

```bash
# Step 1: Build and install the flink-restart-adapter module
cd flink-restart-adapter
mvn clean install -DskipTests

# This installs flink-restart-adapter-2.2.0.jar to your local Maven repository (~/.m2/repository)
```

### Building Test Modules with Restart Dependencies

After building `flink-restart-adapter`, you can build any of the test modules that have the restart testing dependencies:

```bash
# Example: Build flink-tests module
cd flink-tests
mvn clean test

# Example: Build flink-cep module
cd flink-libraries/flink-cep
mvn clean test

# Example: Build all modules from Flink root (not recommended for individual test development)
cd /path/to/flink
mvn clean install -DskipTests
```

### Build Order Summary

```bash
# 1. Set up Java 17 and MAVEN_OPTS (do this once per terminal session)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED"

# 2. Build flink-restart-adapter FIRST
cd flink-restart-adapter
mvn clean install -DskipTests

# 3. Build test modules (example)
cd ../flink-tests
mvn clean test

# Or build a specific test class
mvn test -Dtest=YourTestClass_RestartInjected
```
