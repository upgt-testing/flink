#!/usr/bin/env python3
"""
Comprehensive script to map all Flink test classes to the mini cluster they use.
"""

import os
import re
from collections import defaultdict
from pathlib import Path

# Base directory
BASE_DIR = "/home/shuai/xlab/restart_testing/flink"

# Cluster type mapping
CLUSTER_TYPES = {
    'InternalMiniClusterExtension': 'InternalMiniClusterExtension',
    'MiniCluster': 'MiniCluster',
    'MiniClusterClient': 'MiniClusterClient',
    'MiniClusterExtension': 'MiniClusterExtension',
    'MiniClusterResource': 'MiniClusterResource',
    'MiniClusterTestEnvironment': 'MiniClusterTestEnvironment',
    'MiniClusterWithClientResource': 'MiniClusterWithClientResource',
    'TestingMiniCluster': 'TestingMiniCluster',
}

# Base classes that use specific cluster types
BASE_CLASS_CLUSTERS = {
    'AbstractTestBase': 'MiniClusterExtension',
    'AbstractTestBaseJUnit4': 'MiniClusterWithClientResource',
    'BatchAbstractTestBase': 'MiniClusterExtension',
    'StreamAbstractTestBase': 'MiniClusterExtension',
    'SimpleRecoveryITCaseBase': 'MiniClusterWithClientResource',
    'StreamFaultToleranceTestBase': 'MiniClusterWithClientResource',
    'CancelingTestBase': 'MiniClusterWithClientResource',
    'ChangelogRecoveryITCaseBase': 'MiniClusterWithClientResource',
    'UnalignedCheckpointTestBase': 'MiniClusterWithClientResource',
    'MultipleProgramsTestBase': 'MiniClusterExtension',  # extends AbstractTestBase
    'MultipleProgramsTestBaseJUnit4': 'MiniClusterWithClientResource',  # extends AbstractTestBaseJUnit4
    'JavaProgramTestBase': 'MiniClusterExtension',  # extends AbstractTestBase
    'JavaProgramTestBaseJUnit4': 'MiniClusterWithClientResource',  # extends AbstractTestBaseJUnit4
    'TableITCaseBase': 'MiniClusterExtension',
    'AbstractQueryableStateTestBase': 'MiniClusterWithClientResource',
    'SavepointReaderITTestBase': 'MiniClusterWithClientResource',
    'SavepointTestBase': 'MiniClusterWithClientResource',
    'AbstractSqlGatewayStatementITCaseBase': 'MiniClusterWithClientResource',
    'TestFileSystemCatalogTestBase': 'MiniClusterExtension',
    'ExampleOutputTestBase': 'MiniClusterExtension',
    'SimpleRecoveryFixedDelayRestartStrategyITBase': 'MiniClusterWithClientResource',
    'SimpleRecoveryExponentialDelayRestartStrategyITBase': 'MiniClusterWithClientResource',
    'SimpleRecoveryFailureRateStrategyITBase': 'MiniClusterWithClientResource',
    'ChangelogRecoverySwitchEnvTestBase': 'MiniClusterWithClientResource',
    'BatchAbstractTestBase': 'MiniClusterExtension',  # table-planner batch tests
    'StreamAbstractTestBase': 'MiniClusterExtension',  # table-planner stream tests
    'AdaptiveBatchAbstractTestBase': 'MiniClusterExtension',  # table-planner adaptive batch tests
    'AtomicCtasITCaseBase': 'MiniClusterExtension',
    'AtomicRtasITCaseBase': 'MiniClusterExtension',
    'JoinReorderITCaseBase': 'MiniClusterExtension',
    'VectorSearchITCaseBase': 'MiniClusterExtension',
    'CompactionITCaseBase': 'MiniClusterExtension',
    'FileCompactionITCaseBase': 'MiniClusterExtension',
    'RestAPIITCaseBase': 'MiniClusterWithClientResource',
}

# Results: cluster_type -> set of test classes
results = defaultdict(set)

# Track class inheritance for multi-level resolution
class_inheritance = {}  # class_name -> parent_class_name
class_to_file = {}  # class_name -> file_path


def is_test_file(file_path):
    """Check if file is likely a test file."""
    file_str = str(file_path)
    return '/src/test/' in file_str and file_path.suffix == '.java'


def is_test_class(class_name, file_path):
    """Check if class name indicates it's a test class."""
    test_indicators = ['Test', 'ITCase', 'ITC', 'TestBase']
    file_indicators = ['Test.java', 'ITCase.java', 'ITC.java']

    # Check class name
    for indicator in test_indicators:
        if indicator in class_name:
            return True

    # Check file name
    file_name = os.path.basename(str(file_path))
    for indicator in file_indicators:
        if file_name.endswith(indicator):
            return True

    return False


def extract_package_and_class(file_path, class_name):
    """Extract fully qualified class name from file."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Find package
        package_match = re.search(r'package\s+([\w.]+)\s*;', content)
        if package_match:
            package = package_match.group(1)
            return f"{package}.{class_name}"
    except Exception as e:
        pass

    return class_name


def analyze_file(file_path):
    """Analyze a single Java test file for cluster usage."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        return

    # Find all class declarations
    class_pattern = r'(?:public\s+)?(?:abstract\s+)?class\s+(\w+)(?:\s+extends\s+(\w+))?'
    class_matches = re.finditer(class_pattern, content)

    for match in class_matches:
        class_name = match.group(1)
        parent_class = match.group(2)

        # Only process test classes
        if not is_test_class(class_name, file_path):
            continue

        fq_class_name = extract_package_and_class(file_path, class_name)

        # Track inheritance
        if parent_class:
            class_inheritance[class_name] = parent_class
            class_to_file[class_name] = str(file_path)

        # Check for direct cluster usage via annotations
        # @RegisterExtension with MiniClusterExtension
        if re.search(r'@RegisterExtension.*MiniClusterExtension', content, re.DOTALL):
            results['MiniClusterExtension'].add(fq_class_name)
            continue

        # @ClassRule with MiniClusterWithClientResource
        if re.search(r'@ClassRule.*MiniClusterWithClientResource', content, re.DOTALL):
            results['MiniClusterWithClientResource'].add(fq_class_name)
            continue

        # @Rule with MiniClusterWithClientResource
        if re.search(r'@Rule.*MiniClusterWithClientResource', content, re.DOTALL):
            results['MiniClusterWithClientResource'].add(fq_class_name)
            continue

        # @ExtendWith(InternalMiniClusterExtension)
        if 'InternalMiniClusterExtension' in content:
            results['InternalMiniClusterExtension'].add(fq_class_name)
            continue

        # Direct instantiation patterns
        if re.search(r'new\s+MiniCluster\s*\(', content):
            results['MiniCluster'].add(fq_class_name)
            continue

        if re.search(r'new\s+TestingMiniCluster\s*\(', content):
            results['TestingMiniCluster'].add(fq_class_name)
            continue

        if re.search(r'new\s+MiniClusterWithClientResource\s*\(', content):
            results['MiniClusterWithClientResource'].add(fq_class_name)
            continue

        if re.search(r'new\s+MiniClusterExtension\s*\(', content):
            results['MiniClusterExtension'].add(fq_class_name)
            continue

        # MiniClusterTestEnvironment
        if 'MiniClusterTestEnvironment' in content:
            results['MiniClusterTestEnvironment'].add(fq_class_name)
            continue

        # MiniClusterResource
        if 'MiniClusterResource' in content and 'new MiniClusterResource' in content:
            results['MiniClusterResource'].add(fq_class_name)
            continue

        # Check inheritance from base classes
        if parent_class:
            if parent_class in BASE_CLASS_CLUSTERS:
                cluster_type = BASE_CLASS_CLUSTERS[parent_class]
                results[cluster_type].add(fq_class_name)


def resolve_multi_level_inheritance():
    """Resolve multi-level inheritance to find all transitive test classes."""
    # Build a map of parent class -> children classes
    children = defaultdict(set)
    for child, parent in class_inheritance.items():
        children[parent].add(child)

    # For each base class, find all descendants
    for base_class, cluster_type in BASE_CLASS_CLUSTERS.items():
        descendants = set()
        queue = [base_class]
        visited = set()

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            if current in children:
                for child in children[current]:
                    descendants.add(child)
                    queue.append(child)

        # Add all descendants as test classes using this cluster
        for descendant in descendants:
            if descendant in class_to_file:
                file_path = class_to_file[descendant]
                fq_name = extract_package_and_class(Path(file_path), descendant)
                if is_test_class(descendant, Path(file_path)):
                    results[cluster_type].add(fq_name)


def main():
    """Main analysis function."""
    print("Starting analysis of Flink test classes...")

    # Find all Java test files
    test_files = []
    for root, dirs, files in os.walk(BASE_DIR):
        # Skip non-test directories
        if '/src/test/' not in root:
            continue

        for file in files:
            if file.endswith('.java'):
                file_path = Path(root) / file
                test_files.append(file_path)

    print(f"Found {len(test_files)} test files")

    # Analyze each file
    for i, file_path in enumerate(test_files):
        if i % 500 == 0:
            print(f"Processed {i}/{len(test_files)} files...")
        analyze_file(file_path)

    print("Resolving multi-level inheritance...")
    resolve_multi_level_inheritance()

    # Write results to CSV
    output_file = os.path.join(BASE_DIR, 'mini-flink-cluster-test.csv')

    print(f"\nWriting results to {output_file}...")

    # Collect all entries and sort
    entries = []
    for cluster_type in sorted(CLUSTER_TYPES.keys()):
        if cluster_type in results:
            for test_class in sorted(results[cluster_type]):
                entries.append((cluster_type, test_class))

    # Write CSV
    with open(output_file, 'w') as f:
        f.write("ClusterType,TestClass\n")
        for cluster_type, test_class in entries:
            f.write(f"{cluster_type},{test_class}\n")

    # Print summary
    print("\nSummary:")
    print("-" * 80)
    total = 0
    for cluster_type in sorted(CLUSTER_TYPES.keys()):
        count = len(results.get(cluster_type, set()))
        if count > 0:
            print(f"{cluster_type}: {count} test classes")
            total += count
    print("-" * 80)
    print(f"Total: {total} test classes mapped to clusters")
    print(f"\nResults written to: {output_file}")


if __name__ == '__main__':
    main()
