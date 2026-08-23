import os
import subprocess
import sys

import pytest

MAIN = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "main")
)
if MAIN not in sys.path:
    sys.path.insert(0, MAIN)
os.environ["PYTHONPATH"] = MAIN + os.pathsep + os.environ.get("PYTHONPATH", "")


@pytest.fixture(scope="session")
def spark():
    os.environ["JAVA_HOME"] = subprocess.check_output(
        ["/usr/libexec/java_home", "-v", "17"], text=True
    ).strip()

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"])
        .config("spark.pyspark.python", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def discover(spark):
    def run(columns, rows):
        from mvd import MVDDiscovery
        from stats import Stats

        discovery = MVDDiscovery(
            raw_rdd=spark.sparkContext.parallelize(rows, numSlices=2),
            attr_names=columns,
            stats=Stats(spark.sparkContext),
            sampling_config={
                "enabled": False,
                "largest_k": 2,
                "mode": "manual",
                "auto": {"coverage": 0.3, "min_k": 3, "max_k": 20},
            },
            fd_config={"enabled": True, "superkey_threshold": 0.80},
            pruning_config=None,
            num_partitions=2,
            shuffle_partitions=2,
            total_rows=len(rows),
            representation_config=None,
        )

        mvds = set(discovery.discovery())
        fds = {
            (tuple(columns[i] for i in sorted(lhs)), columns[rhs])
            for lhs, rhs in discovery.functional_dependencies
        }

        discovery.packed_rdd.unpersist()
        return fds, mvds

    return run


@pytest.fixture
def packed(spark):
    from packing import bits_for_cardinality
    from workers import build_value_encoders, create_packed_rdd

    built = []

    def build(columns, rows):
        sc = spark.sparkContext
        raw_rdd = sc.parallelize(rows, numSlices=2)
        encoders = build_value_encoders(raw_rdd, len(columns))
        bits = bits_for_cardinality(max(len(e) for e in encoders))

        packed_rdd = create_packed_rdd(
            sc, raw_rdd, encoders, len(columns), bits
        )
        packed_rdd.count()
        built.append(packed_rdd)
        return packed_rdd, bits

    yield build
    for packed_rdd in built:
        packed_rdd.unpersist()
