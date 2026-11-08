"""Create the local Spark session used by the AI Monitoring application."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


ADVANCED_DIR = Path(__file__).resolve().parents[1]
ETL_DIR = ADVANCED_DIR / "ETL"

WAREHOUSE_DIR = ETL_DIR / "warehouse"
METASTORE_DIR = ETL_DIR / "metastore_db"
SPARK_TEMP_DIR = ETL_DIR / ".spark-temp"


def _configure_runtime_environment() -> None:
    """Configure Java/Hadoop paths before the Spark JVM starts."""

    java_home = os.environ.get("JAVA_HOME")

    if java_home:
        java_bin = Path(java_home) / "bin"
        java_executable = java_bin / ("java.exe" if os.name == "nt" else "java")

        if not java_executable.exists():
            raise RuntimeError(
                f"JAVA_HOME does not contain a Java executable: {java_executable}"
            )

        os.environ["PATH"] = os.pathsep.join(
            [str(java_bin), os.environ.get("PATH", "")]
        )
    elif shutil.which("java") is None:
        raise RuntimeError(
            "Java was not found. Set JAVA_HOME or add Java to PATH."
        )

    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME")

        if not hadoop_home:
            raise RuntimeError(
                "HADOOP_HOME is not set. Set it to the Hadoop directory "
                "used by the project's Spark notebooks."
            )

        hadoop_bin = Path(hadoop_home) / "bin"
        required_files = (
            hadoop_bin / "winutils.exe",
            hadoop_bin / "hadoop.dll",
            hadoop_bin / "hdfs.dll",
        )
        missing_files = [str(path) for path in required_files if not path.exists()]

        if missing_files:
            raise FileNotFoundError(
                "Required Hadoop files were not found: "
                + ", ".join(missing_files)
            )

        os.environ["PATH"] = os.pathsep.join(
            [str(hadoop_bin), os.environ.get("PATH", "")]
        )

    # Keep Spark workers on the same Python environment as Streamlit.
    os.environ["PYSPARK_PYTHON"] = sys.executable


def create_spark_session():
    """Create or retrieve the project's Hive-enabled local Spark session.

    The warehouse and Derby metastore paths intentionally match the paths
    used by the project's Gold and query notebooks.
    """
    _configure_runtime_environment()

    # Import after setting the runtime environment.
    from pyspark.sql import SparkSession

    for directory in (WAREHOUSE_DIR, METASTORE_DIR, SPARK_TEMP_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    metastore_url = (
        "jdbc:derby:;databaseName="
        f"{METASTORE_DIR.as_posix()};create=true"
    )

    spark = (
        SparkSession.builder
        .appName("AI_Monitoring_Streamlit")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR.as_uri())
        .config("spark.local.dir", str(SPARK_TEMP_DIR))
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            metastore_url,
        )
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark
