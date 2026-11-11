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

# Keep Windows DLL search-directory handles alive for the lifetime of the
# process. Python 3.8+ uses AddDllDirectory for dependent native libraries.
_DLL_DIRECTORY_HANDLES = []


def _configure_runtime_environment() -> None:
    """Configure Java and Python environments before Spark starts."""

    # --------------------------------------------------------
    # 1. Configure Java
    # --------------------------------------------------------

    java_home = os.environ.get("JAVA_HOME")

    if java_home:
        java_bin = Path(java_home) / "bin"

        java_executable = java_bin / (
            "java.exe" if os.name == "nt" else "java"
        )

        if not java_executable.is_file():
            raise RuntimeError(
                f"JAVA_HOME does not contain Java: {java_executable}"
            )

        os.environ["PATH"] = os.pathsep.join(
            [str(java_bin), os.environ.get("PATH", "")]
        )

    elif shutil.which("java") is None:
        raise RuntimeError(
            "Java was not found. Set JAVA_HOME or add Java to PATH."
        )

    # --------------------------------------------------------
    # 2. Configure required Hadoop native libraries on Windows
    # --------------------------------------------------------

    if os.name == "nt":

        hadoop_home = os.environ.get("HADOOP_HOME")

        if not hadoop_home:
            raise RuntimeError(
                "HADOOP_HOME is not set. On Windows, point it to the Hadoop "
                "distribution whose native binaries match the Hadoop version "
                "bundled with this environment's PySpark installation."
            )

        hadoop_bin = Path(hadoop_home).resolve() / "bin"

        required_binaries = (
            "winutils.exe",
            "hadoop.dll",
            "hdfs.dll",
        )
        missing_binaries = [
            name
            for name in required_binaries
            if not (hadoop_bin / name).is_file()
        ]
        if missing_binaries:
            missing = ", ".join(missing_binaries)
            raise FileNotFoundError(
                f"Missing Hadoop Windows binaries in {hadoop_bin}: {missing}. "
                "Install the native binaries matching the Hadoop version "
                "bundled with PySpark."
            )

        # Put this Hadoop distribution first so an older copy elsewhere on
        # PATH cannot be selected by the JVM.
        os.environ["PATH"] = os.pathsep.join(
            [str(hadoop_bin), os.environ.get("PATH", "")]
        )

        # Register the directory for dependent DLL resolution on modern
        # Python/Windows runtimes, and retain the handle for the process life.
        if hasattr(os, "add_dll_directory"):
            _DLL_DIRECTORY_HANDLES.append(
                os.add_dll_directory(str(hadoop_bin))
            )

    # --------------------------------------------------------
    # 3. Use the current virtual environment Python
    # --------------------------------------------------------

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
