"""
DAT535 Centralized PySpark Session Factory & Configuration.
Author: Group 13
Provides cross-platform (Windows & Linux CI/CD) optimized SparkSession initialization.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("spark_session_factory")


def _configure_environment():
    """Ensure JAVA_HOME, HADOOP_HOME (for Windows), PATH, and PySpark Python paths are properly configured."""
    # If JAVA_HOME is not set, try to locate it within the conda environment
    if "JAVA_HOME" not in os.environ:
        conda_env = sys.prefix
        candidate_jvm = Path(conda_env) / "Library" / "lib" / "jvm"
        candidate_library = Path(conda_env) / "Library"
        if candidate_jvm.exists():
            os.environ["JAVA_HOME"] = str(candidate_jvm)
        elif candidate_library.exists() and (candidate_library / "bin" / "java.exe").exists():
            os.environ["JAVA_HOME"] = str(candidate_library)

    # Windows Hadoop / winutils configuration
    if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
        # Check repo or parent directory
        repo_hadoop = Path(__file__).resolve().parent.parent.parent / "hadoop"
        if (repo_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(repo_hadoop)
            os.environ["PATH"] = str(repo_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")

    # Ensure current Python executable is used for Spark driver and workers
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Ensure conda Library/bin is on PATH on Windows for DLLs
    if sys.platform == "win32":
        lib_bin = str(Path(sys.prefix) / "Library" / "bin")
        if lib_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = lib_bin + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(
    app_name: str = "DAT535-Medallion-Pipeline",
    master: Optional[str] = None,
    extra_configs: Optional[Dict[str, str]] = None,
    log_level: str = "WARN",
):
    """
    Build or retrieve an existing tuned PySpark SparkSession.
    Configures Adaptive Query Execution (AQE), UTC timestamp alignment, Kryo serialization,
    and platform-appropriate local host networking.
    """
    _configure_environment()

    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        logger.error(f"Failed to import PySpark: {e}. Please install pyspark.")
        raise

    builder = SparkSession.builder.appName(app_name)

    # In local development / testing, default master to local[*] if not set
    if master:
        builder = builder.master(master)
    elif "SPARK_MASTER" in os.environ:
        builder = builder.master(os.environ["SPARK_MASTER"])
    else:
        # Default local multi-threaded
        builder = builder.master("local[2]")

    # Adaptive Query Execution (AQE) & Performance Tuning (taught in Lab 2 & 3)
    builder = (
        builder.config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
    )

    # Windows specific networking stability
    if sys.platform == "win32":
        builder = builder.config("spark.driver.host", "127.0.0.1")

    # Apply any custom overrides
    if extra_configs:
        for key, val in extra_configs.items():
            builder = builder.config(key, val)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    logger.debug(f"SparkSession '{app_name}' active (Spark v{spark.version})")
    return spark


def stop_spark_session(spark=None):
    """Safely stop an active SparkSession."""
    if spark:
        try:
            spark.stop()
        except Exception:
            pass
