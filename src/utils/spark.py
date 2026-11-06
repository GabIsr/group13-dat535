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


def _get_java_major_version(java_exe: Path) -> Optional[int]:
    """Parse major Java version from java -version output (e.g. 8, 11, 17, 21)."""
    try:
        import subprocess
        res = subprocess.run(
            [str(java_exe), "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )
        output = res.stderr or res.stdout
        for line in output.splitlines():
            if "version" in line:
                parts = line.split('"')
                if len(parts) >= 2:
                    v_str = parts[1]
                    if v_str.startswith("1."):
                        return int(v_str.split(".")[1])
                    return int(v_str.split(".")[0])
    except Exception:
        pass
    return None


def _configure_environment():
    """Ensure JAVA_HOME, SPARK_HOME, HADOOP_HOME (for Windows), PATH, and PySpark Python paths are properly configured."""
    # 1. Validate & sanitize SPARK_HOME to prevent classpath incompatibilities
    # If SPARK_HOME points to an external Spark install (e.g. /opt/spark 3.5.0) while Python
    # has pip-installed PySpark (e.g. 4.x), Py4J fails to load Spark SQL classes:
    # "TypeError: 'JavaPackage' object is not callable".
    if "SPARK_HOME" in os.environ:
        spark_home = Path(os.environ["SPARK_HOME"])
        spark_home_jars = spark_home / "jars"
        if not spark_home.exists() or not spark_home_jars.is_dir():
            logger.warning(f"SPARK_HOME={spark_home} does not exist or lacks jars directory. Unsetting SPARK_HOME.")
            del os.environ["SPARK_HOME"]
        else:
            try:
                import pyspark
                pyspark_major = pyspark.__version__.split(".")[0]
                sql_jars = list(spark_home_jars.glob("spark-core*.jar"))
                if sql_jars:
                    jar_ver = sql_jars[0].name.split("-")[-1].replace(".jar", "").split(".")[0]
                    if pyspark_major != jar_ver:
                        logger.warning(
                            f"PySpark major version ({pyspark_major}) clashes with SPARK_HOME jars ({jar_ver} at {spark_home}). "
                            f"Unsetting SPARK_HOME to use pip-bundled jars."
                        )
                        del os.environ["SPARK_HOME"]
            except Exception:
                pass

    # 2. Validate JAVA_HOME and check version compatibility with PySpark
    pyspark_major = None
    try:
        import pyspark
        pyspark_major = int(pyspark.__version__.split(".")[0])
    except Exception:
        pass

    if "JAVA_HOME" in os.environ:
        java_home = Path(os.environ["JAVA_HOME"])
        java_exe = "java.exe" if sys.platform == "win32" else "java"
        j_bin = java_home / "bin" / java_exe
        if not j_bin.exists():
            logger.warning(f"JAVA_HOME={java_home} does not contain bin/{java_exe}. Removing invalid JAVA_HOME.")
            del os.environ["JAVA_HOME"]
        elif pyspark_major and pyspark_major >= 4:
            # PySpark 4.x bytecode requires Java 17+ (class file version 61.0).
            # If current JAVA_HOME is Java 8 or 11, it will fail with UnsupportedClassVersionError.
            j_ver = _get_java_major_version(j_bin)
            if j_ver and j_ver < 17:
                logger.warning(
                    f"Configured JAVA_HOME ({java_home}) is Java {j_ver}, but PySpark {pyspark_major}.x "
                    f"requires Java 17+. Searching for compatible Java runtime..."
                )
                del os.environ["JAVA_HOME"]

    # If JAVA_HOME is not set, try to locate it within the conda environment or system paths
    if "JAVA_HOME" not in os.environ:
        conda_env = sys.prefix
        candidate_jvm = Path(conda_env) / "Library" / "lib" / "jvm"
        candidate_library = Path(conda_env) / "Library"
        if candidate_jvm.exists():
            os.environ["JAVA_HOME"] = str(candidate_jvm)
        elif candidate_library.exists() and (candidate_library / "bin" / "java.exe").exists():
            os.environ["JAVA_HOME"] = str(candidate_library)
        elif sys.platform != "win32":
            # Search /usr/lib/jvm on Linux for Java 17 or 21
            jvm_dir = Path("/usr/lib/jvm")
            if jvm_dir.is_dir():
                candidates = sorted(list(jvm_dir.glob("*17*")) + list(jvm_dir.glob("*21*")), reverse=True)
                for cand in candidates:
                    cand_bin = cand / "bin" / "java"
                    if cand_bin.exists() and cand.is_dir():
                        cand_ver = _get_java_major_version(cand_bin)
                        if cand_ver and cand_ver >= 17:
                            logger.info(f"Auto-selected Java {cand_ver} runtime at {cand} for PySpark {pyspark_major}.x")
                            os.environ["JAVA_HOME"] = str(cand)
                            os.environ["PATH"] = str(cand / "bin") + os.pathsep + os.environ.get("PATH", "")
                            break

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


# Run environment configuration on import
_configure_environment()


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
