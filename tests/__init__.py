"""
DAT535 Medallion Test Suite Initialization.
Ensures SparkSession environment is properly sanitized across Linux CI/CD and Windows.
Silences internal third-party library deprecation noise.
"""
import warnings

# Suppress known external library deprecations (Pandas 2/3 and PySpark 3.5 bridge)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)
warnings.filterwarnings("ignore", message=".*copy keyword is deprecated.*")
warnings.filterwarnings("ignore", message=".*is_datetime64tz_dtype is deprecated.*")
warnings.filterwarnings("ignore", message=".*distutils Version classes are deprecated.*")

from src.utils.spark import _configure_environment

_configure_environment()
