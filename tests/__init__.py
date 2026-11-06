"""
DAT535 Medallion Test Suite Initialization.
Ensures SparkSession environment is properly sanitized across Linux CI/CD and Windows.
"""
from src.utils.spark import _configure_environment

_configure_environment()
