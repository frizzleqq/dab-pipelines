"""Logging configuration for dab_pipelines.

This module provides centralized logging configuration with support for
different log levels and output formats. The logging setup supports both
console output and optional file-based logging.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dab_pipelines import databricks_utils


def create_log_volume(
    catalog: str,
    log_subdir: Optional[str] = None,
    schema: str = "default",
    volume_name: str = "logs",
) -> Path:
    """Create log directory in Unity Catalog Volume.

    Creates the logs volume in the specified schema if it doesn't exist,
    and returns the full path including any subdirectory.

    Parameters
    ----------
    catalog : str
        The Unity Catalog name.
    log_subdir : str, optional
        Subdirectory within the logs volume for organizing logs.
    schema : str, default="default"
        The schema name where the logs volume is located.
    volume_name : str, default="logs"
        The volume name for storing logs.

    Returns
    -------
    Path
        Full path to the log directory: /Volumes/{catalog}/{schema}/{volume_name}/{log_subdir}

    Examples
    --------
    >>> create_log_directory("main")
    PosixPath('/Volumes/main/default/logs')
    >>> create_log_directory("main", "data_generator")
    PosixPath('/Volumes/main/default/logs/data_generator')
    >>> create_log_directory("main", schema="prod", volume_name="app_logs")
    PosixPath('/Volumes/main/prod/app_logs')
    """
    # Create the volume if it doesn't exist
    volume_path = databricks_utils.create_volume_if_not_exists(
        catalog=catalog,
        schema=schema,
        volume_name=volume_name,
        comment="Volume for application logs",
    )

    # Append subdirectory if provided
    if log_subdir:
        return volume_path / log_subdir
    return volume_path


def setup_logging(verbose: bool = False, log_dir: Optional[Path] = None) -> None:
    """Configure logging for the application.

    Sets up logging with a consistent format across all modules. By default,
    logs at INFO level and above. When verbose mode is enabled, DEBUG level
    logs are also displayed.

    The log format follows: YYYY-MM-DD HH:MM:SS,mmm - LEVEL - logger_name - message

    Parameters
    ----------
    verbose : bool, default=False
        If True, enable DEBUG level logging. Otherwise, log INFO and above.
    log_dir : Path, optional
        Directory path for log files. If provided, logs will be written to
        a timestamped file: dab_pipelines_YYYYMMDD_HHMMSS.log

    Examples
    --------
    >>> setup_logging(verbose=False)  # INFO and above to console
    >>> setup_logging(verbose=True)  # DEBUG and above to console
    >>> setup_logging(log_dir=Path("./logs"))  # Also write to file
    """
    # Determine log level based on verbose flag
    log_level = logging.DEBUG if verbose else logging.INFO

    # Define log format matching the required pattern:
    # 2025-04-23 10:35:42,789 - INFO - auth - User login successful.
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Create formatter and add to handler
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    console_handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger.addHandler(console_handler)

    # Add file handler if log directory is specified
    if log_dir:
        # Ensure the log directory exists
        log_dir.mkdir(parents=True, exist_ok=True)

        # Create timestamped log filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"dab_pipelines_{timestamp}.log"

        # Create file handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        # Add file handler to root logger
        root_logger.addHandler(file_handler)
