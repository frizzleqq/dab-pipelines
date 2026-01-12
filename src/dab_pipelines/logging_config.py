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
    >>> setup_logging(verbose=True)   # DEBUG and above to console
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


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a module.

    This is a convenience wrapper around logging.getLogger() to maintain
    consistency across the codebase.

    Parameters
    ----------
    name : str
        The name of the logger, typically __name__ of the calling module.

    Returns
    -------
    logging.Logger
        Configured logger instance.

    Examples
    --------
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing started")
    """
    return logging.getLogger(name)
