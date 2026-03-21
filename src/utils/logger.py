import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_logger(module_name: str) -> logging.Logger:
    """
    Create and return a configured logger.

    Features
    - Console + file logging
    - Rotating log files
    - Safe path handling
    - Prevent duplicate handlers
    - Configurable log level via environment variable
    """

    logger = logging.getLogger(module_name)

    if logger.handlers:
        return logger

    # -------------------------------------------------------
    # Log level
    # -------------------------------------------------------

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    logger.propagate = False

    # -------------------------------------------------------
    # Log directory
    # -------------------------------------------------------

    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "system.log"

    # -------------------------------------------------------
    # Handlers
    # -------------------------------------------------------

    console_handler = logging.StreamHandler()

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )

    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.DEBUG)

    # -------------------------------------------------------
    # Formatter
    # -------------------------------------------------------

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
