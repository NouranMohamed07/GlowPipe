"""
Logging configuration for Paula's Choice scraper.

This file creates a reusable logger that writes logs to:
1. Console
2. Log file inside logs/paulas_choice/
"""

import logging
from pathlib import Path

from config import LOGS_DIR
from utils import create_directory


def setup_logger(
    logger_name="paulas_choice_scraper",
    log_file_name="scrape_ingredients.log",
    log_level=logging.INFO,
):
    """
    Create and return a configured logger.

    Prevents duplicate handlers if the logger is called multiple times.
    """

    create_directory(LOGS_DIR)

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if logger.handlers:
        return logger

    log_file_path = Path(LOGS_DIR) / log_file_name

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        log_file_path,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Logger initialized successfully.")
    logger.info(f"Log file path: {log_file_path}")

    return logger