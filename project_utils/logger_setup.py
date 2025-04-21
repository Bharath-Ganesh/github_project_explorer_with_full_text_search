import logging
import os
from project_utils.env_utils import LOGGER_LEVEL

LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

def setup_logger(log_file_path="logs/project_parser.log"):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    level_name = (LOGGER_LEVEL or "INFO").upper()
    level = LOG_LEVEL_MAP.get(level_name, logging.INFO)

    logging.basicConfig(
        filename=log_file_path,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=level
    )

    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)



def get_logger(name=__name__, log_file="project_processing.log"):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():  # Prevent multiple handlers
        level_name = (LOGGER_LEVEL or "INFO").upper()
        level = LOG_LEVEL_MAP.get(level_name, logging.INFO)
        logger.setLevel(level)

        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger