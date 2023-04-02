
import logging
import os
import sys
from dotenv import load_dotenv
load_dotenv()


def get_logger(logger_name: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)

    logger.setLevel(int(os.environ.get("logger_level", 30)))

    handler = logging.FileHandler(f'lof_{logger_name}.log')
    stdout_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stdout_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    stdout_handler.setFormatter(stdout_formatter)
    logger.addHandler(handler)
    logger.addHandler(stdout_handler)
    return logger


if __name__ == "__main__":
    logger = get_logger("macbook")
    logger.info("hello world")
