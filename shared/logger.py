from prefect import get_run_logger

def info(msg):
    """
    Log a message to the console and Prefect logs.

    Args:
        msg (str): The message to log.
    """
    logger = get_run_logger()
    logger.info(msg)

def error(msg):
    """
    Log an error message to the console and Prefect logs.

    Args:
        msg (str): The error message to log.
    """
    logger = get_run_logger()
    logger.error(msg)