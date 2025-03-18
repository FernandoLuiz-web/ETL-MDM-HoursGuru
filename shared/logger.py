import os
import logging
from functools import wraps

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Application Logs")
LOG_NAME = os.path.join(LOG_DIR, "logger.log")

os.makedirs(LOG_DIR, exist_ok=True)

if os.path.exists(LOG_NAME):
    os.remove(LOG_NAME)

def logger(description: str):
    """
        Decorador para pegar automaticamente os eventos de log de execução.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                logging.info(f"INFO - Método {func.__name__}: {description}")
                return result
            except Exception as e:
                logging.error(f"ERRO - Método {func.__name__}: {description} | {e}")
                raise
        return wrapper
    return decorator

logging.basicConfig(
    filename = LOG_NAME,
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    encoding = "utf-8"
)