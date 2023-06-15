import logging
import os

_log_level = os.getenv("LOG_LEVEL")
LOG_LEVEL = logging.getLevelName(_log_level) if _log_level else logging.INFO
LOG_FORMAT = "[%(levelname)s] %(filename)s:%(lineno)s - %(message)s"

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))

logger = logging.getLogger(__name__.split(".")[0])
for h in logger.handlers:
    logger.removeHandler(h)
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)
