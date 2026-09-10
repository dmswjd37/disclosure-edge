import logging
import os
import re
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


LOG_DIR = Path(os.getenv("DISCLOSURE_EDGE_LOG_DIR", "logs"))
LOG_FILE_NAME = "dart_monitor.log"


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()

    if getattr(logger, "_disclosure_edge_configured", False):
        return logger

    logger.setLevel(logging.INFO)
    # HTTPX request logs contain Telegram's bot token in the URL.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=str(LOG_DIR / LOG_FILE_NAME),
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
            utc=False,
        )
    except OSError as exc:
        logger.warning("File logging disabled: %s", exc)
    else:
        file_handler.setFormatter(formatter)
        file_handler.suffix = "%Y%m%d"
        file_handler.namer = lambda name: name.replace(".log.", ".log.")
        file_handler.extMatch = re.compile(r"^\d{8}$")
        logger.addHandler(file_handler)

    logger._disclosure_edge_configured = True
    return logger


logger = configure_logging()
