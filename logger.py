from datetime import datetime
import logging


def log_window(logger, current_trace_index, trace_iterator, miss_bytes, total_bytes):
    logger.info(f"[{datetime.utcnow().isoformat()}] "
                f"[log_window] "
                f"{current_trace_index} "
                f"bmr: {miss_bytes / total_bytes} ")


def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file, mode='w')
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
