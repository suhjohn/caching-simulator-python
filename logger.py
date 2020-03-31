from datetime import datetime
import logging


def log_window(logger, current_trace_index, trace_iterator, miss_byte):
    logger.info(f"[{datetime.utcnow().isoformat()}] "
                f"[log_window] "
                f"{current_trace_index} "
                f"bmr: {miss_byte / trace_iterator.total_size} ")


def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
