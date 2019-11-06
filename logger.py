from datetime import datetime


def log_window(current_trace_index, trace_iterator, miss_count, miss_byte):
    print(f"[{datetime.utcnow().isoformat()}] "
          f"[log_window] "
          f"{current_trace_index} "
          f"{miss_count}/{trace_iterator.total_count} "
          f"{miss_byte}/{trace_iterator.total_size}", flush=True)


def log_error_too_large(capacity, key, size):
    print(f"[{datetime.utcnow().isoformat()}] "
          f"[error_object_too_large] "
          f"{capacity} "
          f"{key} "
          f"{size}", flush=True)
