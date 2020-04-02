import logging
from datetime import datetime

from logger import log_window
from traces import CacheRequest

TEMPORAL_FORMATS = {
    "s", "milli", "micro"
}


class SegmentStatistics:
    def __init__(self):
        self.segment_total_count_list = []
        self.segment_total_bytes_list = []
        self.segment_miss_count_list = []
        self.segment_miss_bytes_list = []
        self.segment_total_count = 0
        self.segment_miss_count = 0
        self.segment_total_bytes = 0
        self.segment_miss_bytes = 0

    def record_segment(self):
        self.segment_total_count_list.append(self.segment_total_count)
        self.segment_total_bytes_list.append(self.segment_total_bytes)
        self.segment_miss_count_list.append(self.segment_miss_count)
        self.segment_miss_bytes_list.append(self.segment_miss_bytes)
        self.segment_total_count = 0
        self.segment_miss_count = 0
        self.segment_total_bytes = 0
        self.segment_miss_bytes = 0

    def update_miss(self, request):
        self.segment_miss_count += 1
        self.segment_miss_bytes += request.size

    def update_req(self, request):
        self.segment_total_count += 1
        self.segment_total_bytes += request.size

    def curr_bmr(self):
        return self.segment_miss_bytes / self.segment_total_bytes

    def curr_omr(self):
        return self.segment_miss_count / self.segment_total_count

    def bmr(self, warmup=0):
        assert 0 <= warmup < 100
        start_index = int(len(self.segment_total_count_list) * warmup / 100)
        return sum(self.segment_miss_bytes_list[start_index:]) / sum(self.segment_total_bytes_list[start_index:])

    def omr(self, warmup=0):
        assert 0 <= warmup < 100
        start_index = int(len(self.segment_total_count_list) * warmup / 100)
        return sum(self.segment_miss_count_list[start_index:]) / sum(self.segment_total_count_list[start_index:])


def do_nothing(request: CacheRequest):
    pass


class Simulation:
    def __init__(self, caching_stack, trace_iterator,
                 ordinal_window=100000, temporal_window=600,
                 temporal_format='s', on_miss_callback=do_nothing, on_hit_callback=do_nothing):
        self._trace_iterator = trace_iterator
        self._simulator = caching_stack
        self._temporal_window = temporal_window
        self._ordinal_window = ordinal_window
        self._temporal_format = temporal_format
        self._execution_logger: logging.Logger = None
        self.on_miss_callback = on_miss_callback
        self.on_hit_callback = on_hit_callback
        self._segment_statistics = SegmentStatistics()
        self._curr_trace_index = 0
        assert self._temporal_format in TEMPORAL_FORMATS

    @property
    def id(self):
        return f"{self._simulator.id}_{self._trace_iterator.trace_filename}"

    def set_execution_logger(self, logger):
        self._execution_logger = logger

    def get_state(self):
        return {
            "cache_type": str(self._simulator.cache_instance),
            "cache_args": dict(self._simulator.cache_instance.args._asdict()),
            "cache_id": self._simulator.cache_instance.id,
            "cache_size": self._simulator.cache_instance.capacity,
            "filter_type": str(self._simulator.filter_instance),
            "filter_args": dict(self._simulator.filter_instance.args._asdict()),
            "filter_id": self._simulator.filter_instance.id,
            "trace_file": self._trace_iterator.trace_filename,
            "no_warmup_byte_miss_ratio": self._segment_statistics.bmr(),
            "segment_stats": {
                "segment_total_count": self._segment_statistics.segment_total_count_list,
                "segment_total_bytes": self._segment_statistics.segment_total_bytes_list,
                "segment_miss_count": self._segment_statistics.segment_miss_count_list,
                "segment_miss_bytes": self._segment_statistics.segment_miss_bytes_list,
            },
            "20p_warmup_bmr": self._segment_statistics.bmr(20),
            "50p_warmup_bmr": self._segment_statistics.bmr(50),
            "20p_warmup_omr": self._segment_statistics.omr(20),
            "50p_warmup_omr": self._segment_statistics.omr(50),
        }

    def tick(self):
        trace_iterator = iter(self._trace_iterator)
        try:
            request = next(trace_iterator)
            if self._simulator.get(request) is None:
                self._segment_statistics.update_miss(request)
                self._simulator.put(request)
                self.on_miss_callback(request)
            else:
                self.on_hit_callback(request)
            self._segment_statistics.update_req(request)
            if self._curr_trace_index != 0 and self._curr_trace_index % self._ordinal_window == 0:
                log_window(self._execution_logger, self._curr_trace_index,
                           self._trace_iterator, self._segment_statistics.curr_bmr(),
                           self._segment_statistics.curr_omr())
                self._segment_statistics.record_segment()
            self._curr_trace_index += 1
            return True
        except StopIteration:
            self._segment_statistics.record_segment()
            return False

    def run(self):
        start_time = datetime.now()
        while self.tick():
            pass
        end_time = datetime.now()
        res = self.get_state()
        res["simulation_time"] = (end_time - start_time).total_seconds()
        res["simulation_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        return res
