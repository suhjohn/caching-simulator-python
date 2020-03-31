import json
import logging
import os
import argparse
from datetime import datetime
import settings

from caching_system import CachingSystem
from caches import initialize_cache
from filters import initialize_filter

from logger import log_window, setup_logger
from traces import initialize_iterator, DEFAULT_TRACE_TYPE

TEMPORAL_FORMATS = {
    "s", "milli", "micro"
}


class Simulation:
    def __init__(self, caching_stack, trace_iterator,
                 ordinal_window=100000, temporal_window=600, temporal_format='s'):
        self._trace_iterator = trace_iterator
        self._simulator = caching_stack
        self._temporal_window = temporal_window
        self._ordinal_window = ordinal_window
        self._temporal_format = temporal_format
        self.execution_logger: logging.Logger = None
        assert self._temporal_format in TEMPORAL_FORMATS

    @property
    def id(self):
        return f"{self._simulator.id}_{self._trace_iterator.trace_filename}"

    def set_execution_logger(self, logger):
        self.execution_logger = logger

    def run(self):
        total_count = 0
        miss_count = 0
        total_bytes = 0
        miss_bytes = 0
        segment_total_count = 0
        segment_miss_count = 0
        segment_total_bytes = 0
        segment_miss_bytes = 0
        current_trace_index = 0
        start_time = datetime.now()

        for request in self._trace_iterator:
            # # logs every window number of traces
            if current_trace_index != 0 and current_trace_index % self._ordinal_window == 0:
                log_window(self.execution_logger, current_trace_index,
                           self._trace_iterator, segment_miss_bytes, segment_total_bytes)
                total_bytes += segment_total_bytes
                miss_bytes += segment_miss_bytes
                segment_total_count, segment_miss_count, segment_total_bytes, segment_miss_bytes = 0, 0, 0, 0

            total_bytes += request.size
            if self._simulator.get(request) is None:
                segment_miss_count += 1
                segment_miss_bytes += request.size
                self._simulator.put(request)
            segment_total_count += 1
            segment_total_bytes += request.size
            current_trace_index += 1
        end_time = datetime.now()

        res = {
            "cache_type": str(self._simulator.cache_instance),
            "cache_args": dict(self._simulator.cache_instance.args._asdict()),
            "cache_id": self._simulator.cache_instance.id,
            "cache_size": self._simulator.cache_instance.capacity,
            "filter_type": str(self._simulator.filter_instance),
            "filter_args": dict(self._simulator.filter_instance.args._asdict()),
            "filter_id": self._simulator.filter_instance.id,
            "trace_file": self._trace_iterator.trace_filename,
            "simulation_time": (end_time - start_time).total_seconds(),
            "simulation_timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "no_warmup_byte_miss_ratio": miss_bytes / self._trace_iterator.total_size
        }
        return res


def run(cache_type, cache_size, file_path, trace_type, filter_type, filter_args, result_identifier,
        log_eviction, ordinal_window, temporal_window,
        eviction_log_dir, execution_log_dir, simulation_res_dir):
    file_path = "./cache_traces/" + file_path
    filter_instance = initialize_filter(filter_type, **filter_args)
    cache_instance = initialize_cache(cache_type, cache_size)
    caching_stack = CachingSystem(filter_instance, cache_instance)
    trace_iterator = initialize_iterator(trace_type, file_path)
    simulation = Simulation(caching_stack, trace_iterator, ordinal_window, temporal_window)
    if log_eviction:
        eviction_logger = setup_logger(
            "eviction_logger",
            f"{eviction_log_dir}/{simulation.id}_eviction.log"
        )
        cache_instance.set_eviction_logger(eviction_logger)
    execution_logger = setup_logger(
        "execution_logger",
        f"{execution_log_dir}/{simulation.id}_execution.log"
    )
    simulation.set_execution_logger(execution_logger)
    res = simulation.run()
    if log_eviction:
        res['eviction_logging'] = True
    else:
        res['eviction_logging'] = False
    with open(f"{simulation_res_dir}/{simulation.id}_{result_identifier}.json", "w") as f:
        json.dump(res, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cacheType')
    parser.add_argument('cacheSize', type=int)
    parser.add_argument('traceFile')
    parser.add_argument('--logEviction', default=False, type=bool)
    parser.add_argument('--temporalWindowSize', default=600, type=int)
    parser.add_argument('--ordinalWindowSize', default=1000000, type=int)
    parser.add_argument('--traceType', default=DEFAULT_TRACE_TYPE, dest='traceType')
    parser.add_argument('--filterType', default="Null", dest='filterType')
    parser.add_argument('--resultIdentifier', default="regular", dest='resultIdentifier')
    args = parser.parse_args()

    eviction_log_dir = os.environ["EVICTION_LOGGING_RESULT_DIRECTORY"] or settings.EVICTION_LOGGING_RESULT_DIRECTORY
    execution_log_dir = os.environ["EXECUTION_LOGGING_RESULT_DIRECTORY"] or settings.EXECUTION_LOGGING_RESULT_DIRECTORY
    simulation_res_dir = os.environ["SIMULATION_RESULT_DIRECTORY"] or settings.SIMULATION_RESULT_DIRECTORY
    if not os.path.exists(eviction_log_dir):
        os.makedirs(eviction_log_dir)
    if not os.path.exists(execution_log_dir):
        os.makedirs(execution_log_dir)
    if not os.path.exists(simulation_res_dir):
        os.makedirs(simulation_res_dir)
    filter_args = settings.FILTER_ARGS
    run(
        args.cacheType,
        args.cacheSize,
        args.traceFile,
        args.traceType,
        args.filterType,
        filter_args,
        args.resultIdentifier,
        args.logEviction,
        args.ordinalWindowSize,
        args.temporalWindowSize,
        eviction_log_dir, execution_log_dir, simulation_res_dir
    )
