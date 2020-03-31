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
        miss_count = 0
        miss_byte = 0
        current_trace_index = 0
        start_time = datetime.now()

        for request in self._trace_iterator:
            cache_obj = self._simulator.get(request)
            if cache_obj is None:
                miss_count += 1
                miss_byte += request.size
                self._simulator.put(request)

            # # logs every window number of traces
            if current_trace_index != 0 and current_trace_index % self._ordinal_window == 0:
                log_window(self.execution_logger, current_trace_index,
                           self._trace_iterator, miss_byte)

            current_trace_index += 1
        end_time = datetime.now()

        res = {
            "cache_type": str(self._simulator.cache_instance),
            "cache_args": dict(self._simulator.cache_instance.args._asdict()),
            "cache_id": self._simulator.cache_instance.id,
            "cache_size": self._simulator.cache_instance.capacity,
            "filter_type": str(self._simulator.filter_instance),
            "filter_args": dict(self._simulator.filter_instance.args._asdict()),
            "trace_file": self._trace_iterator.trace_filename,
            "simulation_time": (end_time - start_time).total_seconds(),
            "simulation_timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "no_warmup_byte_miss_ratio": miss_byte / self._trace_iterator.total_size
        }
        return res


def run(cache_type, cache_size, file_path, trace_type, filter_type, result_identifier,
        log_eviction, ordinal_window, temporal_window):
    file_path = "./cache_traces/" + file_path
    filter_instance = initialize_filter(filter_type)
    cache_instance = initialize_cache(cache_type, cache_size)
    caching_stack = CachingSystem(filter_instance, cache_instance)
    trace_iterator = initialize_iterator(trace_type, file_path)
    simulation = Simulation(caching_stack, trace_iterator, ordinal_window, temporal_window)
    if log_eviction:
        eviction_logger = setup_logger(
            "eviction_logger",
            f"{settings.EVICTION_LOGGING_RESULT_DIRECTORY}/{simulation.id}_eviction.log"
        )
        cache_instance.set_eviction_logger(eviction_logger)
    execution_logger = setup_logger(
        "execution_logger",
        f"{settings.EXECUTION_LOGGING_RESULT_DIRECTORY}/{simulation.id}_execution.log"
    )
    simulation.set_execution_logger(execution_logger)
    res = simulation.run()
    if log_eviction:
        res['eviction_logging'] = True
    else:
        res['eviction_logging'] = False
    with open(f"{settings.SIMULATION_RESULT_DIRECTORY}/"
              f"{simulation.id}_{result_identifier}.json", "w") as f:
        json.dump(res, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cacheType')
    parser.add_argument('cacheSize', type=int)
    parser.add_argument('traceFile')
    parser.add_argument('--logEviction', default=False, type=bool)
    parser.add_argument('--temporalWindowSize', default=600, type=int)
    parser.add_argument('--ordinalWindowSize', default=100000, type=int)
    parser.add_argument('--traceType', default=DEFAULT_TRACE_TYPE, dest='traceType')
    parser.add_argument('--cacheArgs', dest='filterArgs')
    parser.add_argument('--filterType', default="Null", dest='filterType')
    parser.add_argument('--filterArgs', dest='filterArgs')
    parser.add_argument('--resultIdentifier', default="regular", dest='resultIdentifier')
    args = parser.parse_args()
    if not os.path.exists(settings.EVICTION_LOGGING_RESULT_DIRECTORY):
        os.makedirs(settings.EVICTION_LOGGING_RESULT_DIRECTORY)
    if not os.path.exists(settings.EXECUTION_LOGGING_RESULT_DIRECTORY):
        os.makedirs(settings.EXECUTION_LOGGING_RESULT_DIRECTORY)
    if not os.path.exists(settings.SIMULATION_RESULT_DIRECTORY):
        os.makedirs(settings.SIMULATION_RESULT_DIRECTORY)
    run(
        args.cacheType,
        args.cacheSize,
        args.traceFile,
        args.traceType,
        args.filterType,
        args.resultIdentifier,
        args.logEviction,
        args.ordinalWindowSize,
        args.temporalWindowSize,
    )
