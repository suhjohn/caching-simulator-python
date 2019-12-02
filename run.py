import json
import os

from caches import LRUCache, CacheState, initialize_cache
from caching_stack import SimulatedCachingStack
from filters import NullFilter
from logger import log_window
import settings
from traces import StringCacheTraceIterator, initialize_iterator, DEFAULT_TRACE_TYPE
from datetime import datetime
import argparse


class Simulation:
    def __init__(self, caching_stack, trace_iterator):
        self._trace_iterator = trace_iterator
        self._caching_stack = caching_stack

    @property
    def identifier(self):
        return f"{self._caching_stack.identifier}_{self._trace_iterator.trace_filename}"

    def run(self):
        no_warmup_miss_count = 0
        no_warmup_miss_byte = 0
        miss_count = 0
        miss_byte = 0
        current_trace_index = 0

        window_size = 1000000
        start_time = datetime.now()
        for request in self._trace_iterator:
            size = self._caching_stack.get(request)
            if size is None:
                no_warmup_miss_count += 1
                no_warmup_miss_byte += request.size
                if self._caching_stack.cache_instance.state == CacheState.POST_WARMUP:
                    miss_count += 1
                    miss_byte += request.size
                self._caching_stack.put(request)

            # log every window number of traces
            if current_trace_index != 0 and current_trace_index % window_size == 0:
                log_window(current_trace_index, self._trace_iterator,
                           no_warmup_miss_count, no_warmup_miss_byte)
            current_trace_index += 1
        end_time = datetime.now()

        res = {
            "cache_type": str(self._caching_stack.cache_instance),
            "cache_size": str(self._caching_stack.cache_instance.capacity),
            "trace_file": self._trace_iterator.trace_filename,
            "simulation_time": (end_time - start_time).total_seconds(),
            "simulation_timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "no_warmup_byte_miss_ratio": no_warmup_miss_byte / self._trace_iterator.total_size
        }
        return res


def run(cache_type, cache_size, file_path, trace_type, write_simulation_result):
    filter_instance = NullFilter()
    cache_instance = initialize_cache(cache_type, capacity=cache_size)
    caching_stack = SimulatedCachingStack(filter_instance, cache_instance)
    trace_iterator = initialize_iterator(trace_type, file_path)
    simulation = Simulation(caching_stack, trace_iterator)

    print(caching_stack.identifier)
    res = simulation.run()
    print(res)

    if write_simulation_result:
        if not os.path.exists(settings.SIMULATION_RESULT_DIRECTORY):
            os.makedirs(settings.SIMULATION_RESULT_DIRECTORY)
        with open(f"{settings.SIMULATION_RESULT_DIRECTORY}/"
                  f"{simulation.identifier}_{res['simulation_timestamp']}", "w") as f:
            json.dump(res, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cacheType')
    parser.add_argument('cacheSize', type=int)
    parser.add_argument('traceFile')
    parser.add_argument('--traceType', default=DEFAULT_TRACE_TYPE, dest='traceType')
    parser.add_argument('--filterType', dest='filterType')
    parser.add_argument('--filterArgs', dest='filterArgs')
    parser.add_argument('--writeSimResult', default=False, dest='writeSimResult', action='store_true')
    args = parser.parse_args()

    run(
        args.cacheType,
        args.cacheSize,
        args.traceFile,
        args.traceType,
        args.writeSimResult
    )
