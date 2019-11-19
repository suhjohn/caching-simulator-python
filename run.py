import json
import pstats
import sys

import io
import os

from caches import LRUCache, CacheState
from caching_stack import SimulatedCachingStack
from filters import NullFilter
from logger import log_window
import settings
from traces import parse_tr_line, CacheTraceIterator
from datetime import datetime
import argparse, cProfile


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
        for cache_trace in self._trace_iterator:
            size = self._caching_stack.get(cache_trace.key)
            if size is None:
                no_warmup_miss_count += 1
                no_warmup_miss_byte += cache_trace.size
                if self._caching_stack.cache_instance.state == CacheState.POST_WARMUP:
                    miss_count += 1
                    miss_byte += cache_trace.size
                self._caching_stack.put(cache_trace.key, cache_trace.size)

            # log every window number of traces
            if current_trace_index % window_size == 0:
                log_window(current_trace_index, self._trace_iterator, miss_count, miss_byte)
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
        print(res)
        return res


def run(file_path, cache_size, run_profiler, write_profiler_result, write_simulation_result):
    filter_instance = NullFilter()
    cache_instance = LRUCache(cache_size)
    caching_stack = SimulatedCachingStack(filter_instance, cache_instance)
    trace_iterator = CacheTraceIterator(file_path, parse_tr_line)
    simulation = Simulation(caching_stack, trace_iterator)

    if run_profiler:
        pr = cProfile.Profile()
        pr.enable()
        res = simulation.run()
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats()
        print(s.getvalue())
        if write_profiler_result:
            if not os.path.exists(settings.PROFILING_RESULT_DIRECTORY):
                os.makedirs(settings.PROFILING_RESULT_DIRECTORY)
            with open(f"{settings.PROFILING_RESULT_DIRECTORY}/"
                      f"{simulation.identifier}_{res['simulation_timestamp']}", "w") as f:
                f.write(s.getvalue())
    else:
        res = simulation.run()

    if write_simulation_result:
        if not os.path.exists(settings.SIMULATION_RESULT_DIRECTORY):
            os.makedirs(settings.SIMULATION_RESULT_DIRECTORY)
        with open(f"{settings.SIMULATION_RESULT_DIRECTORY}/"
                  f"{simulation.identifier}_{res['simulation_timestamp']}", "w") as f:
            json.dump(res, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('traceFile')
    parser.add_argument('cacheSize', type=int)
    parser.add_argument('--runProfiler', default=False, dest='runProfiler', action='store_true')
    parser.add_argument('--writeProfilerResult', default=False, dest='writeProfilerResult', action='store_true')
    parser.add_argument('--writeSimResult', default=False, dest='writeSimResult', action='store_true')
    args = parser.parse_args()

    run(args.traceFile,
        args.cacheSize,
        args.runProfiler,
        args.writeProfilerResult,
        args.writeSimResult
        )
