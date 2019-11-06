import sys

import constants
from caches import LRUCache, CacheState
from caching_stack import SimulatedCachingStack
from filters import NullFilter
from traces import parse_tr_line, Trace, CacheTraceIterator


def run_simulation(trace_iterator: CacheTraceIterator, caching_stack: SimulatedCachingStack):
    miss_count = 0
    miss_byte = 0

    for cache_trace in trace_iterator:
        size = caching_stack.get(cache_trace.key)
        if size is None and caching_stack.cache_instance.state == CacheState.POST_WARMUP:
            miss_count += 1
            miss_byte += cache_trace.size
            caching_stack.put(cache_trace.key, cache_trace.size)

    print(f"{caching_stack}")
    print(f"Object Miss Rate: {miss_count/trace_iterator.total_count}")
    print(f"Byte Miss Rate: {miss_byte/trace_iterator.total_size}")
    print(trace_iterator.total_count)
    print(trace_iterator.total_size)


def run(filter_instance, cache_instance, filepath):
    caching_stack = SimulatedCachingStack(filter_instance, cache_instance)
    trace_iterator = CacheTraceIterator(filepath, parse_tr_line)
    run_simulation(trace_iterator, caching_stack)


if __name__ == "__main__":
    args = sys.argv[1:]
    filepath = args[0]
    capacity = args[1]
    filter_instance = NullFilter()
    cache_instance = LRUCache(capacity)
    run(filter_instance, cache_instance, filepath)
