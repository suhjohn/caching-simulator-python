from typing import List

import constants
from caches import LRUCache
from caching_stack import SimulatedCachingStack
from filters import NullFilter, BloomFilter
from traces import TraceInfo, parse_tr, Trace, FullCacheTrace


def run_simulation(trace_info: TraceInfo, caching_stack: SimulatedCachingStack):
    miss_count = 0
    miss_byte = 0

    cache_traces: List[Trace] = trace_info.read()
    for cache_trace in cache_traces:
        val, size = caching_stack.get(cache_trace.key)
        if not val and size == 0:
            miss_count += 1
            miss_byte += cache_trace.size
            caching_stack.put(cache_trace.key, None, cache_trace.size)

    print(f"{caching_stack}")
    print(f"Object Miss Rate: {miss_count/trace_info.total_count}")
    print(f"Byte Miss Rate: {miss_byte/trace_info.total_size}")


def run():
    """
    """
    capacity = constants.CACHE_CAPACITY
    bloom_filter_m = constants.BLOOM_FILTER_M
    bloom_filter_k = constants.BLOOM_FILTER_K
    filename = "cache_traces/memc_200m_100mb_100000.tr"
    filter_instance = NullFilter()
    filter_instance = BloomFilter(bloom_filter_m, bloom_filter_k)
    cache_instance = LRUCache(capacity)

    caching_stack = SimulatedCachingStack(filter_instance, cache_instance)
    trace_info = FullCacheTrace(filename, parse_tr)
    run_simulation(trace_info, caching_stack)


if __name__ == "__main__":
    run()
