import json
from collections import namedtuple
from typing import List

import caches

CacheTrace = namedtuple("CacheTrace", ['key', 'size'])
cache_cls_list: List[caches.Cache] = [
    caches.FIFOCache,
    caches.LRUCache,
    # caches.RandomCache
]


def run(trace_filename: str, cache_size: int):
    """
    cache_size in bytes
    :param trace_filename:
    :param cache_size:
    :return:
    """
    # with open(trace_filename) as f:
    #     data = json.load(f)
    data = [
        {"key": 1, "size": 10},
        {"key": 2, "size": 10},
        {"key": 3, "size": 10},
        {"key": 4, "size": 10},
        {"key": 5, "size": 10},
        {"key": 1, "size": 10},
        {"key": 2, "size": 10},
        {"key": 3, "size": 10},
        {"key": 4, "size": 10},
        {"key": 5, "size": 10},
        {"key": 6, "size": 10},
        {"key": 7, "size": 10},
        {"key": 8, "size": 10},
        {"key": 9, "size": 10},
        {"key": 10, "size": 10},
        {"key": 6, "size": 10},
        {"key": 7, "size": 10},
        {"key": 8, "size": 10},
        {"key": 9, "size": 10},
        {"key": 10, "size": 10},
    ]
    assert isinstance(data, list)
    cache_traces: List[CacheTrace] = []
    total_count = len(data)
    total_size = 0
    for cache_trace in data:
        cache_trace_obj = CacheTrace(**cache_trace)
        cache_traces.append(cache_trace_obj)
        total_size += cache_trace_obj.size

    for cache_cls in cache_cls_list:
        cache = cache_cls(cache_size)
        miss_count = 0
        miss_byte = 0
        for cache_trace in cache_traces:
            val = cache.get(cache_trace.key)
            if not val:
                miss_count += 1
                miss_byte += cache_trace.size
                cache.put(cache_trace.key, cache_trace.size)
        print(f"{str(cache)}")
        print(f"Object Miss Rate: {miss_count/total_count}")
        print(f"Byte Miss Rate: {miss_byte/total_size}")


if __name__ == "__main__":
    fn = "sample_cache_trace.json"
    run(fn, 50)
