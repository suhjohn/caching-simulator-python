import argparse
import json

import os

import settings
from caches import initialize_cache
from caching_system import CachingSystem
from filters import initialize_filter, SetFilter, SetFilterArgs, BloomFilter, BloomFilterArgs
from simulation import Simulation
from traces import DEFAULT_TRACE_TYPE, initialize_iterator, CacheRequest


class Callback:
    def __init__(self):
        self.hit = {}
        self.total_hit_count = 0
        self.total_byte_hit_size = 0

    def on_hit_callback(self, request: CacheRequest):
        self.hit[request.index] = request.size
        self.total_hit_count += 1
        self.total_byte_hit_size += request.size


def run_simulation(cache_type, cache_size, trace_type, file_path, n, result_dir):
    file_path = f"{trace_dir}/{file_path}"
    set_filter_instance = SetFilter(SetFilterArgs())
    cache_instance_1 = initialize_cache(cache_type, cache_size)
    set_filter_caching_stack = CachingSystem(set_filter_instance, cache_instance_1)

    bloom_filter_instance = BloomFilter(BloomFilterArgs(n))
    cache_instance_2 = initialize_cache(cache_type, cache_size)
    bloom_filter_caching_stack = CachingSystem(bloom_filter_instance, cache_instance_2)

    trace_iterator_1 = initialize_iterator(trace_type, file_path)
    trace_iterator_2 = initialize_iterator(trace_type, file_path)
    set_callback_container = Callback()
    bloom_callback_container = Callback()
    set_simulation = Simulation(set_filter_caching_stack, trace_iterator_1, 1000000, 600,
                                on_hit_callback=set_callback_container.on_hit_callback)
    bloom_simulation = Simulation(bloom_filter_caching_stack, trace_iterator_2, 1000000, 600,
                                  on_hit_callback=bloom_callback_container.on_hit_callback)

    trace_iterator_3 = iter(initialize_iterator(trace_type, file_path))
    tracking_set_filter = SetFilter(SetFilterArgs())
    # when the second time a key is seen, if the caching_system.get returns True, it's a surprise hit.
    key_seen_second_time_index = set()
    while True:
        set_sim_tick = set_simulation.tick()
        bloom_sim_tick = bloom_simulation.tick()
        try:
            req = next(trace_iterator_3)
            tracking_set_filter.should_filter(req)
            tracking_filter_tick = True
            key_seen_second_time_index.add(req.index)
        except:
            tracking_filter_tick = False
        if not set_sim_tick and not bloom_sim_tick and not tracking_filter_tick:
            break
        elif set_sim_tick != bloom_sim_tick or set_sim_tick != tracking_filter_tick:
            raise Exception("This should not happen since both simulations should end the same time. ")
    bloom_hit_request_indexes = set(bloom_callback_container.hit.keys())
    set_hit_request_indexes = set(set_callback_container.hit.keys())
    unique_bloom_hit_request_indexes = bloom_hit_request_indexes - set_hit_request_indexes
    unique_set_hit_request_indexes = set_hit_request_indexes - bloom_hit_request_indexes
    common_hit_request_indexes = bloom_hit_request_indexes.intersection(set_hit_request_indexes)
    bloom_at_second_hit_keys = unique_bloom_hit_request_indexes.intersection(key_seen_second_time_index)
    bloom_at_second_hit_bytes = sum([bloom_callback_container.hit[k] for k in bloom_at_second_hit_keys])
    res = {
        "total_bloom_hit_bytes": bloom_callback_container.total_byte_hit_size,
        "total_set_hit_bytes": set_callback_container.total_byte_hit_size,
        "unique_bloom_hit_request_count": len(unique_bloom_hit_request_indexes),
        "unique_set_hit_request_count": len(unique_set_hit_request_indexes),
        "common_hit_request_count": len(common_hit_request_indexes),
        "bloom_at_second_hit_request_count": len(bloom_at_second_hit_keys),
        "bloom_at_second_hit_request_bytes": bloom_at_second_hit_bytes,
    }

    output_filepath = f"{result_dir}/{file_path}_bloomsetdiff_{n}.json"
    with open(f"{output_filepath}", "w") as f:
        json.dump(res, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cacheType')
    parser.add_argument('cacheSize', type=int)
    parser.add_argument('traceFile')
    parser.add_argument('--traceType', default=DEFAULT_TRACE_TYPE, dest='traceType')
    parser.add_argument('--n', dest='n')
    trace_dir = os.environ.get("TRACE_DIRECTORY", settings.TRACE_DIRECTORY)
    result_dir = os.environ["BLOOM_SET_DIFF_RESULT_DIRECTORY"]

    args = parser.parse_args()
    run_simulation(
        args.cacheType, args.cacheSize, args.traceType, args.traceFile, args.n, result_dir
    )
