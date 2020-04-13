import hashlib
import json
import logging
import os
import argparse
from datetime import datetime
from json import JSONDecodeError
from urllib import parse

from caching_system import CachingSystem
from caches import initialize_cache
from filters import initialize_filter

from logger import log_window, setup_logger
from simulation import Simulation
from traces import initialize_iterator, DEFAULT_TRACE_TYPE


def run(cache_type, cache_size, file_path, trace_type, filter_type, filter_args, result_identifier,
        log_eviction, ordinal_window, temporal_window,
        trace_dir, eviction_log_dir, execution_log_dir, simulation_res_dir):
    file_path = f"{trace_dir}/{file_path}"
    filter_instance = initialize_filter(filter_type, **filter_args)
    cache_instance = initialize_cache(cache_type, cache_size)
    caching_stack = CachingSystem(filter_instance, cache_instance)
    trace_iterator = initialize_iterator(trace_type, file_path)
    simulation = Simulation(caching_stack, trace_iterator, ordinal_window, temporal_window)

    h = hashlib.blake2s(digest_size=16)
    h.update(f"{simulation.id}_{result_identifier}".encode())
    filename = h.hexdigest()
    if log_eviction:
        eviction_logger = setup_logger(
            "eviction_logger",
            f"{eviction_log_dir}/{filename}.log"
        )
        cache_instance.set_eviction_logger(eviction_logger)
    execution_logger = setup_logger(
        "execution_logger",
        f"{execution_log_dir}/{filename}.log"
    )
    simulation.set_execution_logger(execution_logger)
    res = simulation.run()
    if log_eviction:
        res['eviction_logging'] = True
    else:
        res['eviction_logging'] = False
    
    with open(f"{simulation_res_dir}/{filename}.json", "w") as f:
        json.dump(res, f, sort_keys=True, indent=4)
    print(res)


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
    parser.add_argument('--filterArgs', default="{}")
    parser.add_argument('--resultIdentifier', default="regular", dest='resultIdentifier')

    args = parser.parse_args()

    trace_dir = os.environ["TRACE_DIRECTORY"]
    eviction_log_dir = os.environ["EVICTION_LOGGING_RESULT_DIRECTORY"]
    execution_log_dir = os.environ["EXECUTION_LOGGING_RESULT_DIRECTORY"]
    simulation_res_dir = os.environ["SIMULATION_RESULT_DIRECTORY"]
    if not os.path.exists(eviction_log_dir):
        os.makedirs(eviction_log_dir)
    if not os.path.exists(execution_log_dir):
        os.makedirs(execution_log_dir)
    if not os.path.exists(simulation_res_dir):
        os.makedirs(simulation_res_dir)

    try:
        filter_args = json.loads(args.filterArgs)
    except JSONDecodeError:
        filter_args = json.loads(parse.unquote(args.filterArgs))
    except:
        raise
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
        trace_dir, eviction_log_dir, execution_log_dir, simulation_res_dir
    )
