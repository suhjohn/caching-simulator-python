import argparse

import os

import settings
from filters import BloomFilter, SetFilter, SetFilterArgs, BloomFilterArgs
from traces import initialize_iterator, DEFAULT_TRACE_TYPE


def run_test(trace_dir, trace_file, filter_args):
    trace_file_path = f"{trace_dir}/{trace_file}"
    bloom_filter = BloomFilter(BloomFilterArgs(**filter_args))
    set_filter = SetFilter(SetFilterArgs())
    trace_iterator = initialize_iterator(DEFAULT_TRACE_TYPE, trace_file_path)

    fp_count = 0
    for request in trace_iterator:
        fp_count += int(bloom_filter.should_filter(request) != set_filter.should_filter(request))

    print(fp_count / trace_iterator.total_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('traceFile')
    args = parser.parse_args()

    trace_dir = os.environ.get("TRACE_DIRECTORY", settings.TRACE_DIRECTORY)
    filter_args = settings.FILTER_ARGS
    run_test(trace_dir, args.traceFile, filter_args)
