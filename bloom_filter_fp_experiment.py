import argparse
import json

import os

from filters import BloomFilter, SetFilter, SetFilterArgs, BloomFilterArgs
from traces import initialize_iterator, DEFAULT_TRACE_TYPE


def run_test(trace_dir, trace_file, n, result_dir):
    trace_file_path = f"{trace_dir}/{trace_file}"
    bloom_filter = BloomFilter(BloomFilterArgs(n))
    set_filter = SetFilter(SetFilterArgs())
    trace_iterator = initialize_iterator(DEFAULT_TRACE_TYPE, trace_file_path)

    bloom_filter_false_positives = set()  # contains false positives for one hit wonders and non one hit wonders
    unique_keys = set()
    false_positive_count = 0
    non_one_hit_wonder = set()
    non_one_hit_wonder_count = 0
    # Set filter filters both one hit wonders and non one hit wonders on their initial request.
    # If Bloom filter has a false positive, the false positive can be a one hit wonder or a non one hit wonder.
    #   since at the time of filtering, the request may have been the first for the non one hit wonder that passes.

    for request in trace_iterator:
        should_filter_set = set_filter.should_filter(request)
        should_filter_bloom = bloom_filter.should_filter(request)
        if not should_filter_bloom and should_filter_set:
            bloom_filter_false_positives.add(request.key)
            false_positive_count += 1

        if not should_filter_set:  # at least second time key is seen
            non_one_hit_wonder.add(request.key)
            non_one_hit_wonder_count += 1
        unique_keys.add(request.key)

    true_negative_count = len(unique_keys)  # The only true negatives are the initial time a key is seen.
    # false_positive_object_rate: fp unique objects / (fp unique objects + tn unique objects).
    #                             tn unique objects is the entire unique object set.
    # false_positive_request_rate: shows the false positive rate of bloom filter compared to using a hash set
    # false_positive_unique_object_count: total count of unique objects
    #                                     that were falsely identified as existing in the filter
    res = {
        "trace_file": trace_iterator.trace_filename,
        "n": n,
        "total_request_count": trace_iterator.total_count,
        "total_object_count": len(unique_keys),
        "one_hit_wonder_object_count": len(unique_keys) - len(non_one_hit_wonder),
        "non_one_hit_wonder_request_count": non_one_hit_wonder_count,
        "non_one_hit_wonder_object_count": len(non_one_hit_wonder),
        "false_positive_request_count": false_positive_count,
        "false_positive_object_count": len(bloom_filter_false_positives),  # comparison to set
        "false_positive_request_rate": false_positive_count / (false_positive_count + true_negative_count),
        "false_positive_object_rate": len(bloom_filter_false_positives) / (
                len(bloom_filter_false_positives) + true_negative_count),
        "one_hit_wonder_pass_object_count": len(bloom_filter_false_positives) - len(
            bloom_filter_false_positives.intersection(non_one_hit_wonder)),
        "non_one_hit_wonder_pass_object_count": len(bloom_filter_false_positives.intersection(non_one_hit_wonder)),
    }

    output_filepath = f"{result_dir}/{trace_file}_bloomfp_{n}.json"
    with open(f"{output_filepath}", "w") as f:
        json.dump(res, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('traceFile')
    parser.add_argument('n')
    args = parser.parse_args()

    trace_dir = os.environ["TRACE_DIRECTORY"]
    result_dir = os.environ["BLOOM_FP_RESULT_DIRECTORY"]

    run_test(trace_dir, args.traceFile, int(args.n), result_dir)
