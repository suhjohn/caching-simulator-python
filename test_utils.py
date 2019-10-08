import json
import os

import caches

__all__ = [
    "cache_info_map",
    "TraceInfo"
]

_expected_response_events = {
    "exists", "get_size"
}
_expected_response_event_type = {
    "exists": bool,
    "get_size": int
}


class TraceInfo:
    def __init__(self, cache_size, traces, expected_responses):
        assert isinstance(cache_size, int)
        for trace in traces:
            assert isinstance(trace, dict)
            assert {"key", "size"} == set(trace.keys()), trace
            assert isinstance(trace["key"], int)
            assert isinstance(trace["size"], int)
        for expected_response in expected_responses:
            assert isinstance(expected_response, dict)
            assert {"key", "event", "value"} == set(expected_response.keys())
            assert isinstance(expected_response["key"], int)
            assert expected_response["event"] in _expected_response_events
            assert isinstance(expected_response["value"],
                              _expected_response_event_type[expected_response["event"]])
        self.cache_size = cache_size
        self.traces = traces
        self.expected_responses = expected_responses


def collect_trace_infos(dir_path):
    test_trace_filepaths = []
    try:
        for fp in os.listdir(dir_path):
            full_fp = f"{dir_path}/{fp}"
            if os.path.isfile(full_fp):
                test_trace_filepaths.append(full_fp)
    except FileNotFoundError as e:
        print(e)
        return []

    trace_infos = []
    for fp in test_trace_filepaths:
        with open(fp) as f:
            loaded_data = json.load(f)
        trace_infos.append(
            TraceInfo(
                cache_size=loaded_data["size"],
                traces=loaded_data["traces"],
                expected_responses=loaded_data["expectedResponses"]
            )
        )
    return trace_infos


correctness_base_fp = "./cache_traces/test_correctness"
common_trace_infos = collect_trace_infos(correctness_base_fp)

cache_info_map = {
    "fifo": {
        "cache_cls": caches.FIFOCache,
        "trace_infos": common_trace_infos + collect_trace_infos(f"{correctness_base_fp}/fifo")
    },
    "lru": {
        "cache_cls": caches.LRUCache,
        "trace_infos": common_trace_infos + collect_trace_infos(f"{correctness_base_fp}/lru")
    }
}
