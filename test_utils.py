import json
import os

import caches
from traces import CacheRequest

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
    def __init__(self, filepath, cache_size, traces, expected_responses):
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
        self.filepath = filepath
        self.cache_size = cache_size
        self.traces = traces
        self.expected_responses = expected_responses


def collect_trace_infos(dir_path):
    test_trace_filepaths = []
    try:
        for fp in os.listdir(dir_path):
            full_fp = f"{dir_path}/{fp}"
            if os.path.isfile(full_fp) and fp.split(".")[-1] == "json":
                test_trace_filepaths.append(full_fp)
    except FileNotFoundError as e:
        print(e)
        return []

    trace_infos = []
    for fp in test_trace_filepaths:
        with open(fp, 'r') as f:
            loaded_data = json.load(f)
        trace_infos.append(
            TraceInfo(
                filepath=fp,
                cache_size=loaded_data["size"],
                traces=loaded_data["traces"],
                expected_responses=loaded_data["expectedResponses"]
            )
        )
    return trace_infos


correctness_base_fp = "./cache_traces/test_correctness"
common_trace_infos = collect_trace_infos(correctness_base_fp)
cache_info_map = {
    "lru": {
        "cache_cls": caches.LRUCache,
        "trace_infos": common_trace_infos + collect_trace_infos(f"{correctness_base_fp}/lru")
    },
}


def _assert_exists(cache_obj: caches.Cache, key, should_exist: bool):
    size = cache_obj.get(CacheRequest(key, 0, 0, 0))
    if should_exist:
        assert size is not None
    else:
        assert size is None


def _assert_size(cache_obj: caches.Cache, key, expected_size: int):
    obj = cache_obj.get(CacheRequest(key, 0, 0, 0))
    assert obj.size == expected_size, f"{obj.size} != {expected_size}"


def execute_traces(cache_cls: caches.Cache, trace_info: TraceInfo):
    cache_obj = cache_cls(trace_info.cache_size, caches.LRUArgs())
    print(cache_obj)
    for trace in trace_info.traces:
        print(trace)
        req = CacheRequest(trace["key"], trace["size"], 0, 0)
        size = cache_obj.get(req)
        if size is None:
            # miss
            pass
        cache_obj.admit(req)
    return cache_obj


def assert_expected_responses(cache_snapshot: caches.Cache, trace_info: TraceInfo):
    for expected_response in trace_info.expected_responses:
        event, key, value = expected_response["event"], expected_response["key"], expected_response["value"]
        assertion_fn = None
        if event == "exists":
            assertion_fn = _assert_exists
        elif event == "get_size":
            assertion_fn = _assert_size
        else:
            raise Exception
        assertion_fn(cache_snapshot, key, value)
