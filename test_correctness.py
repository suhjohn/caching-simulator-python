import caches
from test_utils import cache_info_map, TraceInfo


def assert_exists(cache_obj: caches.Cache, key, should_exist: bool):
    value, size = cache_obj.get(key)
    if should_exist:
        assert value is not None or size is not None
    else:
        assert value is None and size is None


def assert_size(cache_obj: caches.Cache, key, expected_size: int):
    value, size = cache_obj.get(key)
    assert size == expected_size


def execute_traces(cache_cls: caches.Cache, trace_info: TraceInfo):
    cache_obj: caches.Cache = cache_cls(trace_info.cache_size)
    for trace in trace_info.traces:
        key = trace["key"]
        val, size = cache_obj.get(key)
        if val is None and size is None:
            # miss
            pass
        cache_obj.put(key, None, trace["size"])
    return cache_obj


def assert_expected_responses(cache_snapshot: caches.Cache, trace_info: TraceInfo):
    for expected_response in trace_info.expected_responses:
        if expected_response["event"] == "exists":
            assert_exists(cache_snapshot, expected_response["key"], expected_response["value"])
        elif expected_response["event"] == "get_size":
            assert_size(cache_snapshot, expected_response["key"], expected_response["value"])


def test_lru():
    cache_info = cache_info_map["lru"]
    cache_cls, trace_infos = cache_info["cache_cls"], cache_info["trace_infos"]
    for trace_info in cache_info["trace_infos"]:
        cache_snapshot = execute_traces(cache_cls, trace_info)
        assert_expected_responses(cache_snapshot, trace_info)
