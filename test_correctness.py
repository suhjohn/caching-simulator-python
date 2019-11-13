import caches
from test_utils import cache_info_map, TraceInfo, execute_traces, assert_expected_responses


def test_lru():
    cache_info = cache_info_map["lru"]
    cache_cls, trace_infos = cache_info["cache_cls"], cache_info["trace_infos"]
    for trace_info in cache_info["trace_infos"]:
        cache_snapshot = execute_traces(cache_cls, trace_info)
        assert_expected_responses(cache_snapshot, trace_info)
