from typing import TypeVar, Tuple, Any

from caches import Cache
from filters import Filter


class SimulatedCachingStack:
    """
    A simulation of the entire caching stack
    """

    def __init__(self, filter_instance: Filter, cache_instance: Cache):
        self.filter_instance = filter_instance
        self.cache_instance = cache_instance

    def __repr__(self):
        return f"SimulatedCachingStack(filter={self.filter_instance}, cache={self.cache_instance})"

    def put(self, key: int, value: Any, size: int):
        if self.filter_instance.should_filter(key, value, size):
            return False
        self.cache_instance.put(key, value, size)
        return True

    def get(self, key: int) -> Tuple[Any, int]:
        if self.filter_instance.should_filter(key, None, 0):
            return None, 0
        return self.cache_instance.get(key)
