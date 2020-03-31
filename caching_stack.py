from typing import TypeVar, Tuple, Any

from caches import Cache, CacheObject
from filters import Filter


class CachingSystemSimulator:
    """
    A simulation of the entire caching stack
    """

    def __init__(self, filter_instance: Filter, cache_instance: Cache):
        self.filter_instance = filter_instance
        self.cache_instance = cache_instance

    def __repr__(self):
        return f"CachingSystemSimulator(filter={self.filter_instance}, cache={self.cache_instance.id})"

    @property
    def id(self):
        return f"{self.cache_instance}_{self.cache_instance.id}_{self.filter_instance}"

    def put(self, request):
        if self.filter_instance.should_filter(request):
            return False
        if not self.cache_instance.get(request):
            self.cache_instance.admit(request)
            return False
        return True

    def get(self, request) -> CacheObject:
        """

        :param request: traces.CacheRequest
        :return:
        """
        return self.cache_instance.get(request)
