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

    @property
    def identifier(self):
        return f"{self.filter_instance}_{self.cache_instance}"

    def put(self, request):
        if self.filter_instance.should_filter(request):
            return False
        self.cache_instance.put(request)
        return True

    def get(self, request):
        """

        :param request: traces.CacheRequest
        :return:
        """
        if self.filter_instance.should_filter(request):
            return None
        return self.cache_instance.get(request)
