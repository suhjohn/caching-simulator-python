from abc import ABC, abstractmethod
from typing import NewType


class IFilter(ABC):
    @abstractmethod
    def should_filter(self, key, size):
        pass


class NullFilter(IFilter):
    def should_filter(self, key, size):
        return False


class BypassFilter(IFilter):
    def __init__(self, threshold_size):
        self.threshold_size = threshold_size

    def should_filter(self, key, size):
        return size > self.threshold_size


class BloomFilter(IFilter):
    """
    Bloom Filter implementation.
    Assumes that the key is integer.
    """

    def __init__(self, m, k):
        """
         m: int, size of the filter
         k: int, number of hash fnctions to compute
        """
        self._filters = [set() for _ in range(2)]
        self._m = m

    def should_filter(self, key, size) -> bool:
        if self.exists(key):
            return False
        self.put(key)
        return True

    def put(self, key):
        """ insert the pair (key,value) in the database """
        pass

    def exists(self, key):
        """ check if key is exists in the filter
            using the filter mechanism """
        if not len(self._filters[self._current_filter]) < self._m:
            if len(self._filters[1 - self._current_filter]) > 0:
                self._filters[1 - self._current_filter] = set()
            self._current_filter = 1 - self._current_filter

        return False


Filter = NewType("Filter", IFilter)
