from abc import ABC, abstractmethod
from typing import NewType

import xxhash


def get_hash(string: str) -> int:
    return xxhash.xxh32(string).intdigest()


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
    Bloom Filter
    Adapted From: https://glowingpython.blogspot.com/2013/01/bloom-filter.html
    """

    def __init__(self, m: int, k: int):
        """
         m, size of the vector
         k, number of hash fnctions to compute
        """
        self.m = m
        self.vector = [0] * m
        self.k = k
        self.hash_fun = get_hash
        self.false_positive = 0

    def should_filter(self, key, size) -> bool:
        if self.exists(key):
            return False
        self.put(key)
        return True

    def put(self, key):
        """ insert the pair (key,value) in the database """
        for i in range(self.k):
            self.vector[self.hash_fun(str(key) + str(i)) % self.m] = 1

    def exists(self, key):
        """ check if key is exists in the filter
            using the filter mechanism """
        for i in range(self.k):
            if self.vector[self.hash_fun(str(key) + str(i)) % self.m] == 0:
                return False  # the key doesn't exist
        return True  # the key can be in the data set


Filter = NewType("Filter", IFilter)
