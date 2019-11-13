from abc import ABC, abstractmethod
from enum import IntEnum
from typing import NewType
from collections import OrderedDict

from logger import log_error_too_large


class CacheState(IntEnum):
    PRE_WARMUP = 0
    POST_WARMUP = 1


class BaseCache(ABC):
    def __init__(self, capacity):
        self.capacity = capacity
        self._is_post_hydrate = False

    @abstractmethod
    def put(self, key, size):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @property
    def state(self):
        if self._is_post_hydrate:
            return CacheState.POST_WARMUP
        return CacheState.PRE_WARMUP


class LRUCache(BaseCache):
    def __init__(self, capacity):
        """
        [] capacity 5
        {} forward
        {} backward

        1 <-> 2 <-> 3 <-> 4 <->
        {

        }
        :param capacity:
        """
        super().__init__(capacity)
        self.curr_capacity = 0
        self.map = OrderedDict()

    def __str__(self):
        return "LRU"

    def put(self, key, size):
        # object feasible to store?
        if size > self.capacity:
            log_error_too_large(self.capacity, key, size)
            return False

        while self.curr_capacity + size > self.capacity:
            # Set flag to denote that cache has completed filling up
            self._is_post_hydrate = True
            _, popped_size = self.map.popitem(last=False)
            self.curr_capacity -= popped_size

        if key in self.map:
            self.map.move_to_end(key)
            self.curr_capacity -= self.map[key]
        self.map[key] = size
        self.curr_capacity += self.map[key]
        return True

    def get(self, key):
        if key not in self.map:
            return None
        self.map.move_to_end(key)
        return self.map[key]


Cache = NewType("Cache", BaseCache)
