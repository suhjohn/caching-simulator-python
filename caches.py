from abc import ABC, abstractmethod
from enum import IntEnum
from typing import NewType
from collections import OrderedDict

from logger import log_error_too_large
from traces import CacheRequest


class CacheState(IntEnum):
    PRE_WARMUP = 0
    POST_WARMUP = 1


class BaseCache(ABC):
    def __init__(self, capacity):
        self.capacity = capacity
        self.curr_capacity = 0

    @property
    @abstractmethod
    def is_post_hydrate(self):
        pass

    @abstractmethod
    def put(self, request):
        """

        :param request: traces.CacheRequest
        :return:
        """
        pass

    @abstractmethod
    def get(self, request):
        """
        Returns the size of the key if the object exists in cache.
        Otherwise, returns None.

        :param request: traces.CacheRequest
        :return:
        """
        pass

    @property
    def state(self):
        if self.is_post_hydrate:
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
        self._is_post_hydrate = False
        self.map = OrderedDict()

    def __repr__(self):
        return "LRU"

    def peek_head(self):
        return next(self.map.__iter__())

    @property
    def is_post_hydrate(self):
        return self._is_post_hydrate

    def evict(self, key):
        if key in self.map:
            self.curr_capacity -= self.map[key]
            del self.map[key]

    def evict_return(self):
        lru_key = next(reversed(self.map))
        lru_size = self.map[lru_key]
        req = CacheRequest(lru_key, lru_size)
        self.evict(lru_key)
        return req

    def put(self, request):
        # object feasible to store?
        if request.size > self.capacity:
            log_error_too_large(self.capacity, request.key, request.size)
            return False

        while self.curr_capacity + request.size > self.capacity:
            # Set flag to denote that cache has completed filling up
            self._is_post_hydrate = True
            _, popped_size = self.map.popitem(last=False)
            self.curr_capacity -= popped_size

        if request.key in self.map:
            self.map.move_to_end(request.key)
            self.curr_capacity -= self.map[request.key]
        self.map[request.key] = request.size
        self.curr_capacity += self.map[request.key]
        return True

    def get(self, request):
        if request.key not in self.map:
            return None
        self.map.move_to_end(request.key)
        return self.map[request.key]


class S4LRUCache(BaseCache):
    """
    Four queues are maintained at levels 0 to 3.
    On a cache miss, the item is inserted at the head of queue 0.
    On a cache hit, the item is moved to the head of the next higher queue
        (items in queue 3 move to the head of queue 3).
    Each queue is allocated 1/4 of the total cache size and items are
    evicted from the tail of a queue to the head of the next lower queue to
    maintain the size invariants.
    Items evicted from queue 0 are evicted from the cache.
    """

    def __init__(self, capacity):
        super().__init__(capacity)
        self.segments = [LRUCache(0) for _ in range(4)]
        self._set_capacity(capacity)

    def __repr__(self):
        return "S4LRU"

    def segment_peek_head(self, i):
        return self.segments[i].peek_head()

    @property
    def is_post_hydrate(self):
        # is_post_hydrate = all([c.is_post_hydrate for c in self.segments])
        return all([c.is_post_hydrate for c in self.segments])

    def _set_capacity(self, capacity):
        total = capacity
        quarter_capacity = capacity // 4
        for i in range(3, -1, -1):
            if i:
                self.segments[i].capacity = quarter_capacity
                total -= quarter_capacity
            else:
                self.segments[i].capacity = total
            print(f"segment {i} size: {self.segments[i].capacity}")

    def get(self, request):
        for i in range(4):
            size = self.segments[i].get(request)
            if size is not None:
                if i < 3:
                    self.segments[i].evict(request.key)
                    self._segment_put(i + 1, request)
                return size
        return None

    def put(self, request):
        self.segments[0].put(request)

    def _segment_put(self, i, request):
        self.segments[i].put(request)
        if i == 0:
            return

        while self.segments[i].curr_capacity > self.segments[i].capacity:
            self._segment_put(i - 1, self.segments[i].evict_return())


_name_to_cls = {
    "LRU": LRUCache,
    "S4LRU": S4LRUCache,
}


def initialize_cache(cache_name, **kwargs):
    try:
        cls = _name_to_cls[cache_name]
    except KeyError:
        raise KeyError(f"Cache with {cache_name} is not implemented. Check _name_to_cls in caches.py")
    return cls(**kwargs)


Cache = NewType("Cache", BaseCache)
