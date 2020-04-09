import hashlib
from abc import ABC, abstractmethod
from enum import IntEnum
from heapq import heappush, heappop
from logging import Logger
from typing import NewType, Optional, NamedTuple
from collections import OrderedDict, namedtuple, defaultdict
from traces import CacheRequest
from sortedcontainers import SortedDict


class CacheState(IntEnum):
    PRE_WARMUP = 0
    POST_WARMUP = 1


class CacheObject:
    def __init__(self, key, size, ts, index):
        self.key = key
        self.size = size
        self.ts = ts
        self.index = index
        self.frequency = 1

    def as_log(self, request):
        return f"{self.key} {self.size} {self.frequency} {self.ts} " \
               f"{request.ts - self.ts} {self.index} {request.index - self.index}"

    def touch(self):
        self.frequency += 1


class BaseCache(ABC):
    def __init__(self, capacity, args):
        self.capacity = capacity
        self.args: NamedTuple = args
        self.curr_capacity = 0
        self.eviction_logger: Logger = None
        self.eviction_fn = self._evict_without_logging

    def __repr__(self):
        return f"{self.__class__.__name__}({self.capacity},{self.args})"

    def __str__(self):
        return f"{self.__class__.__name__}"

    @property
    def id(self):
        h = hashlib.blake2s(digest_size=8)
        args = self.args._asdict()
        keys = sorted(args.keys())
        args_list = []
        for key in keys:
            args_list.append(key)
            args_list.append(args[key])
        h.update(f"{self.__class__.__name__}({self.capacity},{tuple(args_list)})".encode())
        return h.hexdigest()

    def set_eviction_logger(self, logger):
        self.eviction_logger = logger
        self.eviction_fn = self._evict_with_logging

    def _evict_with_logging(self, request):
        obj = self._evict()
        self.eviction_logger.info(obj.as_log(request))
        return obj

    def _evict_without_logging(self, request):
        obj = self._evict()
        return obj

    def evict(self, request: CacheRequest) -> Optional[CacheObject]:
        return self.eviction_fn(request)

    def admit(self, request: CacheRequest) -> None:
        self._admit(request)

    def get(self, request: CacheRequest) -> Optional[CacheObject]:
        obj = self._get(request)
        if obj:
            obj.touch()
            return obj
        else:
            return None

    @abstractmethod
    def _evict(self) -> CacheObject:
        """

        :return:
        """
        pass

    @abstractmethod
    def _admit(self, request: CacheRequest) -> None:
        """

        :param request: traces.CacheRequest
        :return:
        """
        pass

    @abstractmethod
    def _get(self, request: CacheRequest) -> CacheObject:
        """
        Returns the size of the key if the object exists in cache.
        Otherwise, returns None.
        :param request: traces.CacheRequest
        :return:
        """
        pass


LRUArgs = namedtuple("LRUArgs", [])


class LRUCache(BaseCache):
    def __init__(self, capacity, args):
        super().__init__(capacity, args)
        self.map: OrderedDict[str, CacheObject] = OrderedDict()

    def _evict(self):
        lru_key, lru_obj = self.map.popitem(last=False)
        self.curr_capacity -= lru_obj.size
        return lru_obj

    def _get(self, request: CacheRequest):
        if request.key not in self.map:
            return None
        self.map.move_to_end(request.key)
        return self.map[request.key]

    def _admit(self, request: CacheRequest):
        if request.size > self.capacity:
            return False

        while self.curr_capacity + request.size > self.capacity:
            self.evict(request)

        if request.key not in self.map:
            self.curr_capacity += request.size
            self.map[request.key] = CacheObject(request.key, request.size, request.ts, request.index)
        self.map.move_to_end(request.key)
        return True

    def pop(self, key):
        try:
            cache_obj = self.map.pop(key)
            self.curr_capacity -= cache_obj.size
            return cache_obj
        except Exception as e:
            print(e)
            return None


class SLRUArgs:
    def __init__(self, n=4, ratios=[0.25, 0.25, 0.25, 0.25]):
        self.n = n
        self.ratios = ratios

    def _asdict(self):
        return OrderedDict({"n": self.n, "ratios": self.ratios})


class SLRUCache(BaseCache):
    default_args = SLRUArgs(4, [0.25, 0.25, 0.25, 0.25])

    def __init__(self, capacity, args=default_args):
        assert args.n == len(args.ratios)
        assert sum(args.ratios) == 1
        super().__init__(capacity, args)
        self.segments = [LRUCache(0, LRUArgs()) for _ in range(args.n)]
        self._set_capacity(capacity, args.ratios)

    def __repr__(self):
        return "S4LRU"

    def _set_capacity(self, capacity, ratios):
        for i, ratio in enumerate(ratios):
            self.segments[i].capacity = int(capacity * ratio)

    def _get(self, request):
        for i, segment in enumerate(self.segments):
            obj = segment.get(request)
            if obj is not None and i != len(self.segments) - 1:
                self.segments[i].pop(request.key)
                self._segment_put(i + 1, request)
                return obj
        return None

    def _admit(self, request):
        self._segment_put(0, request)

    def _evict(self):
        pass

    def _segment_put(self, i, request):
        self.segments[i].admit(request)
        if i == 0:
            return

        while self.segments[i].curr_capacity > self.segments[i].capacity:
            self._segment_put(i - 1, self.segments[i].evict(request))


GDSFArgs = namedtuple("GDSFArgs", [])


class GreedyDualCacheObj(CacheObject):
    def __init__(self, key, size, ts, index, priority):
        super().__init__(key, size, ts, index)
        self.priority = priority


class GDSFCache(BaseCache):
    def __init__(self, capacity, args):
        super().__init__(capacity, args)
        self._value_map = SortedDict()
        self._cache_map = dict()
        self._current_l = 0

    def _compute_priority(self, request):
        freq = self._cache_map[request.key].frequency
        return self._current_l + (freq / request.size)

    def _has(self, _id):
        return _id in self._cache_map

    def _get(self, request: CacheRequest):
        obj = self._cache_map.get(request.key)
        if obj:
            new_priority = self._compute_priority(request)
            self._value_map[obj.priority].remove(request.key)
            if len(self._value_map[obj.priority]) == 0:
                del self._value_map[obj.priority]
            self._value_map.setdefault(new_priority, [])
            self._value_map[new_priority].append(request.key)
            obj.priority = new_priority
            assert self._cache_map.get(request.key).priority == new_priority
            return obj
        return None

    def _evict(self):
        priority, keys = self._value_map.peekitem(0)  # item with smallest priority
        key = keys.pop(0)  # item that was inserted the oldest
        if len(keys) == 0:
            del self._value_map[priority]
        cache_obj = self._cache_map[key]
        self.curr_capacity -= self._cache_map[key].size
        self._current_l = priority
        del self._cache_map[key]
        return cache_obj

    def _admit(self, request: CacheRequest):
        if request.size >= self.capacity:
            return False
        self._cache_map[request.key] = GreedyDualCacheObj(
            request.key, request.size, request.ts, request.index, 0
        )
        priority = self._compute_priority(request)
        self._cache_map[request.key].priority = priority
        self._value_map.setdefault(priority, [])
        self._value_map[priority].append(request.key)
        self.curr_capacity += request.size
        while self.curr_capacity + request.size > self.capacity:
            self.evict(request)
        return True


_name_to_cls = {
    "LRU": {
        "cache": LRUCache,
        "args": LRUArgs,
    },
    "SLRU": {
        "cache": SLRUCache,
        "args": SLRUArgs
    },
    "GDSF": {
        "cache": GDSFCache,
        "args": GDSFArgs
    }
}


def initialize_cache(cache_name, capacity, **kwargs):
    try:
        cls = _name_to_cls[cache_name]["cache"]
    except KeyError:
        raise KeyError(f"Cache with {cache_name} is not implemented. Check _name_to_cls in caches.py")
    args = _name_to_cls[cache_name]["args"](**kwargs)
    return cls(capacity, args)


Cache = NewType("Cache", BaseCache)
