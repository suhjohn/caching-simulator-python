import hashlib
from abc import ABC, abstractmethod
from enum import IntEnum
from logging import Logger
from typing import NewType, Optional, NamedTuple
from collections import OrderedDict, namedtuple
from traces import CacheRequest


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

    def __repr__(self):
        return f"{self.__class__.__name__}({self.capacity},{self.args})"

    def __str__(self):
        return f"{self.__class__.__name__}_{self.capacity}"

    @property
    def id(self):
        h = hashlib.blake2s(digest_size=16)
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

    def evict(self, request: CacheRequest) -> Optional[CacheObject]:
        obj = self._evict()
        self.eviction_logger.info(obj.as_log(request))
        return obj

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
        # object feasible to store?
        if request.size > self.capacity:
            return False

        while self.curr_capacity + request.size > self.capacity:
            self.evict(request)

        if request.key in self.map:
            self.map.move_to_end(request.key)
        else:
            self.curr_capacity += request.size
            self.map[request.key] = CacheObject(request.key, request.size, request.ts, request.index)
        return True

    def pop(self, key):
        try:
            cache_obj = self.map.pop(key)
            self.curr_capacity -= cache_obj.size
            return cache_obj
        except Exception as e:
            print(e)
            return None


SLRUArgs = namedtuple("S4LRUArgs", ["n", "ratios"])


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


_name_to_cls = {
    "LRU": {
        "cache": LRUCache,
        "args": LRUArgs,
    },
    "SLRU": {
        "cache": SLRUCache,
        "args": SLRUArgs
    },
}


def initialize_cache(cache_name, capacity, **kwargs):
    try:
        cls = _name_to_cls[cache_name]["cache"]
    except KeyError:
        raise KeyError(f"Cache with {cache_name} is not implemented. Check _name_to_cls in caches.py")
    args = _name_to_cls[cache_name]["args"](**kwargs)
    return cls(capacity, args)


Cache = NewType("Cache", BaseCache)
