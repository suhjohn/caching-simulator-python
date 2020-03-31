from abc import abstractmethod, ABC
from typing import NewType, NamedTuple

from collections import defaultdict, namedtuple
import bloom_filter
import math


class BaseFilter(ABC):
    def __init__(self, args):
        self.args: NamedTuple = args

    @abstractmethod
    def should_filter(self, request):
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}({self.args})"

    def __str__(self):
        return f"{self.__class__.__name__}"


NullFilterArgs = namedtuple("NullFilterArgs", [])


class NullFilter(BaseFilter):
    @classmethod
    def init(cls, args):
        return cls(args)

    def should_filter(self, request):
        return False


BypassFilterArgs = namedtuple("BypassFilterArgs", ["threshold_size"])


class BypassFilter(BaseFilter):

    def __init__(self, args):
        super().__init__(args)
        self.threshold_size = args.threshold_size

    def should_filter(self, request):
        return request.size > self.threshold_size


BloomFilterArgs = namedtuple("BloomFilterArgs", ["m"])


class BloomFilter(BaseFilter):
    """
    Bloom Filter implementation.
    Assumes that the key is integer.
    """

    def __init__(self, args):
        """
         m: int, size of the filter
        """
        super().__init__(args)
        self._filters = [pybloomfilter.BloomFilter(args.m, error_rate=0.001) for _ in range(2)]
        self._current_filter = 0
        self._m = args.m
        self._i = 0

    def should_filter(self, request) -> bool:
        if self.exists(request.key):
            return False
        self.put(request.key)
        return True

    def put(self, key):
        """ insert the pair (key,value) in the database """
        if self._i > self._m:
            self._i = 0
            self._current_filter = 1 if self._current_filter == 0 else 0
            self._filters[self._current_filter] = bloom_filter.BloomFilter(self._m, error_rate=0.001)

        if key not in self._filters[self._current_filter]:
            self._filters[self._current_filter].add(key)
            self._i += 1

    def exists(self, key):
        """ check if key is exists in the filter
            using the filter mechanism """
        return key in self._filters[0] or key in self._filters[1]


_name_to_cls = {
    "Bloom": {
        "filter": BloomFilter,
        "args": BloomFilterArgs
    },
    "Bypass": {
        "filter": BypassFilter,
        "args": BypassFilterArgs
    },
    "Null": {
        "filter": NullFilter,
        "args": NullFilterArgs
    }
}


def initialize_filter(filter_name, **kwargs):
    try:
        cls = _name_to_cls[filter_name]["filter"]
    except KeyError:
        raise KeyError(f"Filter with {filter_name} is not implemented. Check _name_to_cls in filters.py")
    args = _name_to_cls[filter_name]["args"](**kwargs)
    return cls(args)


Filter = NewType("Filter", BaseFilter)
