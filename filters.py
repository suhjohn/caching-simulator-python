import hashlib
from abc import abstractmethod, ABC
from typing import NewType, NamedTuple
from sortedcontainers import SortedList
from collections import defaultdict, namedtuple, deque
import bloom_filter
import math

from quickselect import kthSmallest


class BaseFilter(ABC):
    def __init__(self, args):
        self.args: NamedTuple = args

    @property
    def id(self):
        h = hashlib.blake2s(digest_size=8)
        args = self.args._asdict()
        keys = sorted(args.keys())
        args_list = []
        for key in keys:
            args_list.append(key)
            args_list.append(args[key])
        h.update(f"{self.__class__.__name__}({tuple(args_list)})".encode())
        return h.hexdigest()

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


SetFilterArgs = namedtuple("SetFilterArgs", [])


class SetFilter(BaseFilter):

    def __init__(self, args):
        super().__init__(args)
        self.set = set()

    def should_filter(self, request):
        should_filter = request.key not in self.set
        self.set.add(request.key)
        return should_filter


BloomFilterArgs = namedtuple("BloomFilterArgs", ["n"])


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
        self._filters = [bloom_filter.BloomFilter(args.n, error_rate=0.001) for _ in range(2)]
        self._current_filter = 0
        self._n = args.n
        self._i = 0

    def should_filter(self, request) -> bool:
        if self.exists(request.key):
            return False
        self.put(request.key)
        return True

    def put(self, key):
        """ insert the pair (key,value) in the database """
        if self._i > self._n:
            self._i = 0
            self._current_filter = 1 if self._current_filter == 0 else 0
            self._filters[self._current_filter] = bloom_filter.BloomFilter(self._n, error_rate=0.001)

        if key not in self._filters[self._current_filter]:
            self._filters[self._current_filter].add(key)
            self._i += 1

    def exists(self, key):
        """ check if key is exists in the filter
            using the filter mechanism """
        return key in self._filters[0] or key in self._filters[1]


PercentileFilterArgs = namedtuple("PercentileFilterArgs", ["size", "percentile"])


class PercentileFilter(BaseFilter):
    default_args = PercentileFilterArgs(1000000, 75)

    def __init__(self, args):
        super().__init__(args)
        self.sliding_window = deque(maxlen=args.size)
        self.sorted_sizes = SortedList()
        self.window_size = args.size
        self.percentile = args.percentile
        self.percentile_index = int(args.size * (args.percentile / 100))
        self.curr_index = 0

    def should_filter(self, request) -> bool:
        if self.curr_index < self.window_size:
            self.sliding_window.append(request)
            self.sorted_sizes.add(request.size)
            self.curr_index += 1
            return False

        should_filter = request.size > self.sorted_sizes[self.percentile_index]
        oldest_req = self.sliding_window.popleft()
        self.sorted_sizes.remove(oldest_req.size)
        self.sliding_window.append(request)
        self.sorted_sizes.add(request.size)
        return should_filter


PercentileBloomFilterArgs = namedtuple("PercentileBloomFilterArgs", ["size", "percentile"])


class PercentileBloomFilter(BaseFilter):

    def __init__(self, args):
        super().__init__(args)
        self.sliding_window = deque(maxlen=args.size)
        self.sorted_sizes = SortedList()
        self.window_size = args.size
        self.percentile = args.percentile
        self.percentile_index = int(args.size * (args.percentile / 100))
        self.bloom_filter = BloomFilter(BloomFilterArgs(n=1000000))
        self.curr_index = 0

    def should_filter(self, request) -> bool:
        if self.curr_index < self.window_size:
            self.sliding_window.append(request)
            self.sorted_sizes.add(request.size)
            self.curr_index += 1
            return False
        should_filter = request.size > self.sorted_sizes[self.percentile_index]
        if should_filter:
            should_filter = self.bloom_filter.should_filter(request)
        oldest_req = self.sliding_window.popleft()
        self.sorted_sizes.remove(oldest_req.size)
        self.sliding_window.append(request)
        self.sorted_sizes.add(request.size)
        self.bloom_filter.put(request.key)
        return should_filter


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
    },
    "Percentile": {
        "filter": PercentileFilter,
        "args": PercentileFilterArgs
    },
    "PercentileBloom": {
        "filter": PercentileBloomFilter,
        "args": PercentileBloomFilterArgs
    },
    "Set": {
        "filter": SetFilter,
        "args": SetFilterArgs
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
