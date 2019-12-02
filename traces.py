from abc import abstractmethod, ABC
from collections import namedtuple
from six.moves import cPickle as pickle
from multiprocessing import Pool

from trace_to_binary import BinTraceReader, BinArrTraceReader

DEFAULT_TRACE_TYPE = "string"
CacheRequest = namedtuple("CacheRequest", ['key', 'size'])


def parse_tr_line(tr_data_line: str) -> CacheRequest:
    split_line = tr_data_line.split(" ")
    timestamp = split_line[0]
    size = int(split_line[2])
    try:
        key = int(split_line[1])
    except:
        key = split_line[1]
    trace = CacheRequest(key, size)
    return trace


class CacheTraceIterator(ABC):
    def __init__(self, file_path):
        self.file_path = file_path
        self.total_count = 0
        self.total_size = 0

    @abstractmethod
    def __iter__(self):
        pass

    @property
    def trace_filename(self):
        trace_filename = self.file_path.split("/")[-1]
        return trace_filename.split(".")[0]


class StringCacheTraceIterator(CacheTraceIterator):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.parser_fn = parse_tr_line

    def __iter__(self):
        """

        :return: generator(CacheRequest)
        """
        file = open(self.file_path, 'r')
        next_line = file.readline()
        while next_line:
            trace = self.parser_fn(next_line)
            self.total_count += 1
            self.total_size += trace.size
            yield trace
            next_line = file.readline()
        file.close()


class BatchStringCacheTraceIterator(CacheTraceIterator):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.parser_fn = parse_tr_line

    def __iter__(self):
        """

        :return: generator(CacheRequest)
        """
        file = open(self.file_path, 'r')
        next_lines = file.readlines(100000)
        while next_lines:
            for next_line in next_lines:
                trace = self.parser_fn(next_line)
                self.total_count += 1
                self.total_size += trace.size
                yield trace
            next_lines = file.readlines(100000)
        file.close()


class PickleCacheTraceIterator(CacheTraceIterator):

    def __init__(self, file_path):
        super().__init__(file_path)

    def __iter__(self):
        file = open(self.file_path, 'rb')
        traces = pickle.load(file)
        while True:
            for trace in traces:
                self.total_count += 1
                self.total_size += trace.size
                yield trace
            try:
                traces = pickle.load(file)
            except EOFError:
                break
        file.close()


class BinCacheTraceIterator(CacheTraceIterator):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.file = None
        self.reader = None

    def __iter__(self):
        self.file = open(self.file_path, 'rb+')
        self.reader = BinTraceReader(self.file)
        for line in self.reader:
            trace = CacheRequest(line[2], line[1])
            self.total_count += 1
            self.total_size += trace.size
            yield trace

    def __del__(self):
        if self.file:
            self.file.close()


class BinArrCacheTraceIterator(CacheTraceIterator):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.file = None
        self.reader = None

    def __iter__(self):
        self.file = open(self.file_path, 'rb+')
        self.reader = BinArrTraceReader(self.file)
        for line in self.reader:
            trace = CacheRequest(line[2], line[1])
            self.total_count += 1
            self.total_size += trace.size
            yield trace

    def __del__(self):
        if self.file:
            self.file.close()


_name_to_cls = {
    "string": StringCacheTraceIterator,
    "batch_string": BatchStringCacheTraceIterator,
    "pickle": PickleCacheTraceIterator,
    "binary": BinCacheTraceIterator,
    "bin_arr": BinArrCacheTraceIterator
}


def initialize_iterator(trace_type, file_path):
    try:
        cls = _name_to_cls[trace_type]
    except KeyError:
        raise KeyError(f"Cache with {trace_type} is not implemented. Check _name_to_cls in traces.py")
    return cls(file_path)
