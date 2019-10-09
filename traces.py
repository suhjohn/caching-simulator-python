from abc import abstractmethod
from typing import List, NewType


class Trace:
    def __init__(self, key, size):
        assert isinstance(size, int), f"size: {type(size)}({size})"
        self.key = key
        self.size = size


def parse_tr(raw_data) -> List[dict]:
    """
    Parser function for human-readable .tr data format
    """
    lines = raw_data.split("\n")
    traces = []
    for line in lines:
        split_line = line.split(" ")
        # error handling in case invalid lines
        if not split_line:
            print(f"Invalid line: {line}")
            continue
        # Last line typically is just a newline which gets caught.
        # Conditional escapes
        if len(split_line) < 3:
            print(f"Invalid line: {line}")
            continue
        timestamp, key, size = split_line[0], int(split_line[1]), int(split_line[2])
        traces.append(
            {
                "timestamp": timestamp,
                "key": key,
                "size": size
            }
        )
    return traces


class ITraceInfo:
    """
    Method
    read()

    Property
    total_count:int: total number of traces
    total_size:int: total size of traces in bytes
    """

    @abstractmethod
    def read(self):
        pass


class FullCacheTrace(ITraceInfo):
    """
    Fully read the file
    """

    def __init__(self, filename, parser_fn):
        with open(filename, "r") as f:
            raw_data = f.read()
        data = parser_fn(raw_data)
        self.data: List[Trace] = []
        self.total_count = len(data)
        self.total_size = 0
        for cache_trace in data:
            cache_trace_obj = Trace(key=cache_trace["key"], size=cache_trace["size"])
            self.data.append(cache_trace_obj)
            self.total_size += cache_trace_obj.size

    def read(self):
        return self.data


class StreamingCacheTrace(ITraceInfo):
    """
    Chunk the reading of the file by an appropriate size for each read call
    """

    def __init__(self, filename, parser_fn):
        self.file = open(filename, "r")
        self.parser_fn = parser_fn
        self.total_count = 0
        self.total_size = 0

    def __del__(self):
        self.file.close()

    def read(self):
        return self._read_chunk()

    def _read_chunk(self):
        """
        Reads a chunk of data from filename using parser_fn.
        Next call of this method will read the next chunk.
        If we reach EOF, returns -1, and resets the index counter to beginning of the file.
        :return:
        """
        data = []

        return data


TraceInfo = NewType("TraceInfo", ITraceInfo)
