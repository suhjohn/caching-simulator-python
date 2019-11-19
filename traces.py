from abc import abstractmethod
from typing import List, NewType


class Trace:
    def __init__(self, key, size):
        assert isinstance(size, int), f"size: {type(size)}({size})"
        self.key = key
        self.size = size


def parse_tr_line(tr_data_line: str) -> Trace:
    split_line = tr_data_line.split(" ")
    timestamp, key, size = split_line[0], int(split_line[1]), int(split_line[2])
    return Trace(key, size)


class CacheTraceIterator:
    """
    parser_fn is for various trace formats.
    """

    def __init__(self, file_path, parser_fn):
        self.file = None
        self.file_path = file_path
        self.parser_fn = parser_fn
        self.total_count = 0
        self.total_size = 0

    def __iter__(self):
        self.file = open(self.file_path, 'r')
        return self

    def __next__(self) -> Trace:
        next_line = self.file.readline()
        if next_line:
            trace = self.parser_fn(next_line)
            self.total_count += 1
            self.total_size += trace.size
            return trace

        # no new line means end of file
        self.file.close()
        raise StopIteration

    def __del__(self):
        if self.file and not self.file.closed:
            self.file.close()

    @property
    def trace_filename(self):
        trace_filename = self.file_path.split("/")[-1]
        return trace_filename.split(".")[0]
