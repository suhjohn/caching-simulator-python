from abc import abstractmethod
from multiprocessing import pool
from typing import List, NewType


class CacheRequest:
    def __init__(self, key, size):
        assert isinstance(size, int), f"size: {type(size)}({size})"
        self.key = key
        self.size = size


def parse_tr_line(tr_data_line: str) -> CacheRequest:
    split_line = tr_data_line.split(" ")
    timestamp, key, size = split_line[0], int(split_line[1]), int(split_line[2])
    return CacheRequest(key, size)


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
        self._next_lines = []
        self._next_i = 0


    # Version 1
    # def __iter__(self):
    #     self.file = open(self.file_path, 'r')
    #     self._next_lines = self.file.readlines(65536)
    #     return self
    #
    # def __next__(self):
    #     try:
    #         next_line = self._next_lines[self._next_i]
    #     except IndexError:
    #         self._next_lines = self.file.readlines(65536)
    #         self._next_i = 0
    #         try:
    #             next_line = self._next_lines[self._next_i]
    #         except IndexError:
    #             self.file.close()
    #             raise StopIteration
    #
    #     trace = self.parser_fn(next_line)
    #     self._next_i += 1
    #     self.total_count += 1
    #     self.total_size += trace.size
    #     return trace

    # Version 2
    # def __iter__(self):
    #     self.file = open(self.file_path, 'r')
    #     self._next_lines = self.file.readlines(65536)
    #     return self
    #
    # def __next__(self):
    #     if not self._next_lines:
    #         self.file.close()
    #         raise StopIteration
    #
    #     next_line = self._next_lines[self._next_i]
    #     trace = self.parser_fn(next_line)
    #     self._next_i += 1
    #     self.total_count += 1
    #     self.total_size += trace.size
    #     if self._next_i == 65536:
    #         self._next_i = 0
    #         self._next_lines = self.file.readlines(65536)
    #     return trace


    def __iter__(self):
        self.file = open(self.file_path, 'r')
        return self

    def __next__(self) -> CacheRequest:
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
