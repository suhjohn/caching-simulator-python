import struct

import mmap

import array

"""
Binary Format
|           |            | 
 {timestamp} {trace size}  {key}
"""
HEADER_FMT = "<Qs"
HEADER_FMT_LEN = struct.calcsize(HEADER_FMT)
_header_unpack = struct.Struct(HEADER_FMT).unpack_from

BIN_FMT_BASE = "<QQ{key_size}{key_type}"


class BinTraceWriter:

    def __init__(self, key_size, key_type):
        if key_type == str:
            struct_fmt_type = "s"
        elif key_type == int:
            struct_fmt_type = "Q"
        else:
            raise Exception

        self.bin_fmt = BIN_FMT_BASE.format(
            key_size=key_size, key_type=struct_fmt_type
        )
        self.bin_fmt_len = struct.calcsize(self.bin_fmt)
        self.key_size = key_size
        self.key_type = key_type
        self.struct_fmt_type = struct_fmt_type

    def _write_header(self, write_to):
        write_to.write(
            struct.pack(
                HEADER_FMT, self.key_size, self.struct_fmt_type.encode("utf-8")
            )
        )

    def _parse_tr_line(self, tr_data_line):
        split_line = tr_data_line.split(" ")
        timestamp = int(split_line[0])
        size = int(split_line[2])
        if self.key_type == int:
            key = self.key_type(split_line[1])
        elif self.key_type == str:
            key = split_line[1].encode("utf-8")
        else:
            raise Exception

        return struct.pack(
            self.bin_fmt,
            timestamp, size, key
        )

    def dump(self, source, dest):
        self._write_header(dest)
        for line in source:
            tr_in_bin = self._parse_tr_line(line)
            dest.write(tr_in_bin)


class BinTraceReader:

    def __init__(self, bin_file):
        self.bin_file = mmap.mmap(bin_file.fileno(), 0)
        _header_data = self.bin_file.read(HEADER_FMT_LEN)
        header = _header_unpack(_header_data)
        self.bin_fmt = BIN_FMT_BASE.format(
            key_size=header[0], key_type=header[1].decode("utf-8")
        )
        self.bin_fmt_len = struct.calcsize(self.bin_fmt)
        self.unpack_fn = struct.Struct(self.bin_fmt).unpack_from

    def __iter__(self):
        chunk_size = 100000
        chunks = self.bin_file.read(self.bin_fmt_len * chunk_size)
        while chunks:
            for i in range(chunk_size):
                start = i * self.bin_fmt_len
                try:
                    yield self.unpack_fn(chunks[start:start + self.bin_fmt_len])
                except:
                    return

            chunks = self.bin_file.read(self.bin_fmt_len * chunk_size)


class BinArrTraceWriter:
    def _parse_tr_line(self, tr_data_line):
        split_line = tr_data_line.split(" ")
        timestamp = int(split_line[0])
        size = int(split_line[2])
        key = hash(split_line[1])
        return array.array('l', [timestamp, size, key])

    def dump(self, source, dest):
        for line in source:
            tr_array = self._parse_tr_line(line)
            tr_array.tofile(dest)


class BinArrTraceReader:
    def __init__(self, bin_file):
        self.bin_file = bin_file

    def __iter__(self):
        chunk_size = 10000
        while True:
            a = array.array('l')
            try:
                a.fromfile(self.bin_file, 3 * chunk_size)
            except EOFError:
                break
            for i in range(chunk_size):
                yield a[i * 3], a[1 + i * 3 ], a[2 + i * 3]
        if a:
            for i in range(len(a) // 3):
                yield a[i * 3], a[1 + i * 3], a[2 + i * 3]



if __name__ == "__main__":
    import sys

    source_filename = sys.argv[1]
    dest_filename = sys.argv[2]
    trace_type = sys.argv[3]
    source = open(source_filename, 'r')
    dest = open(dest_filename, 'wb+')

    if trace_type == "bin":
        key_size = 1
        key_type = int

        # Writing
        writer = BinTraceWriter(key_size, key_type)
        writer.dump(source, dest)
    elif trace_type == "bin_arr":
        writer = BinArrTraceWriter()
        writer.dump(source, dest)
    else:
        source.close()
        dest.close()
        raise KeyError("invalid trace_type")

    source.close()
    dest.close()
