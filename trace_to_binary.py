import struct

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
        self.bin_file = bin_file
        _header_data = self.bin_file.read(HEADER_FMT_LEN)
        header = _header_unpack(_header_data)
        self.bin_fmt = BIN_FMT_BASE.format(
            key_size=header[0], key_type=header[1].decode("utf-8")
        )
        self.bin_fmt_len = struct.calcsize(self.bin_fmt)
        self.unpack_fn = struct.Struct(self.bin_fmt).unpack_from

    def __iter__(self):
        data = self.bin_file.read(self.bin_fmt_len)
        while data:
            yield self.unpack_fn(data)
            data = self.bin_file.read(self.bin_fmt_len)


if __name__ == "__main__":
    import sys

    source_filename = sys.argv[1]
    dest_filename = sys.argv[2]
    key_size = 1
    key_type = int

    # Writing
    source = open(source_filename, 'r')
    dest = open(dest_filename, 'wb+')
    writer = BinTraceWriter(key_size, key_type)
    writer.dump(source, dest)

    source.close()
    dest.close()

    # Reading
    # bin_file = open(dest_filename, 'rb')
    # reader = BinTraceReader(bin_file)
    # for line in reader:
    #     print(line)
    # bin_file.close()
