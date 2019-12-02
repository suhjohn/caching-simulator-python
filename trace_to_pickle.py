import pickle
from pickle import dumps

from traces import parse_tr_line

"""
s_dump and s_dump_elt from https://github.com/pgbovine/streaming-pickle
"""


def s_dump(iterable_to_pickle, file_obj, buff_size=100000):
    li = []
    for elt in iterable_to_pickle:
        li.append(elt)
        if len(li) == buff_size:
            s_dump_elt(li, file_obj)
            li = []
    if li:
        s_dump_elt(li, file_obj)


def s_dump_elt(elt_to_pickle, file_obj):
    pickled_elt_str = dumps(elt_to_pickle, protocol=pickle.HIGHEST_PROTOCOL)
    file_obj.write(pickled_elt_str)


def do_pickle(source, destination):
    write_to = open(destination, 'wb+')
    with open(source) as trace_file:
        trace_file_as_generator = (parse_tr_line(line) for line in trace_file)
        s_dump(trace_file_as_generator, write_to)
    write_to.close()


# sample execution
#

if __name__ == "__main__":
    import sys

    do_pickle(
        sys.argv[1],
        sys.argv[2]
    )
