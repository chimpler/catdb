from contextlib import contextmanager
import sys


class CatDbException(Exception):
    pass


@contextmanager
def open_output_file(filename):
    if filename == '-':
        yield sys.stdout
    else:
        with open(filename, 'w') as fd:
            yield fd


@contextmanager
def open_input_file(filename):
    if filename == '-':
        yield sys.stdin
    else:
        with open(filename, 'r') as fd:
            yield fd
