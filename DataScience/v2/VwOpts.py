import functools
import itertools
import json
import re


def apply(first, second):
    return dict(first, **second)


def product(*dimensions):
    result = functools.reduce(
        lambda d1, d2: map(
            lambda tuple: apply(tuple[0], tuple[1]),
            itertools.product(d1, d2)
        ), dimensions)
    return result


def dimension(name, values):
    return list(map(lambda v: dict([(name, str(v))]), values))