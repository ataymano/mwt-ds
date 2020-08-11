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
    return list(result)


def dimension(name, values):
    return list(map(lambda v: dict([(name, str(v))]), values))


class Configuration:
    def __init__(self, name, promote=1, output=1):
        self.Name = name
        self.Promote = promote
        self.Output = output


class Grid:
    def __init__(self, points, config):
        self.Points = points
        self.Config = config


def generate():
    hyper_points = product(
        dimension('-l', [1e-6, 1e-5]),
        dimension('--cb_type', ['ips', 'mtr']),
    )

    return [
        Grid(hyper_points, Configuration(name='hyper1', output=2, promote=1)),
    ]
