def string_hash(cmd: str) -> str:
    items = [i.strip() for i in f' {cmd}'.split(' -')]
    items.sort()
    return ' '.join(items).strip()

def to_string(opts: dict) -> str:
    return ' '.join(['{0} {1}'.format(key, opts[key]) if not key.startswith('#')
        else str(opts[key]) for key in sorted(opts.keys())])

def product(*dimensions: list) -> list:
    import functools
    import itertools
    result = functools.reduce(
        lambda d1, d2: map(
            lambda tuple: dict(tuple[0], **tuple[1]),
            itertools.product(d1, d2)
        ), dimensions)
    return list(result)

def dimension(name: str, values: list) -> list:
    return list(map(lambda v: dict([(name, str(v))]), values))
