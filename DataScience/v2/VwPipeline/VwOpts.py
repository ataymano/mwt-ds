def string_hash(cmd: str) -> str:
    items = [i.strip() for i in f' {cmd}'.split(' -')]
    items.sort()
    return ' '.join(items).strip()

def to_string(opts: dict) -> str:
    return ' '.join(['{0} {1}'.format(key, opts[key]) if not key.startswith('#')
        else str(opts[key]) for key in sorted(opts.keys())])
