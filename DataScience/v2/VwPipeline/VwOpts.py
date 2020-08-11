def normalize(cmd):
    items = [i.strip() for i in f' {cmd}'.split(' -')]
    items.sort()
    return ' '.join(items).strip()