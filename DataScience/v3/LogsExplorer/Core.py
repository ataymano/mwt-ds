import os
import pandas as pd
import datetime
from itertools import chain

from LogsExplorer import Parser

def load_last_modified(local_path):
    meta_path = f'{local_path}.lm'
    if not os.path.exists(meta_path):
        return None
    meta = pd.to_datetime(open(meta_path, 'r').read())
    return meta if isinstance(meta, datetime.datetime) else None

def save_last_modified(last_modified, local_path):
    with open(f'{local_path}.lm','w') as f:
        f.write(str(last_modified))

def is_in_sync(input, output):
    input_lm = load_last_modified(input)
    output_lm = load_last_modified(output)
    return input_lm and output_lm and input_lm == output_lm

def sync(input, output):
    save_last_modified(load_last_modified(input), output)

def __process__(input, processor):
    output = f'{input}.{processor.__name__}'
    if not is_in_sync(input, output):
        processor(open(input, 'r', encoding='utf-8')).to_csv(output)
        sync(input, output)
    return output

def process(inputs, processor):
    if isinstance(inputs, str):
        return __process__(inputs, processor)
    return [__process__(p, processor) for p in inputs]
