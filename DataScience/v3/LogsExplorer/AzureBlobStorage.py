from azure.storage.blob import ContainerClient, BlobServiceClient
import datetime
import os
import json
import pandas as pd
import multiprocessing

def __add_path__(subtree, parts, last_modified):
    if len(parts) == 1:
        subtree[parts[0]] = last_modified
    else:
        if parts[0] not in subtree:
            subtree[parts[0]] = {}
        __add_path__(subtree[parts[0]], parts[1:], last_modified)
    

def get_file_tree(blobs_with_last_modified):
    result = {}
    for b, last_modified in blobs_with_last_modified:
        __add_path__(result, b.split('/'), last_modified)
    return result

def goto(tree, path:list=[]):
    cur = tree
    for p in path:
        if p not in cur:
            raise Exception(f'Cannot find {p} from {path}')
        cur = cur[p]
    return cur

def get_folders(tree, path:list=[], prefix='', full_path=False):
    cur = goto(tree, path)
    return [k if not full_path else '/'.join(path+[k]) for k in cur if not isinstance(cur[k], datetime.datetime) and k.startswith(prefix)]   

def get_files(tree, path:list=[], prefix='', full_path=False, recursive=False):
    cur = goto(tree, path)
    result = [k if not full_path else '/'.join(path+[k]) for k in cur if isinstance(cur[k], datetime.datetime) and k.startswith(prefix)]
    if recursive:
        for f in get_folders(tree, path, full_path):
            result = result + self.get_files(tree, path + f, full_path)  
    return result

def get_last_modified(tree, path:list=[]):
    cur = goto(tree, path)
    return cur if isinstance(cur, datetime.datetime) else None

def __to_local_path__(path):
    return os.path.join(*path.split('/'))

class Container:
    def __init__(self, container_client, local_folder, prefix=''):
        self.__impl__ = container_client
        self.refresh(prefix)
        self.local_folder = local_folder

    def refresh(self, prefix=''):
        self.__tree__ = get_file_tree([(b['name'], b['last_modified']) for b in self.__impl__.list_blobs(name_starts_with=prefix)])

    def __load_last_modified__(self, local_path):
        meta_path = f'{local_path}.lm'
        if not os.path.exists(meta_path):
            return None
        meta = pd.to_datetime(open(meta_path, 'r').read())
        return meta if isinstance(meta, datetime.datetime) else None
    
    def __save_last_modified__(self, last_modified, local_path):
        with open(f'{local_path}.lm','w') as f:
            f.write(str(last_modified))

    def __download__(self, path, local_path, max_concurrency=multiprocessing.cpu_count()):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self.__impl__.download_blob(path).download_to_stream(open(local_path, "wb"), max_concurrency=max_concurrency)
        return True

    def __sync__(self, path, local_path, last_modified=None, max_concurrency=multiprocessing.cpu_count()):
        lm_local = self.__load_last_modified__(local_path)
        lm_remote = last_modified if last_modified else self.get_last_modified(path)
        if not lm_local or lm_local < lm_remote:
            if self.__download__(path, local_path, max_concurrency=max_concurrency):
                self.__save_last_modified__(lm_remote, local_path)
            else:
                return None
        return local_path

    def get_last_modified(self, path):
        for blob in self.__impl__.list_blobs(name_starts_with=path):
            if blob['name']==path:
                return blob['last_modified']
        return None#get_last_modified(self.__tree__, path.split('/'))

    def sync_blobs(self, paths: list, max_concurrency=multiprocessing.cpu_count()):
        return [self.__sync__(p, os.path.join(self.local_folder, __to_local_path__(p))) for p in paths]

    def sync_folder(self, folder, max_concurrency=multiprocessing.cpu_count()):
        if folder[-1] != '/': folder = folder + '/'
        return sorted([self.__sync__(p['name'], os.path.join(self.local_folder, __to_local_path__(p['name'])), p['last_modified']) 
            for p in self.__impl__.list_blobs(name_starts_with=folder)])

class LogsClient(Container):
    def __init__(self, container_client, local_folder):
        super().__init__(container_client, local_folder)
        
    def get_models(self):
        import re
        p = re.compile(r'\d+')
        return sorted([f for f in get_folders(self.__tree__) if p.match(f)])

    def get_dates(self, model, years: list=None, months: list=None):
        result = set([])
        model_tree = goto(self.__tree__,  [model, 'data'])
        ys = years if years else get_folders(model_tree)
        for y in ys:
            year_tree = model_tree[y]
            ms = months if months else get_folders(year_tree)
            for m in ms:
                month_tree = year_tree[m]
                for d in get_files(month_tree):
                    result.add(datetime.date(int(y), int(m), int(d[0:2])))
        return sorted(list(result))

    def get_chunks(self, model, date, full_path=True):
        return get_files(self.__tree__,
            [model, 'data', str(date.year), str(date.month).zfill(2)],
            prefix=str(date.day).zfill(2),
            full_path=full_path)

    def get_chunk_ids(self, model, date):
        return sorted([int(p[3:p.find('.')]) for p in get_files(self.__tree__,
            [model, 'data', str(date.year), str(date.month).zfill(2)],
            prefix=str(date.day).zfill(2),
            full_path=False)])

    def get_full_path(self, model, date, chunk_id: int, is_local=False):
        path = [model, 'data', str(date.year), str(date.month).zfill(2), f'{str(date.day).zfill(2)}_{str(chunk_id).zfill(10)}.json']
        return '/'.join(path) if not is_local else os.path.join(*path)


