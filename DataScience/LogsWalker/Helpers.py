from azure.datalake.store import core, lib, multithread
from azure.storage.blob import BlockBlobService
import os
import datetime
import sys

def update_progress(current, total):
    barLength = 50 # Length of the progress bar
    progress = current/total
    block = int(barLength*progress)
    text = "\rProgress: [{0}] {1:.1f}%".format( "#"*block + "-"*(barLength-block), progress*100)
    sys.stdout.write(text)
    sys.stdout.flush()

class AzureStorage:
    @staticmethod
    def get_app_folders(bbs, container):
        folders = bbs.list_blobs(container, delimiter='/')
        folders = sorted([d.name[:-1] for d in filter(lambda d: d.name[-1] == '/' and d.name[0] == '2', folders)])
        return folders

    @staticmethod
    def get_latest_app_folder(bbs, container):
        folders = bbs.list_blobs(container, delimiter='/')
        folder = max([d.name[:-1] for d in filter(lambda d: d.name[-1] == '/' and d.name[0] == '2', folders)])
        return folder

    @staticmethod
    def get_date(path):
        dirs = path.split('/')
        return datetime.date(int(dirs[2]), int(dirs[3]), int(dirs[4].split('_')[0]))

    @staticmethod
    def get_days(bbs, container, model):
        return [AzureStorage.get_date(b.name) for b in bbs.list_blobs(container, prefix='{0}/data/'.format(model))]

class Adls:
    @staticmethod
    def adls_download(adls, path_server, path_local, buffersize=4 * 1024 **2, blocksize=4 * 1024, log=print):
        try:
            log('Downloading {0}'.format(path_server))
            multithread.ADLDownloader(adls, lpath=path_local, rpath=path_server, overwrite=True)
            log('Done')
            return True
        except:
            log('Cannot download {0}'.format(path_server))
            return False  

    def get_app_folders(adls, app):
        return list(filter(lambda p : p[0] == '2', [p[p.rindex('/') + 1:] for p in adls.ls('daily/{0}'.format(app))]))

class File:
    @staticmethod
    def find_last_eof(path):
        result = -1
        with open(path, 'rb') as f:
            f.seek(-1, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
                result = result - 1
            return result

    @staticmethod
    def find_first_eof(path):
        with open(path, 'rb') as f:
            f.readline()
            return f.tell()  

class DateTime:
    @staticmethod
    def range(first, second):
        return [first + datetime.timedelta(days=i) for i in range((second - first).days)]

class AzureStorageBlob:
    def __init__(self, bbs, container, path, log = print):
        self.__Bbs__ = bbs
        self.Container = container
        self.Path = path
        self.MaxConnections = 4
        self.Log = log

    def download(self, path, start_offset, end_offset = None, with_progress = False):
        try:
            self.__Bbs__.get_blob_to_path(self.Container, self.Path, path,
                start_range=start_offset, end_range=end_offset, max_connections = self.MaxConnections,
                progress_callback=update_progress if with_progress else None)
            self.Log('')
            return True
        except:
            self.Log('Cannot download {0} to {1}'.format(self.Path, path))
            return False

    def get_size(self):
        try:
            bp = self.__Bbs__.get_blob_properties(self.Container, self.Path)
            return bp.properties.content_length
        except:
            self.Log('Cannot get size of {0}'.format(self.Path))
