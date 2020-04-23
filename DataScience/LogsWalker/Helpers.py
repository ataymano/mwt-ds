from azure.datalake.store import core, lib, multithread
from azure.storage.blob import BlockBlobService
import os
import datetime

class AzureStorage:
    @staticmethod
    def get_latest_app_folder(bbs, container):
        folders = bbs.list_blobs(container, delimiter='/')
        folder = max([d.name[:-1] for d in filter(lambda d: d.name[-1] == '/', folders)])
        return folder

    @staticmethod
    def get_date(path):
        dirs = path.split('/')
        return datetime.date(int(dirs[2]), int(dirs[3]), int(dirs[4].split('_')[0]))

    @staticmethod
    def get_latest_day(bbs, container, model):
        dates = [AzureStorage.get_date(b.name) for b in bbs.list_blobs(container, prefix='{0}/data/'.format(model))]
        return max(dates)

class Adls:
    @staticmethod
    def adls_download(adls, path_server, path_local, buffersize=4 * 1024 **2, blocksize=4 * 1024):
        try:
            print('Downloading {0}'.format(path_server))
            multithread.ADLDownloader(adls, lpath=path_local, rpath=path_server, overwrite=True)
            print('Done')
            return True
        except:
            return False  

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
    def __init__(self, bbs, container, path):
        self.__Bbs__ = bbs
        self.Container = container
        self.Path = path
        self.MaxConnections = 4

    def download(self, path, start_offset, end_offset):
        try:
            self.__Bbs__.get_blob_to_path(self.Container, self.Path, path,
                start_range=start_offset, end_range=end_offset, max_connections = self.MaxConnections)
            return True
        except:
            print('Cannot download {0} to {1}'.format(self.Path, path))
            return False

    def get_size(self):
        try:
            bp = self.__Bbs__.get_blob_properties(self.Container, self.Path)
            return bp.properties.content_length
        except:
            print('Cannot get size of {0}'.format(self.Path))
