import os
import json

from VwPipeline import Logger

class Workspace:
    def __init__(self, path, logger = Logger.console_logger(0, 'INFO'), reset=False, norun=False):
        self.Path = path
        self.Reset = reset
        self.Logger = logger
        self.NoRun = norun
        os.makedirs(self.Path, exist_ok=True)

    def __make_path__(self, method, args_hash):
        import hashlib
        folder_name = os.path.join(self.Path, method)
        os.makedirs(folder_name, exist_ok=True)
        
        fname = hashlib.md5(args_hash.encode('utf-8')).hexdigest()
        self.Logger.debug('Generating path: ({0},{1})\t{2}'.format(method, args_hash, fname))
        return os.path.join(folder_name, fname)

    @staticmethod
    def __save__(obj, path):
        with open(path, 'w') as f:
            json.dump(obj, f)

    @staticmethod
    def __load__(path):
        with open(path, 'r') as f:
            return json.load(f)

    @staticmethod
    def __default_hash__(args):
        return ','.join([str(a) for a in args])

    def run(self, method, *args, **kwargs):
        args_hash = kwargs['hash'](*args) if 'hash' in kwargs else Workspace.__default_hash__(args)
        path = self.__make_path__(method.__name__, args_hash)  
        
        if self.Reset or not os.path.exists(path):
            if not self.NoRun:
                self.Logger.debug('Executing {0}...'.format(method.__name__))
                result = method(*args)
                Workspace.__save__(result, path)
            else:
                raise 'Result is not found, and execution is deprecated'
        else:
            self.Logger.debug('Result of {0} is found'.format(method.__name__))
        return Workspace.__load__(path)

class DummyWorkspace:
    def __init__(self, logger = Logger.console_logger(0, 'INFO')):
        self.Logger = logger

    def run(self, method, *args):
        return method(*args)

    def log(self, *args):
        print(*args)

    def __make_path__(self, method, args):
        raise 'Not supported. Please use real workspace'





