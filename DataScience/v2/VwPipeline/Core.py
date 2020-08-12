import os
import json

from VwPipeline import Logger
from VwPipeline.VwCache import VwCache
from VwPipeline import VwOpts

class Workspace:
    def __init__(self, cache, reset=False, norun=False):
        self.Cache = cache
        self.Reset = reset
        self.Logger = cache.Logger
        self.NoRun = norun

    @staticmethod
    def __save__(obj, path):
        with open(path, 'w') as f:
            json.dump(obj, f)

    @staticmethod
    def __load__(path):
        with open(path, 'r') as f:
            return json.load(f)

    def run(self, vw, opts_in: dict, opts_out: list):
        populated = {o: self.Cache.get_path(opts_in, o) for o in opts_out}
        metrics_path = self.Cache.get_path(opts_in)

        result_files = list(populated.values()) + [metrics_path]
        not_exist = next((p for p in result_files if not os.path.exists(p)), None)

        opts = dict(opts_in, **populated)

        if self.Reset or not_exist:
            if not_exist:
                Logger.debug(self.Logger, f'{not_exist} had not been found.')
            if self.NoRun:
                raise 'Result is not found, and execution is deprecated'  

            result = vw.__run__(opts)
            Workspace.__save__(result, metrics_path)                          
        else:
            Logger.debug(self.Logger, f'Result of vw execution is found: {VwOpts.to_string(opts)}')
        return Workspace.__load__(metrics_path), populated
