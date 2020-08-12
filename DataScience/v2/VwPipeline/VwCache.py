import os

class VwCache:
    def __init__(self, path: str, create=True):
        self.Path = path
        if create:
            os.makedirs(self.Path, exist_ok=True)

    @staticmethod
    def __file_name__(string_hash: str) -> str:
        import hashlib
        return hashlib.md5(string_hash.encode('utf-8')).hexdigest()

    def __make_path__(self, context: str, args_hash: str) -> str:
        folder_name = os.path.join(self.Path, context)
        os.makedirs(folder_name, exist_ok=True)
        return os.path.join(folder_name, VwCache.__file_name__(args_hash))

    def get_path(self, opts_in: dict, opt_out: str = None) -> str:
        import VwOpts
        args_hash = VwOpts.string_hash(VwOpts.to_string(opts_in))
        return self.__make_path__(f'populate-{opt_out}', args_hash)

    


