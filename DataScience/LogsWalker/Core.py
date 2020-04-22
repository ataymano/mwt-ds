import os

class Context:
    def __init__(self, path, parent=None, reset=False, debug=False):
        self.Path = path
        self.Parent = parent
        self.FullPath = path if not parent else os.path.join(parent.FullPath, path)
        self.Reset = reset if not parent else parent.Reset or reset
        self.Debug = debug if not parent else parent.Debug
        os.makedirs(self.FullPath, exist_ok=True)

    def log(self, *args):
        if self.Debug:
            print(args)

class Data:
    def __init__(self, path, context):
        self.Path = path
        self.Context = context
        self.FullPath = os.path.join(context.FullPath, path)
        self.Exists = os.path.exists(self.FullPath)
        if context.Reset or not self.Exists:
            self.Context.log('Reloading {0}'.format(self.FullPath))
            self.Exists = self.__load__()
        else:
            self.Context.log('Reusing {0}'.format(self.FullPath))

