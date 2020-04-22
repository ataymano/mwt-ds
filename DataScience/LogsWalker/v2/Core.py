import os

class Context:
    def __init__(self, path, parent=None, reset=False):
        self.Path = path
        self.Parent = parent
        self.FullPath = path if not parent else os.path.join(parent.FullPath, path)
        self.Reset = reset if not parent else parent.Reset or reset
        os.makedirs(self.FullPath, exist_ok=True)

class Data:
    def __init__(self, path, context):
        self.Path = path
        self.Context = context
        self.FullPath = os.path.join(context.FullPath, path)
        self.Exists = os.path.exists(self.FullPath)
        if context.Reset or not self.Exists:
            print('Reloading {0}'.format(self.FullPath))
            self.Exists = self.__load__()
        else:
            print('Reusing {0}'.format(self.FullPath))

