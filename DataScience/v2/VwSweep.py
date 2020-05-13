import VwOptsGrid
from Vw import Vw
from Pool import MultiThreadPool, SeqPool

class VwSweepResult:
    def __init__(self, vw_result, opts, name = None):
        self.Loss = vw_result.Loss
        self.Opts = opts
        self.Model = vw_result.Populated[-1]['-f']
        self.Name = name

class VwSweep:
    def __init__(self, vw, pool = MultiThreadPool()):
        self.Core = vw
        self.Pool = pool
        self.Logger = self.Core.Ws.Logger

    def __iteration__(self, points, inputs):
        opts = [(inputs, point, ['-f']) for point in points]
        result = [VwSweepResult(result, opts) for (result, opts) in list(zip(self.Pool.map(self.Core.train, opts), points))]
        return sorted(result, key=lambda item: item.Loss)

    def run(self, multi_grid, inputs, base_command={}):
        base = [base_command]
        result = []
        promoted = []
        for grid in multi_grid:
            self.Logger.info('Sweeping params from {0}...'.format(grid.Config.Name))
            points = VwOptsGrid.product(base, grid.Points)
            ranked = self.__iteration__(points, inputs)
            promoted = ranked[:min(grid.Config.Promote, len(ranked))]
            output = ranked[:min(grid.Config.Output, len(ranked))]
            for i, o in enumerate(output):
                o.Name = '{0}n{1}'.format(grid.Config.Name, i)
            base = list(map(lambda p: p.Opts, promoted))
            result = result + output
        return result
