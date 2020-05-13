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

    def iteration(self, points, inputs, name='NoName'):
        opts = [(inputs, point, ['-f']) for point in points]
        raw_results = self.Pool.map(self.Core.train, opts)
        results = sorted(list(zip(raw_results, points)), key=lambda x: x[0].Loss)
        return [VwSweepResult(result, opts, '{0}n{1}'.format(name, index)) 
            for (index, (result, opts)) in enumerate(results)]

    def run(self, multi_grid, inputs, base_command={}):
        base = [base_command]
        result = []
        promoted = []
        for grid in multi_grid:
            self.Logger.info('Sweeping params from {0}...'.format(grid.Config.Name))
            points = VwOptsGrid.product(base, grid.Points)
            ranked = self.iteration(points, inputs, grid.Config.Name)
            promoted = ranked[:min(grid.Config.Promote, len(ranked))]
            output = ranked[:min(grid.Config.Output, len(ranked))]
            base = list(map(lambda p: p.Opts, promoted))
            result = result + output
        return result
