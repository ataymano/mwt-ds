import VwOptsGrid
from Vw import Vw, VwInput

class VwSweepResult:
    def __init__(self, vw_result, opts, name = None):
        self.Loss = vw_result.Loss
        self.Opts = opts
        self.Model = vw_result.Populated[-1]['-f']
        self.Name = name

class VwSweep:
    def __init__(self, vw, input_mode = VwInput.cache):
        self.Core = vw
        self.Logger = self.Core.Ws.Logger
        self.InputMode = input_mode

    def iteration(self, inputs, points, name='NoName'):
        self.Logger.info('Sweeping {0} is started'.format(name))
        raw_results = self.Core.train(inputs, points, ['-f'], self.InputMode)
        results = sorted(list(zip(raw_results, points)), key=lambda x: x[0].Loss)
        self.Logger.info('Sweeping {0} is finished'.format(name))
        return [VwSweepResult(result, opts, '{0}n{1}'.format(name, index)) 
            for (index, (result, opts)) in enumerate(results)]

    def run(self, inputs, multi_grid, base_command={}):
        base = [base_command]
        result = []
        promoted = []
        for grid in multi_grid:
            points = VwOptsGrid.product(base, grid.Points)
            ranked = self.iteration(points, inputs, grid.Config.Name)
            promoted = ranked[:min(grid.Config.Promote, len(ranked))]
            output = ranked[:min(grid.Config.Output, len(ranked))]
            base = list(map(lambda p: p.Opts, promoted))
            result = result + output
        return result
