from VwPipeline import VwOptsGrid
from VwPipeline.Vw import Vw, VwInput
import pandas as pd

class VwSweepResult:
    def __init__(self, vw_result, opts, name = None):
        self.Result = vw_result
        self.Opts = opts
        self.Model = vw_result.Populated[-1]['-f']
        self.Name = name

class VwSweep:
    def __init__(self, vw, input_mode = VwInput.raw):
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
    
    def iteration_df(self, inputs, df, name='NoName'):
        results = self.iteration(inputs, df['Args'].to_list(), name)
        return pd.DataFrame(map(lambda r: {'Loss': r.Result.Loss, 'Args': r.Opts, 'Name': r.Name, 'Model': r.Model}, results)).sort_values('Loss')

    def predict(self, inputs, points):
        self.Logger.info('Prediction stage is started')
        raw_results = self.Core.train(inputs, points, ['-p'], self.InputMode)
        results = sorted(list(zip(raw_results, points)), key=lambda x: x[0].Loss)
        self.Logger.info('Prediction stage is finished')
        return [(result, opts) for (result, opts) in results]

    def predict_df(self, inputs, df):
        results = self.predict(inputs, df['PredictArgs'].to_list())
        result = df[['Name', 'PredictArgs']]
        result['Predictions'] = [[p['-p'] for p in r[0].Populated] for r in results]
        return result

        self.Logger.info('Predicting {0} is started'.format(name))
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
            ranked = self.iteration(inputs, points, grid.Config.Name)
            promoted = ranked[:min(grid.Config.Promote, len(ranked))]
            output = ranked[:min(grid.Config.Output, len(ranked))]
            base = list(map(lambda p: p.Opts, promoted))
            result = result + output
        return result
