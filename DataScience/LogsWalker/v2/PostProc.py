import pandas as pd

class Statistics:
    @staticmethod
    def __read_stats__(path, time_column):
        return pd.read_csv(path, parse_dates=[time_column]).drop(['clicks1', 'impressions1'], axis=1).set_index([time_column])

    @staticmethod
    def read_stats(paths, time_column = 'hour'):
        return Statistics.concat([Statistics.__read_stats__(p, time_column) for p in paths], time_column)

    @staticmethod
    def concat(substats, time_column = 'hour'):
        stats = pd.concat(substats)
        stats['tmp'] = stats.PassRatio * stats.impressions
        stats = stats.drop(['PassRatio'], axis = 1)
        stats = stats.groupby([time_column, 'model']).sum() \
            [['obser', 'clicks', 'impressions', 'clicksIps1', 'impressionsIps1', 'clicksIpsR', 'impressionsIpsR', 'tmp']]
        stats['PassRatio'] = stats.tmp / stats.impressions
        return stats.drop(['tmp'], axis=1).reset_index(['model'])

    @staticmethod
    def add_baselines(stats):
        stats['Online'] = stats.clicks / stats.impressions
        stats['Baseline1'] = stats.clicksIps1 / stats.impressionsIps1
        stats['BaselineR'] = stats.clicksIpsR / stats.impressionsIpsR
        return stats
