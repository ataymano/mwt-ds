import pandas as pd

class Statistics:
    @staticmethod
    def __read_stats__(path, time_column):
        return pd.read_csv(path, parse_dates=[time_column]).drop(['clicks1', 'impressions1'], axis=1).set_index([time_column])

    @staticmethod
    def read_stats(paths, time_column = 'Timestamp'):
        return Statistics.concat([Statistics.__read_stats__(p, time_column) for p in paths], time_column)

    @staticmethod
    def concat(substats, time_column = 'Timestamp'):
        stats = pd.concat(substats)
        stats = stats.groupby([time_column]).sum() \
            [['obser', 'clicks', 'impressions', 'clicksIps1', 'impressionsIps1', 'clicksIpsR', 'impressionsIpsR', 'impressionsObserved']]
        return stats

    @staticmethod
    def add_baselines(stats):
        stats['Online'] = stats.clicks / stats.impressions
        stats['Baseline1'] = stats.clicksIps1 / stats.impressionsIps1
        stats['BaselineR'] = stats.clicksIpsR / stats.impressionsIpsR
        return stats
