import pandas as pd

class Statistics:
    @staticmethod
    def __read_stats__(path, time_column):
        stats = pd.read_csv(path, parse_dates=[time_column]).set_index([time_column])
        if 'clicksIpsRSlot1' not in stats.columns:
            stats['clicksIpsRSlot1'] = 0
        if 'impressionsIpsRSlot1' not in stats.columns:
            stats['impressionsIpsRSlot1'] = 1
        return stats


    @staticmethod
    def read_stats(paths, time_column = 'Timestamp'):
        return Statistics.concat([Statistics.__read_stats__(p, time_column) for p in paths], time_column)

    @staticmethod
    def concat(substats, time_column = 'Timestamp'):
        stats = pd.concat(substats)
        stats = stats.groupby([time_column]).sum()
        return stats

    @staticmethod
    def add_baselines(stats):
        stats['Online'] = stats.Rewards / stats.Events
        stats['OnlineSlot1'] = stats.RewardsSlot1 / stats.EventsSlot1
        stats['Baseline1'] = stats.RewardsIps1 / stats.EventsIps1         
        stats['Baseline1Slot1'] = stats.RewardsIps1Slot1 / stats.EventsIps1Slot1
        stats['BaselineR'] = stats.RewardsIpsR / stats.EventsIpsR         
        stats['BaselineRSlot1'] = stats.RewardsIpsRSlot1 / stats.EventsIpsRSlot1      
        return stats

class SlimLogs:
    @staticmethod
    def __read_dangling_rewards__(path):
        return pd.read_csv(path, parse_dates=['EnqueuedTimeUtc'])

    @staticmethod
    def __read_decisions__(path, chunksize = None):
        return pd.read_csv(path, parse_dates=['Timestamp'], chunksize = chunksize)

    @staticmethod
    def dangling_rewards(paths):
        return pd.concat([SlimLogs.__read_dangling_rewards__(path) for path in paths])

    @staticmethod
    def decisions(paths):
        return pd.concat([SlimLogs.__read_decisions__(path) for path in paths])

