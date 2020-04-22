from azure.datalake.store import core, lib, multithread
import itertools
import datetime
from Helpers import DateTime
import pandas as pd
import os
import sys

def adls_download(adls, adlsPath, localPath, buffersize = 4 * 1024 **2, blocksize = 4 * 1024):
    if os.path.exists(localPath):
        os.remove(localPath)
    multithread.ADLDownloader(adls, lpath=localPath, rpath=adlsPath, overwrite=True)

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

class SlimLogs:
    @staticmethod
    def __read_dangling_rewards__(path):
        return pd.read_csv(path, parse_dates=['EnqueuedTimeUtc'])

    @staticmethod
    def __read_decisions__(path, chunksize = None):
        return pd.read_csv(path, parse_dates=['Timestamp'], chunksize = chunksize)

    @staticmethod
    def read_dangling_rewards(paths):
        return pd.concat([SlimLogs.__read_dangling_rewards__(path) for path in paths])

    @staticmethod
    def read_decisions(paths):
        return pd.concat([SlimLogs.__read_decisions__(path) for path in paths])

    @staticmethod
    def iterate_decisions(paths, chunksize):
        for path in paths:
            print('\n' + path)
            for chunk in SlimLogs.__read_decisions__(path, chunksize):
                sys.stdout.write('.')
                yield chunk

    @staticmethod
    def anayze_dangling_rewards(decisions_paths, rewards_paths, chunksize):
        rewards = SlimLogs.read_dangling_rewards(rewards_paths)['EventId', 'EnqueuedTimeUtc'].groupby('EventId').min()
#        for decisions in SlimLogs.read_decisions(decisions_paths, chunksize):
#            joined = 

class StatsContext:
    def __init__(self, local_folder, adlsClient, app, model=None, adls_folder = 'daily'):
        self.__Adls__ = adlsClient
        self.AdlsFolder = '{0}/{1}'.format(adls_folder, app)
        self.LocalFolder = local_folder
        self.Model = model
        self.JoinPath = os.path.join(self.LocalFolder, 'join')
        os.makedirs(self.JoinPath, exist_ok=True)

    @staticmethod
    def __get_path_suffix__(date):
        return '{0}-{1}-{2}'.format(date.year, str(date.month).zfill(2), str(date.day).zfill(2))

    @staticmethod
    def __match__(decisions, rewards):
        matched = pd.merge(decisions[['EventId', 'Timestamp']], rewards, how='left', on='EventId')
        return matched[matched['EnqueuedTimeUtc'].notnull()] 

    def __match_files__(self, decisions_path, rewards_path, decisions_chunk):
        fname = '{0}-{1}'.format(os.path.basename(decisions_path), os.path.basename(rewards_path))
        result_path = os.path.join(self.JoinPath, fname)
        if os.path.exists(result_path):
            return pd.read_csv(result_path, parse_dates=['EnqueuedTimeUtc', 'Timestamp'])
        dr = SlimLogs.read_dangling_rewards([rewards_path])[['EventId', 'EnqueuedTimeUtc']].groupby('EventId').min()     
        result = pd.concat(map(lambda decs: StatsContext.__match__(decs, dr), SlimLogs.iterate_decisions([decisions_path], decisions_chunk)))
        result.to_csv(result_path, index=False)
        return result
    
    def __match_filesets__(self, decision_paths, reward_paths, decisions_chunk):
        for decision_path in decision_paths:
            for reward_path in reward_paths:
                yield self.__match_files__(decision_path, reward_path, decisions_chunk)

    def match(self, decision_paths, reward_paths, decisions_chunk):
        return pd.concat(self.__match_filesets__(decision_paths, reward_paths, decisions_chunk))

    def __get_file__(self, prefix, date, reset=False):
        suffix = StatsContext.__get_path_suffix__(date)
        fname = '{0}-{1}.csv'.format(prefix, suffix)
        f_adls = '{0}/{1}'.format(self.AdlsFolder, fname)
        f_local = os.path.join(self.LocalFolder, fname)
        if reset and os.path.exists(f_local):
            os.remove(f_local)
            
        if not os.path.exists(f_local):
            try:
                adls_download(self.__Adls__, f_adls, f_local)
            except Exception as e:
                return None
        return f_local

    def get(self, prefix, first, last = None, reset=False):
        if not last: last = first + datetime.timedelta(days=1)
        return list(filter(lambda s : s is not None,[self.__get_file__(prefix, d) for d in DateTime.range(first, last)]))
