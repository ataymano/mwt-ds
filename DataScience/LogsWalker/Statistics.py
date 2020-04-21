from azure.datalake.store import core, lib, multithread
import itertools
import datetime
from Helpers import DateTime
import pandas as pd
import os

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
    def read_dangling_rewards(path, chunksize = None):
        return pd.read_csv(path, parse_dates=['EnqueuedTimeUtc'], chunksize = chunksize)

    @staticmethod
    def read_decisions(path, chunksize = None):
        return pd.read_csv(path, parse_dates=['Timestamp'], chunksize = chunksize)

class StatsContext:
    def __init__(self, local_folder, adlsClient, app, model=None, adls_folder = 'daily'):
        self.__Adls__ = adlsClient
        self.AdlsFolder = '{0}/{1}'.format(adls_folder, app)
        self.LocalFolder = local_folder
        self.Model = model
        os.makedirs(self.LocalFolder, exist_ok=True)

    @staticmethod
    def __get_path_suffix__(date):
        return '{0}-{1}-{2}'.format(date.year, str(date.month).zfill(2), str(date.day).zfill(2))

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
