from azure.datalake.store import core, lib, multithread
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
    def get_stats(path):
        return pd.read_csv(path, parse_dates=['hour']).drop(['clicks1', 'impressions1'], axis=1).set_index(['hour'])

    @staticmethod
    def concat(substats):
        stats = pd.concat(substats)
        stats['tmp'] = stats.PassRatio * stats.impressions
        stats = stats.drop(['PassRatio'], axis = 1)
        stats = stats.groupby(['hour', 'model']).sum() \
            [['obser', 'clicks', 'impressions', 'clicksIps1', 'impressionsIps1', 'clicksIpsR', 'impressionsIpsR', 'tmp']]
        stats['PassRatio'] = stats.tmp / stats.impressions
        return stats.drop(['tmp'], axis=1).reset_index(['model'])

    def add_baselines(self, stats):
        stats['Online'] = stats.clicks / stats.impressions
        stats['Baseline1'] = stats.clicksIps1 / stats.impressionsIps1
        stats['BaselineR'] = stats.clicksIpsR / stats.impressionsIpsR
        return stats

class SlimLogs:
    @staticmethod
    def get_dangling_rewards(path):
        return pd.read_csv(path, parse_dates=['EnqueuedTimeUtc'])

    @staticmethod
    def get_decisions(path):
        return pd.read_csv(path, parse_dates=['Timestamp'])

class StatsContext:
    def __init__(self, local_folder, adlsClient, app, model=None, adls_folder = 'daily'):
        self.__Adls__ = adlsClient
        self.AdlsFolder = '{0}/{1}'.format(adls_folder, app)
        self.LocalFolder = local_folder
        self.Model = model
        os.makedirs(self.LocalFolder, exist_ok=True)

    def __filter_by_model__(self, df):
        return df if self.Model == None else df[df.model == int(self.Model)]

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

    def download_stats(self, first, last = None):
        if not last: last = first + datetime.timedelta(days=1)
        for d in DateTime.range(first, last):
            self.__get_file__('statistics-h', d, reset=True)

    def get_stats(self, first, last = None):
        if not last: last = first + datetime.timedelta(days=1)
        return self.__filter_by_model__(Statistics.concat([Statistics.get_stats(p) for p in
            filter(lambda s : s is not None,[self.__get_file__('statistics-h', d) for d in DateTime.range(first, last)])]))

    def download_decisions(self, first, last = None):
        if not last: last = first + datetime.timedelta(days=1)
        for d in DateTime.range(first, last):
            self.__get_file__('interactions', d, reset=True)

    def get_decisions(self, first, last = None):
        if not last: last = first + datetime.timedelta(days=1)
        return self.__filter_by_model__(pd.concat([SlimLogs.get_decisions(p) for p in 
            filter(lambda s : s is not None,[self.__get_file__('interactions', d) for d in DateTime.range(first, last)])]))

    def download_dangling_rewards(self, first, last = None):
        if not last: last = first + datetime.timedelta(days=1)
        for d in DateTime.range(first, last):
            self.__get_file__('dangling', d, reset=True)

    def get_dangling_rewards(self, first, last = None):
        if not last: last = first + datetime.timedelta(days=1)
        return self.__filter_by_model__(pd.concat([SlimLogs.get_dangling_rewards(p) for p in 
            filter(lambda s : s is not None,[self.__get_file__('dangling', d) for d in DateTime.range(first, last)])]))

