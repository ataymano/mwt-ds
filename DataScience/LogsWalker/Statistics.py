from azure.datalake.store import core, lib, multithread
import pandas as pd
import os

def adls_download(adls, adlsPath, localPath, buffersize = 4 * 1024 **2, blocksize = 4 * 1024):
    if os.path.exists(localPath):
        os.remove(localPath)
    multithread.ADLDownloader(adls, lpath=localPath, rpath=adlsPath, overwrite=True)


class Statistics:
    @staticmethod
    def concat(substats):
        stats = pd.concat(substats)
        stats['tmp'] = stats.PassRatio * stats.impressions
        stats = stats.drop(['PassRatio'], axis = 1)
        stats = stats.groupby(['hour', 'model']).sum() \
            [['obser', 'clicks', 'impressions', 'clicksIps1', 'impressionsIps1', 'clicksIpsR', 'impressionsIpsR', 'tmp']]
        stats['PassRatio'] = stats.tmp / stats.impressions
        return stats.drop(['tmp'], axis=1)

    def add_baselines(self, stats):
        stats['Online'] = stats.clicks / stats.impressions
        stats['Baseline1'] = stats.clicksIps1 / stats.impressionsIps1
        stats['BaselineR'] = stats.clicksIpsR / stats.impressionsIpsR
        return stats


class StatsContext:
    def __init__(self, local_folder, adlsClient, app, folder = 'daily'):
        self.__Adls__ = adlsClient
        self.AdlsFolder = '{0}/{1}'.format(folder, app)
        self.LocalFolder = local_folder
        os.makedirs(self.LocalFolder, exist_ok=True)

    @staticmethod
    def __get_path_suffix__(year, month, day):
        return '{0}-{1}-{2}'.format(year, str(month).zfill(2), str(day).zfill(2))

    def get_stats(self, year, month, day):
        suffix = StatsContext.__get_path_suffix__(year, month, day)

        stats_h_file = 'statistics-h-{0}.csv'.format(suffix)
        stats_h_adls = '{0}/{1}'.format(self.AdlsFolder, stats_h_file)
        stats_h_local = os.path.join(self.LocalFolder, stats_h_file) 
        adls_download(self.__Adls__, stats_h_adls, stats_h_local)
        return pd.read_csv(stats_h_local, parse_dates=['hour']).drop(['clicks1', 'impressions1'], axis=1).set_index(['hour', 'model'])

    def download_interactions(self, year, month, day):
        suffix = StatsContext.__get_path_suffix__(year, month, day)

        interactions_h_file = 'interactions-{0}.csv'.format(suffix)
        interactions_h_adls = '{0}/{1}'.format(self.AdlsFolder, interactions_h_file)
        interactions_h_local = os.path.join(self.LocalFolder, interactions_h_file) 
        adls_download(self.__Adls__, interactions_h_adls, interactions_h_local)

    def get_dangling_rewards(self, year, month, day):
        suffix = StatsContext.__get_path_suffix__(year, month, day)

        dangling_h_file = 'dangling-{0}.csv'.format(suffix)
        dangling_h_adls = '{0}/{1}'.format(self.AdlsFolder, dangling_h_file)
        dangling_h_local = os.path.join(self.LocalFolder, dangling_h_file) 
        adls_download(self.__Adls__, dangling_h_adls, dangling_h_local)
        return pd.read_csv(dangling_h_local, parse_dates=['EnqueuedTimeUtc']).set_index('EventId')
