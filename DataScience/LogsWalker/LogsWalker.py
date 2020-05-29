from Core import Context, Data
from PostProc import Statistics, SlimLogs
from Helpers import AzureStorage, AzureStorageBlob, File, DateTime, Adls
from LogsParser import DsJson

import pandas as pd
import os
import datetime
import json

class Workspace(Context):
    def __init__(self, path, bbs=None, adls=None, debug=False):
        super().__init__(path, parent=None, reset=False, debug=debug)
        self.__Bbs__ = bbs
        self.__Adls__ = adls

    def Bbs(self):
        if not self.__Bbs__:
            raise Exception('Block blob storage client was not initialized')
        return self.__Bbs__

    def Adls(self):
        if not self.__Adls__:
            raise Exception('ADLS client was not initialized')
        return self.__Adls__

    def get_app(self, app):
        return AppContext(self, app)

class AppContext(Context):
    def __init__(self, workspace, app):
        super().__init__(app, workspace)
        self.App = app

    def get_instance(self, model=None):
        return InstanceContext(self, model)

    def get_stats(self, start_date, end_date, prefix='statistics', time_column='Timestamp', adls_path='daily'):
        models = AzureStorage.get_app_folders(self.Parent.Bbs(), self.App)
        result = []
        for m in models:
            stats = self.get_instance(m).get_stats(start_date, end_date)
            if stats is not None:
                result.append((m, stats))
        return result

class InstanceContext(Context):
    def __init__(self, app_context, model=None):
        if not model:
            model = AzureStorage.get_latest_app_folder(app_context.Parent.Bbs(), app_context.App)
        super().__init__(model, app_context)
        self.Model=int(model)

    def get_day(self, date = None):
        if not date:
            date = max(AzureStorage.get_days(self.Parent.Parent.Bbs(), self.Parent.App, self.Model))
        return DayContext(self, date)

    def get_dangling_rewards(self, start_date, end_date, prefix='dangling', adls_path='daily'):
        postproc = PostProcContext(self, adls_path)
        return postproc.get_dangling_rewards(start_date, end_date, prefix)

    def get_stats(self, start_date, end_date, prefix='statistics', time_column='Timestamp', adls_path='daily'):
        postproc = PostProcContext(self, adls_path)
        return postproc.get_stats(start_date, end_date, prefix, time_column)

    def overview(self):
        days = AzureStorage.get_days(self.Parent.Parent.Bbs(), self.Parent.App, self.Model)
        day = self.get_day(max(days))
        size = day.get_size()
        _, last_ts = day.__Index__.__get_offset_info___(size - 1024 ** 2, 1024 ** 2 - 1)
        return pd.DataFrame([{'App': self.Parent.App,
                              'Model': self.Model,
                              'First day': min(days),
                              'Last day': max(days),
                              'Last day size': '{:.2f} GB'.format(size / (1024 ** 3)),
                              'Last timestamp': last_ts}])

class DayContext(Context):
    def __init__(self, instance_context, date):
        super().__init__(str(date), instance_context)
        self.Date = date
        azure_path = '{0}/data/{1}/{2}/{3}_0.json'.format(instance_context.Model, date.year, str(date.month).zfill(2), str(date.day).zfill(2))
        self.Asb = AzureStorageBlob(self.Parent.Parent.Parent.Bbs(), self.Parent.Parent.App, azure_path, log=self.log)
        self.__Index__ = DayIndex(self)

    def get_size(self):
        return self.Asb.get_size()

    def get_segment(self, start, size=None, temporary=False, with_progress=True):
        return DaySegment(self, start, None if not size else start + size, temporary, with_progress)

    def lookup(self, hours, minutes, tolerance=datetime.timedelta(minutes=5), offset_limit=64 * 1024 ** 2,
               iterations_limit=10):
        import pytz
        dt = datetime.datetime(self.Date.year, self.Date.month, self.Date.day, hours, minutes, 0, tzinfo=pytz.utc)
        return self.__Index__.lookup(dt, tolerance, offset_limit, iterations_limit)

    def overview(self):
        size = self.get_size()
        _, last_ts = self.__Index__.__get_offset_info___(size - 1024 ** 2, 1024 ** 2 - 1)
        return pd.DataFrame([{'App': self.Parent.Parent.App,
                              'Model': self.Parent.Model,
                              'Day': self.Date,
                              'Size': '{:.2f} GB'.format(size / (1024 ** 3)),
                              'LastTimestamp': last_ts}])

class PostProcContext(Context):
    def __init__(self, instance_context, adls_path):
        super().__init__('postproc', instance_context)
        self.AdlsPath = adls_path

    def get(self, prefix, date):
        return PostProcData(self, prefix, date)

    def get_dangling_rewards(self, start_date, end_date, prefix='dangling'):
        all = [self.get(prefix, d) for d in DateTime.range(start_date, end_date)]
        ready = list(filter(lambda s: s.Exists, all))
        paths = [s.FullPath for s in ready]
        return None if len(paths) == 0 else SlimLogs.dangling_rewards(paths)
    
    def get_stats(self, start_date, end_date, prefix='statistics', time_column='Timestamp'):
        all = [self.get(prefix, d) for d in DateTime.range(start_date, end_date)]
        ready = list(filter(lambda s: s.Exists, all))
        paths = [s.FullPath for s in ready]
        return None if len(paths) == 0 else Statistics.read_stats(paths, time_column=time_column)


class PostProcData(Data):
    def __init__(self, postproc_context, prefix, date):
        super().__init__('{0}-{1}.csv'.format(prefix, str(date)), postproc_context)

    def __load__(self):
        path_server = '{0}/{1}/{2}/{3}'.format(self.Context.AdlsPath, self.Context.Parent.Parent.App,
            self.Context.Parent.Model, self.Path)
        return Adls.adls_download(self.Context.Parent.Parent.Parent.Adls(), path_server, self.FullPath, log=self.Context.log)

class DaySegment(Data):
    def __init__(self, day_context, start_offset, end_offset, temporary=False, with_progress=False):
        self.StartOffset = start_offset
        self.EndOffset = end_offset
        self.WithProgress = with_progress
        super().__init__('view.{0}-{1}'.format(start_offset, end_offset), day_context)
        self.__Temporary__ = temporary
        if self.Exists:
            self.__FirstEof__ = File.find_first_eof(self.FullPath)
            self.__LastEof__ = File.find_last_eof(self.FullPath)

    def __load__(self):
        tmp = '{0}.tmp'.format(self.FullPath)
        if self.Context.Asb.download(tmp, self.StartOffset, self.EndOffset, self.WithProgress):
            os.rename(tmp, self.FullPath)
            return True
        return False

    def __del__(self):
        if self.__Temporary__ and self.Exists:
            os.remove(self.FullPath)

    def get_server_offset(self, aligned=True):
        return self.StartOffset if not aligned else self.StartOffset + self.__FirstEof__

    def read(self, previous = None):
        current = previous
        with open(self.FullPath, 'r', encoding='utf-8') as f:
            first = f.readline()
            if current is not None:
                current = current + first
            for l in f:
                if current is not None:
                    yield current
                current = l

    def save(self, name, dataframe):
        dataframe.to_csv('{0}.{1}'.format(self.FullPath, name))

    def load(self, name):
        return pd.read_csv('{0}.{1}'.format(self.FullPath, name))

class DayIndex(Data):
    def __init__(self, day_context):
        self.Index = {}
        super().__init__('index', day_context)
        if os.path.exists(self.FullPath):
            with open(self.FullPath, 'r') as f: 
                self.Index = dict(json.load(f))

    def __load__(self):
        return True

    def __save__(self):
        with open(self.FullPath, 'w') as f:
            json.dump(list(self.Index.items()), f)

    def __get_offset_info___(self, offset, chunk_size=1024 ** 2):
        segment = DaySegment(self.Context, offset, offset + chunk_size, temporary=True)
        return segment.get_server_offset(), DsJson.first_timestamp(segment.read())

    def __init_lookup_range__(self, dt):
        import bisect
        import pytz
        ts = dt.timestamp()
        keys = list(self.Index.keys()) if len(self.Index) > 0 else []
        keys.sort()
        pos = bisect.bisect_left(keys, ts)
        last = self.Index[keys[pos]] if pos < len(keys) else None
        first = self.Index[keys[pos - 1]] if pos > 0 else 0
        first_dt = datetime.datetime.fromtimestamp(keys[pos - 1], tz=pytz.utc) if pos > 0 else None
        return first, last, first_dt

    def lookup(self, timestamp, tolerance=datetime.timedelta(minutes=5), offset_limit=64 * 1024 ** 2,
               iterations_limit=10):
        _min, _max, _min_dt = self.__init_lookup_range__(timestamp)
        if _min_dt and timestamp - _min_dt < tolerance:
            return _min
        
        if not _max:
            _max = self.Context.get_size()

        i = 0
        while _max - _min > offset_limit:
            candidate = int(round((_min + _max) / 2))
            candidate_offset, candidate_ts = self.__get_offset_info___(candidate)
            self.Index[candidate_ts.timestamp()] = candidate_offset
            if candidate_ts > timestamp:
                _max = candidate_offset
            else:
                _min = candidate_offset
                delta = timestamp - candidate_ts
                if delta < tolerance or i >= iterations_limit:
                    break
            i += 1
        self.__save__()
        return _min

