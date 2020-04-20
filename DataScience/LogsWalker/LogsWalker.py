from LogsParser import DsJson
from Statistics import StatsContext, Statistics

import os.path
import shutil
import datetime
import pandas as pd
import itertools
import json


class FileHelpers:
    @staticmethod
    def find_last_eof(path):
        result = -1
        with open(path, 'rb') as f:
            f.seek(-1, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
                result = result - 1
            return result

    @staticmethod
    def find_first_eof(path):
        with open(path, 'rb') as f:
            f.readline()
            return f.tell()

    @staticmethod
    def cmp_files(f1, f2, start_range_f1=0, start_range_f2=0, erase_checkpoint_line=True):
        with open(f1, 'rb+' if erase_checkpoint_line else 'rb') as fp1, open(f2, 'rb') as fp2:
            if start_range_f1 != 0:
                fp1.seek(start_range_f1, os.SEEK_SET if start_range_f1 > 0 else os.SEEK_END)
            if start_range_f2 != 0:
                fp2.seek(start_range_f2, os.SEEK_SET if start_range_f2 > 0 else os.SEEK_END)

            prev_b1 = b''
            while True:
                b1 = fp1.read(1)
                if b1 != fp2.read(1):
                    # if erase_checkpoint_line=True and b1 != b2 only due to checkpoint info line,
                    # then data is still valid. Checkpoint info line is removed
                    if erase_checkpoint_line and prev_b1 + b1 == b'\n[':
                        fp1.seek(-2, os.SEEK_CUR)
                        fp1.truncate()
                        return True
                    return False
                if not b1:
                    return True
                prev_b1 = b1


class AzureStorageBlob:
    def __init__(self, bbs, container, path):
        self.__Bbs__ = bbs
        self.Container = container
        self.Path = path
        self.MaxConnections = 4

    def download(self, path, start_offset, end_offset):
        self.__Bbs__.get_blob_to_path(self.Container, self.Path, path,
            start_range=start_offset, end_range=end_offset, max_connections = self.MaxConnections)

    def get_size(self):
        bp = self.__Bbs__.get_blob_properties(self.Container, self.Path)
        return bp.properties.content_length


class AzureStorageBlobView:
    def __init__(self, azure_storage_blob, local_folder, start_offset, end_offset, temporary=False):
        self.__Asb__ = azure_storage_blob
        self.Path = os.path.join(local_folder, 'view.{0}-{1}'.format(start_offset, end_offset))
        if not os.path.exists(self.Path):
            self.__Asb__.download(self.Path, start_offset, end_offset)
        self.__PreviousPath__ = '{0}.previous'.format(self.Path)
        self.__FirstEof__ = FileHelpers.find_first_eof(self.Path)
        self.__LastEof__ = FileHelpers.find_last_eof(self.Path)
        self.__StartOffset__ = start_offset
        self.__EndOffset__ = end_offset
        self.__Temporary__ = temporary

    def __del__(self):
        if self.__Temporary__:
            os.remove(self.Path)
            if os.path.exists(self.__PreviousPath__):
                os.remove(self.__PreviousPath__)

    def __get_previous__(self):
        if os.path.exists(self.__PreviousPath__):
            with open(self.__PreviousPath__, 'r', encoding='utf-8') as f:
                return f.read()
        return None

    def set_previous(self, previous):
        with open(self.__PreviousPath__, 'w', encoding='utf-8') as f:
            f.write(previous)

    def get_server_offset(self, aligned=True):
        if not aligned:
            return self.__StartOffset__

        previous = self.__get_previous__()
        if previous is None:
            return self.__StartOffset__ + self.__FirstEof__

    def read(self):
        current = self.__get_previous__()
        with open(self.Path, 'r', encoding='utf-8') as f:
            first = f.readline()
            if current is not None:
                current = current + first
            for l in f:
                if current is not None:
                    yield current
                current = l

    def align(self):
        aligned_path = '{0}.{1}'.format(self.Path, 'aligned')
        with open(self.Path, 'rb') as f1:
            f1.seek(self.__FirstEof__)
            with open(aligned_path, 'ab') as f2:
                shutil.copyfileobj(f1, f2, length=100 * 1024 ** 2)
                f2.truncate(os.path.getsize(self.Path) + self.__LastEof__ - self.__FirstEof__)
        return aligned_path


class AzureDsJsonTimestampIndex:
    def __init__(self, azure_storage_blob, folder):
        self.__Asb__ = azure_storage_blob
        self.__Folder__ = folder
        self.__TmpFolder__ = os.path.join(self.__Folder__, 'tmp')
        self.__IndexPath__ = os.path.join(self.__Folder__, 'index')
        os.makedirs(self.__TmpFolder__, exist_ok=True)

    def __get_offset_info___(self, offset, chunk_size=1024 ** 2):
        view = DsJsonDayView(
            AzureStorageBlobView(self.__Asb__, self.__TmpFolder__, offset, offset + chunk_size, temporary=True))
        return view.Impl.get_server_offset(), view.get_timestamp()

    def __get_index__(self):
        if os.path.exists(self.__IndexPath__):
            with open(self.__IndexPath__, 'r') as f: 
                return dict(json.load(f))
        return {}

    def __update_index__(self, index):
        with open(self.__IndexPath__, 'w') as f:
            json.dump(list(index.items()), f)

    @staticmethod
    def __init_lookup_range__(dt, index):
        import bisect
        import pytz
        ts = dt.timestamp()
        keys = list(index.keys()) if len(index) > 0 else []
        keys.sort()
        pos = bisect.bisect_left(keys, ts)
        last = index[keys[pos]] if pos < len(keys) else None
        first = index[keys[pos - 1]] if pos > 0 else 0
        first_dt = date = datetime.datetime.fromtimestamp(ts, tz=pytz.utc) if pos > 0 else None
        return first, last, first_dt

    def lookup(self, timestamp, tolerance=datetime.timedelta(minutes=5), offset_limit=64 * 1024 ** 2,
               iterations_limit=10):
        index = self.__get_index__()
        _min, _max, _min_dt = AzureDsJsonTimestampIndex.__init_lookup_range__(timestamp, index)
        if _min_dt and timestamp - _min_dt < tolerance:
            return _min
        
        if not _max:
            _max = self.__Asb__.get_size()

        i = 0
        while _max - _min > offset_limit:
            candidate = int(round((_min + _max) / 2))
            candidate_offset, candidate_ts = self.__get_offset_info___(candidate)
            index[candidate_ts.timestamp()] = candidate_offset
            if candidate_ts > timestamp:
                _max = candidate_offset
            else:
                _min = candidate_offset
                delta = timestamp - candidate_ts
                if delta < tolerance or i >= iterations_limit:
                    break
            i += 1
        self.__update_index__(index)
        return _min


class DsJsonDayView:
    def __init__(self, azure_storage_blob_view):
        self.Impl = azure_storage_blob_view

    def dangling_reward_lines(self):
        return filter(lambda l: DsJson.is_dangling_reward(l), self.Impl.read())

    def ccb_decision_lines(self):
        return filter(lambda l: DsJson.is_ccb_event(l), self.Impl.read())

    def dangling_rewards(self):
        df = pd.DataFrame(
            map(lambda l: DsJson.get_dangling_reward(l), self.dangling_reward_lines()))
        return df.set_index('Timestamp')

    def ccb_events(self):
        events = map(lambda l: DsJson.ccb_2_cb(*DsJson.get_ccb_event(l)), self.ccb_decision_lines())
        df = pd.DataFrame(itertools.chain(*events))
        return df.set_index('Timestamp')

    def contexts(self):
        return map(lambda e: DsJson.get_context(e),
            filter(lambda l: DsJson.is_ccb_event(l), self.Impl.read()))

    def read(self):
        return self.Impl.read()

    def get_timestamp(self):
        line = next(self.read())
        return DsJson.get_timestamp(line)


class DsJsonDayClient:
    def __init__(self, bbs, app, folder, date, local_folder):
        self.Date = date
        self.App = app

        azure_path = '{0}/data/{1}/{2}/{3}_0.json'.format(folder, date.year, str(date.month).zfill(2), str(date.day).zfill(2))

        self.__Asb__ = AzureStorageBlob(bbs, app, azure_path)
        self.Folder = local_folder
        os.makedirs(self.Folder, exist_ok=True)

        self.__TmpFolder__ = os.path.join(self.Folder, 'tmp')
        os.makedirs(self.__TmpFolder__, exist_ok=True)

        self.__Index__ = AzureDsJsonTimestampIndex(self.__Asb__, self.Folder)

    def get_view(self, start_offset, size=8 * 1024 ** 2):
        return DsJsonDayView(AzureStorageBlobView(self.__Asb__, self.Folder, start_offset, start_offset + size))

    def get_size(self):
        return self.__Asb__.get_size()

    def lookup(self, hours, minutes, tolerance=datetime.timedelta(minutes=5), offset_limit=64 * 1024 ** 2,
               iterations_limit=10):
        import pytz
        dt = datetime.datetime(self.Date.year, self.Date.month, self.Date.may, hours, minutes, 0, tzinfo=pytz.utc)
        return self.__Index__.lookup(dt, tolerance, offset_limit, iterations_limit)

    def overview(self):
        size = self.get_size()
        _, last_ts = self.__Index__.__get_offset_info___(size - 1024 ** 2, 1024 ** 2 - 1)
        return pd.DataFrame([{'App': self.App,
                              'Day': self.Date,
                              'Size': '{:.2f} GB'.format(size / (1024 ** 3)),
                              'LastTimestamp': last_ts}])


class AppContext:
    def __init__(self, workspace_folder, bbs, app, folder = None, adlsClient = None):
        self.Bbs = bbs
        self.App = app
        if folder:
            self.Folder = folder
        else:
            self.Folder = self.__get_latest_folder__(self.Bbs, self.App)
            print('Latest folder was resolved as: {0}'.format(self.Folder))

        self.AppFolder = os.path.join(workspace_folder, app, self.Folder)
        os.makedirs(self.AppFolder, exist_ok=True)
        self.AdlsClient = adlsClient

    def get_day(self, date):
        day_folder = os.path.join(self.AppFolder, '{0}-{1}-{2}'.format(date.year, str(date.month).zfill(2), str(date.day).zfill(2)))
        return DsJsonDayClient(self.Bbs, self.App, self.Folder, date, day_folder)

    @staticmethod
    def __get_latest_folder__(bbs, container):
        folders = bbs.list_blobs(container, delimiter='/')
        folder = max([d.name[:-1] for d in filter(lambda d: d.name[-1] == '/', folders)])
        return folder

    def get_stats(self):
        return StatsContext(os.path.join(self.Folder, 'stats'), self.AdlsClient, self.App)

class Workspace:
    def __init__(self, folder, bbs = None, adls = None):
        self.Bbs = bbs
        self.Adls = adls
        self.Folder = folder

    def get_app(self, app, folder = None):
        return AppContext(self.Folder, self.Bbs, app, folder, self.Adls)

    
