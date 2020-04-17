import pandas as pd


class NaiveJson:
    def __init__(self, line):
        self.Line = line

    def get_string(self, key):
        key_end = self.Line.find('"{0}"'.format(key)) + len(key) + 2
        value_start = self.Line.find('"', key_end) + 1
        value_end = self.Line.find('"', value_start)
        return self.Line[value_start:value_end]


class CcbEvent:
    def __init__(self, line):
        self.Obj = NaiveJson(line)

    def get_timestamp(self):
        return pd.to_datetime(self.Obj.get_string("Timestamp"))


class DanglingReward:
    def __init__(self, line):
        self.Obj = NaiveJson(line)

    def get_enqueued_time_utc(self):
        return pd.to_datetime(self.Obj.get_string("EnqueuedTimeUtc"))


class DsJson:
    @staticmethod
    def get_timestamp(line):
        if line.startswith('{"RewardValue'):
            return DanglingReward(line).get_enqueued_time_utc()
        return CcbEvent(line).get_timestamp()
