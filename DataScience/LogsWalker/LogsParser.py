import pandas as pd
import json
import uuid

class NaiveJson:
    def __init__(self, line):
        self.Line = line

    def get_string(self, key):
        key_end = self.Line.find('"{0}"'.format(key)) + len(key) + 2
        value_start = self.Line.find('"', key_end) + 1
        value_end = self.Line.find('"', value_start)
        return self.Line[value_start:value_end]


class DsJson:
    @staticmethod
    def is_ccb_event(line):
        return line.startswith('{"Timestamp"')

    @staticmethod
    def is_cb_event(line):
        return line.startswith('{"_label_cost"')

    @staticmethod
    def is_dangling_reward(line):
        return line.startswith('{"RewardValue')

    @staticmethod
    def get_timestamp(line):
        obj = NaiveJson(line)
        if line.startswith('{"RewardValue'):
            return pd.to_datetime(obj.get_string("EnqueuedTimeUtc"))
        return pd.to_datetime(obj.get_string("Timestamp"))

    @staticmethod
    def get_context(line):
        parsed = json.loads(line)
        return json.dumps(parsed['c']) + '\n'

    @staticmethod
    def get_dangling_reward(line):
        parsed = json.loads(line[:-2])
        return {'Timestamp': pd.to_datetime(parsed['EnqueuedTimeUtc']), 'EventId': parsed['EventId'], 'Reward': parsed['RewardValue']}

    @staticmethod
    def get_ccb_event(line):
        parsed = json.loads(line)
        session = {'Session': str(uuid.uuid4()),
                 'Timestamp': pd.to_datetime(parsed['Timestamp']),
                 'NumActions': len(parsed['c']['_multi']),
                 'NumSlots': len(parsed['c']['_slots']),
                 'VWState': parsed['VWState']['m']}
        slots = [None] * len(parsed['_outcomes'])
        for i, o in enumerate(parsed['_outcomes']):
            slots[i] = {'SlotIdx': i,
                    'Cost': o['_label_cost'],
                    'EventId': o['_id'],
                    'ActionsPerSlot': len(o['_a']),
                    'Chosen': o['_a'][0],
                    'Prob': o['_p'][0]}
        return session, slots

    @staticmethod
    def ccb_2_cb(session, slots):
        return [dict(session, **s) for s in slots]
