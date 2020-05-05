import pandas as pd
import json
import uuid
import itertools

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
        try:
            o = json.loads(line)
        except:
            return False
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
    def context(line):
        parsed = json.loads(line)
        return json.dumps(parsed['c']) + '\n'

    @staticmethod
    def dangling_reward(line):
        parsed = json.loads(line[:-2])
        return {'Timestamp': pd.to_datetime(parsed['EnqueuedTimeUtc']), 'EventId': parsed['EventId'], 'Reward': parsed['RewardValue']}

    @staticmethod
    def ccb_event(line):
        parsed = json.loads(line)
        session = {'Session': str(uuid.uuid4()),
                 'Timestamp': pd.to_datetime(parsed['Timestamp']),
                 'NumActions': len(parsed['c']['_multi']),
                 'NumSlots': len(parsed['c']['_slots']),
                 'VWState': parsed['VWState']['m']}

        multi = [None] * len(parsed['c']['_multi'])
   #     for i, o in enumerate(parsed['c']['_multi']):
   #         multi[i] = {'Id': o['Id']}

        slots = [None] * len(parsed['_outcomes'])
        for i, o in enumerate(parsed['_outcomes']):
            slots[i] = {'SlotIdx': i,
                    'Cost': o['_label_cost'],
                    'EventId': o['_id'],
                    'ActionsPerSlot': len(o['_a']),
                    'Chosen': o['_a'][0],
                    'Prob': o['_p'][0]}
        
        return session, slots, multi

    @staticmethod
    def ccb_2_cb(session, slots, multi):
        return [dict(session, **s) for s in slots]

    @staticmethod
    def dangling_reward_lines(lines):
        return filter(lambda l: DsJson.is_dangling_reward(l), lines)

    @staticmethod
    def ccb_decision_lines(lines):
        return filter(lambda l: DsJson.is_ccb_event(l), lines)
    
    @staticmethod
    def dangling_rewards(lines):
        df = pd.DataFrame(
            map(lambda l: DsJson.dangling_reward(l), DsJson.dangling_reward_lines(lines)))
        return df.set_index('Timestamp')

    @staticmethod
    def ccb_events(lines):
        events = map(lambda l: DsJson.ccb_2_cb(*DsJson.ccb_event(l)), DsJson.ccb_decision_lines(lines))
        df = pd.DataFrame(itertools.chain(*events))
        return df.set_index('Timestamp')

    @staticmethod
    def contexts(lines):
        return map(lambda e: DsJson.context(e),
            filter(lambda l: DsJson.is_ccb_event(l), lines))
    
    @staticmethod
    def first_timestamp(lines):
        line = next(lines)
        return DsJson.get_timestamp(line)

