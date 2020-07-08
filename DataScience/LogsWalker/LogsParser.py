import pandas as pd
import json
import uuid
import itertools
import pytz

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
                 'VWState': parsed['VWState']['m'],
                 'StringLen': len(line),
                 'Pdrop': 0.0 if 'pdrop' not in parsed else parsed['pdrop']}

        multi = [None] * len(parsed['c']['_multi'])
        for i, o in enumerate(parsed['c']['_multi']):
            multi[i] = {#'Id': o['c']['Id'],
                        'Len': len(json.dumps(o))}

        slots = [None] * len(parsed['_outcomes'])
        for i, o in enumerate(parsed['_outcomes']):
            slots[i] = {'SlotIdx': i,
                    'Cost': o['_label_cost'],
                    'EventId': o['_id'],
                    'ActionsPerSlot': len(o['_a']),
                    'Chosen': o['_a'][0],
                    'Prob': o['_p'][0],
                    'HasObservation': '_o' in o and len(o['_o']) > 0,
                  #  'Product': multi[o['_a'][0]]['Id'],
                    'ChosenActionLen': multi[o['_a'][0]]['Len']}
        
        return session, slots, multi

    @staticmethod
    def ccb_2_cb(session, slots, multi):
        return [dict(session, **s) for s in slots]

    @staticmethod
    def ccb_as_cb_to_stats(df):
        result = df
        result['TimestampFloor'] = result.index.floor('1min')
        result['TimestampFloor'] = result['TimestampFloor'].dt.tz_localize(None)
        result['Observations'] = result['HasObservation'].astype(int).div(1 - result['Pdrop'])
        result['Rewards'] = -result['Cost'].div(1 - result['Pdrop'])
        result['Events'] = 1
        result['EventsLogged'] = result['Events']
        result['Events'] = result['Events'].div(1 - result['Pdrop'])
        result['RewardsSlot1'] = result['Rewards'].mul((result['SlotIdx']==0).astype(int))
        result['EventsSlot1'] = result['Events'].mul((result['SlotIdx']==0).astype(int))
        result['RewardsIps1'] = result['Rewards'].mul((result['SlotIdx']==result['Chosen']).astype(int)).div(result['Prob'])
        result['EventsIps1'] = result['Events'].mul((result['SlotIdx']==result['Chosen']).astype(int)).div(result['Prob'])
        result['RewardsIps1Slot1'] = result['RewardsIps1'].mul((result['SlotIdx']==0).astype(int))
        result['EventsIps1Slot1'] = result['EventsIps1'].mul((result['SlotIdx']==0).astype(int))
        result['RewardsIpsR'] = result['Rewards'].mul(result['ActionsPerSlot']).div(result['Prob'])
        result['EventsIpsR'] = result['Events'].mul(result['ActionsPerSlot']).div(result['Prob'])
        result['RewardsIpsRSlot1'] = result['RewardsIpsR'].mul((result['SlotIdx']==0).astype(int))
        result['EventsIpsRSlot1'] = result['EventsIpsR'].mul((result['SlotIdx']==0).astype(int))

        return result[['TimestampFloor', 'Observations', 'Rewards', 'Events', 'RewardsSlot1', 'EventsSlot1', 'RewardsIps1', 'EventsIps1', 'RewardsIps1Slot1', 'EventsIps1Slot1', 'RewardsIpsR', 'EventsIpsR', 'RewardsIpsRSlot1', 'EventsIpsRSlot1', 'EventsLogged']].reset_index().drop('Timestamp', axis=1).rename(columns = {'TimestampFloor': 'Timestamp'}).groupby('Timestamp').sum()

    @staticmethod
    def ccb_action(line):
        parsed = json.loads(line)
        session = {'Session': parsed['_outcomes'][0]['_id'], 'Timestamp': pd.to_datetime(parsed['Timestamp'])}
        multi = [None] * len(parsed['c']['_multi'])
        for i, o in enumerate(parsed['c']['_multi']):
            multi[i] = {#'Id': o['c']['Id'],
                        'Action': o,
                        'SlotIdx': -1,
                        'Cost': 0}
        for i, o in enumerate(parsed['_outcomes']):
            multi[o['_a'][0]]['SlotIdx'] = i
            multi[o['_a'][0]]['Cost'] = o['_label_cost']
        return [dict(session, **m) for m in multi]      

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
    def ccb_stats(lines):
        events = map(lambda l: DsJson.ccb_2_cb(*DsJson.ccb_event(l)), DsJson.ccb_decision_lines(lines))
        df = pd.DataFrame(itertools.chain(*events))
        return DsJson.ccb_as_cb_to_stats(df.set_index('Timestamp'))

    @staticmethod
    def ccb_actions(lines):
        actions = map(lambda l: DsJson.ccb_action(l), DsJson.ccb_decision_lines(lines))
        df = pd.DataFrame(itertools.chain(*actions))
        return df.set_index('Timestamp')

    @staticmethod
    def contexts(lines):
        return map(lambda e: DsJson.context(e),
            filter(lambda l: DsJson.is_ccb_event(l), lines))
    
    @staticmethod
    def first_timestamp(lines):
        line = next(lines)
        return DsJson.get_timestamp(line)

