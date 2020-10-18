import orjson as json
import uuid
import pandas as pd
from itertools import chain

def is_ccb_event(line):
    return line.startswith('{"Timestamp"')

def is_cb_event(line):
    return line.startswith('{"_label_cost"')

def is_dangling_reward(line):
    return line.startswith('{"RewardValue')

def parse_ccb_stats(line):
    if not is_ccb_event(line):
        return []

    parsed = json.loads(line)
    timestamp = pd.to_datetime(parsed['Timestamp'])
    pdrop = 0.0 if 'pdrop' not in parsed else parsed['pdrop']

    is_active = 1 if not '_skipLearn' in parsed else int(not parsed['_skipLearn'])

    one = 1.0 / (1.0 - pdrop)

    result = [None] * len(parsed['_outcomes'])
    for i, o in enumerate(parsed['_outcomes']):
        reward = -o['_label_cost']
        has_observations = '_o' in o and len(o['_o']) > 0
        chosen = o['_a'][0]
        prob = o['_p'][0]
        actions = len(o['_a'])

        is_slot1 = int(i == 0)
        is_baseline1 = int(i == chosen)

        ips = one / prob
        ips_r = one / (prob * actions)

        result[i] = {'Timestamp': timestamp.floor('5min'),
            'RewardedEvents': int(reward != 0.0) * one,
            'EventsWithObservations': int(has_observations) * one,
            'Rewards': is_active * reward * one,
            'Events': one,
            'RewardsSlot1': is_slot1 * reward * one,
            'EventsSlot1': is_slot1 * one,  
            'RewardsIps1': is_active * is_baseline1 * reward * ips,
            'EventsIps1': is_active * is_baseline1 * ips,
            'NumSquared1': is_active * is_baseline1 * (reward * ips) **2,
            'RewardsIps1Slot1': is_active * is_baseline1 * is_slot1 * reward * ips,
            'EventsIps1Slot1': is_active * is_baseline1 * is_slot1 * ips,
            'NumSquared1Slot1': is_active * is_baseline1 * is_slot1 * (reward * ips) **2,
            'RewardsIpsR': is_active * reward * ips_r,
            'EventsIpsR': is_active  * ips_r,
            'NumSquaredR': is_active * (reward * ips_r) **2,  
            'RewardsIpsRSlot1': is_active * is_slot1 * reward * ips_r,
            'EventsIpsRSlot1': is_active * is_slot1 * ips_r,
            'NumSquaredRSlot1': is_active * is_slot1 * (reward * ips_r) **2,
            'EventsLogged': 1.0,
            'ActivatedEvents': is_active * one                                       
             }

    return result

def ccb_stats(lines):
    return pd.DataFrame(chain.from_iterable([parse_ccb_stats(l) for l in lines])).groupby('Timestamp').sum()
