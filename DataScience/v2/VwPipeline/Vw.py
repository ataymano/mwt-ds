import sys
import subprocess
import re
import json
import os

from VwPipeline.Pool import SeqPool, MultiThreadPool
from VwPipeline import VwOpts
from VwPipeline import Logger

import multiprocessing

def __safe_to_float__(str, default):
    try:
        return float(str)
    except (ValueError, TypeError):
        return default

# Helper function to extract example counter lines from VW output.
# These lines are preceeded by a single line containing the text:
#   loss     last          counter         weight    label  predict features
# and followed by a blank line
def __extract_counter_lines__(out_lines):
    counter_lines = []
    record = False
    for line in out_lines:
        line = line.strip()
        if record:
            if line == '':
                record = False
            else:
                counter_lines.append(line.split())
        else:
            if line.startswith('loss'):
                fields = line.split()
                if fields[0] == 'loss' and fields[1] == 'last' and fields[2] == 'counter':
                    record = True     
    return counter_lines

# Scan VW output for a table of average loss value per example and log
def __get_loss_per_example__(out_lines):
    average_loss_dict = {}
    since_last_dict = {}
    '''Parse Vowpal Wabbit output, looking for a table of average loss scores, organized by numbers of examples.
    Logs these to the given run as a table row.
    
    Arguments:
    run -- Run object to capture logging.
    out_lines -- List of output lines from Vowpal Wabbit.
    '''
    counter_lines = __extract_counter_lines__(out_lines)
    for counter_line in counter_lines:
        count, average_loss, since_last = counter_line[2], counter_line[0], counter_line[1]
        average_loss_dict[count] = average_loss
        since_last_dict[count] = since_last
    return average_loss_dict, since_last_dict

def __get_final_metrics__(out_lines):
    '''Parse Vowpal Wabbit output, logging any detected metrics to the given Run.
    Looks for text of the form 'metric = value' and logs.
    Treats the metric 'average loss' specially, logging as 'loss' since this is used as a primary metric for Hyperdrive.
    
    Arguments:
    run -- Run object to capture logging.
    out_lines -- List of output lines from Vowpal Wabbit.
    '''
    metrics = {}
    loss = None
    for line in out_lines:
        line = line.strip()
        if '=' in line:
            keyval = line.split('=')
            key = keyval[0].strip()
            val = keyval[1].strip()
            metrics[key] = val
            if key == 'average loss':
                # Include the final loss as the primary metric
                loss = __safe_to_float__(val, None)
    return metrics, loss

def __parse_vw_output__(txt):
    success = False
    lines = txt.split('\n')
    average_loss, since_last = __get_loss_per_example__(lines)
    metrics, loss = __get_final_metrics__(lines)
    success = loss is not None
    return {'loss_per_example': average_loss, 'since_last': since_last, 'metrics': metrics, 'loss': loss}, success

def __save__(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f)

def __load__(path):
    with open(path, 'r') as f:
        return json.load(f)

class VwInput:
    @staticmethod
    def cache(opts, i):
        return {'--cache_file': i, **opts}

    @staticmethod
    def raw(opts, i):
        return {'-d': i, **opts}

class VwResult:
    def __init__(self, loss, populated, metrics):
        self.Loss = loss
        self.Populated = populated
        self.Metrics = metrics

class Vw:
    def __init__(self, path, cache, procs=multiprocessing.cpu_count(), reset=False, norun=False):
        self.Path = path
        self.Cache = cache
        self.Logger = self.Cache.Logger
        self.Pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.Reset = reset
        self.NoRun = norun

    def __generate_command_line__(self, opts):
        return f'{self.Path} {VwOpts.to_string(opts)}'

    def __run__(self, opts: dict):
        command = self.__generate_command_line__(opts)
        Logger.debug(self.Logger, f'Executing: {command}')
        process = subprocess.Popen(
            command.split(),
            universal_newlines=True,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        error = process.communicate()[1]
        Logger.debug(self.Logger, error)        
        parsed, success = __parse_vw_output__(error)
        if not success:
            Logger.critical(self.Logger, f'ERROR: {command}')
            Logger.critical(self.Logger, error)
            raise Exception('Unsuccesful vw execution')
        return parsed

    def run(self, opts_in: dict, opts_out: list):
        populated = {o: self.Cache.get_path(opts_in, o) for o in opts_out}
        metrics_path = self.Cache.get_path(opts_in)

        result_files = list(populated.values()) + [metrics_path]
        not_exist = next((p for p in result_files if not os.path.exists(p)), None)

        opts = dict(opts_in, **populated)

        if self.Reset or not_exist:
            if not_exist:
                Logger.debug(self.Logger, f'{not_exist} had not been found.')
            if self.NoRun:
                raise 'Result is not found, and execution is deprecated'  

            result = self.__run__(opts)
            __save__(result, metrics_path)                          
        else:
            Logger.debug(self.Logger, f'Result of vw execution is found: {VwOpts.to_string(opts)}')
        return __load__(metrics_path), populated

    def __test__(self, inputs, opts_in, opts_out, input_mode):
        opts_populated = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            Logger.info(self.Logger, f'Vw.Test: {inp}, opts_in: {json.dumps(opts_in)}, opts_out: {json.dumps(opts_out)}')
            current_opts = input_mode(opts_in, inp)
            result, populated = self.run(current_opts, opts_out)
            opts_populated[index] = populated
        return VwResult(result['loss'], opts_populated, result)

    def test(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            inputs = [inputs]
        if isinstance(opts_in, list):
            args = [(inputs, point, opts_out, input_mode) for point in opts_in]
            return self.Pool.map(self.__test__, args)            
        return self.__test__(inputs, opts_in, opts_out, input_mode)

    def __train__(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if '-f' not in opts_out:
            opts_out.append('-f')
        opts_populated = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            Logger.info(self.Logger, f'Vw.Train: {inp}, opts_in: {json.dumps(opts_in)}, opts_out: {json.dumps(opts_out)}')
            current_opts = input_mode(opts_in, inp)
            if index > 0:
                current_opts['-i'] = opts_populated[index - 1]['-f']
            result, populated = self.run(current_opts, opts_out)
            opts_populated[index] = populated
        return VwResult(result['loss'], opts_populated, result)     

    def train(self, inputs, opts_in, opts_out=[], input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            inputs = [inputs]
        if isinstance(opts_in, list):
            args = [(inputs, point, opts_out, input_mode) for point in opts_in]
            return self.Pool.map(self.__train__, args)            
        return self.__train__(inputs, opts_in, opts_out, input_mode)   

    def cache(self, inputs, opts):
        if not isinstance(inputs, list):
            inputs = [inputs]
        return self.test(inputs, {'#cmd': VwOpts.to_cache_cmd(opts)}, ['--cache_file'])
