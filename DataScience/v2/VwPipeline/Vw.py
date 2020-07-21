import sys
import subprocess
import re
import json

from VwPipeline.Core import Workspace, DummyWorkspace
from VwPipeline.Pool import SeqPool, MultiThreadPool

import multiprocessing

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
    def __init__(self, path, workspace=None, procs=multiprocessing.cpu_count()):
        self.Path = path
        self.Ws = workspace if workspace else DummyWorkspace()
        self.Pool = SeqPool() if procs == 1 else MultiThreadPool(procs)

    @staticmethod
    def __safe_to_float__(str, default):
        try:
            return float(str)
        except (ValueError, TypeError):
            return default

    # Helper function to extract example counter lines from VW output.
    # These lines are preceeded by a single line containing the text:
    #   loss     last          counter         weight    label  predict features
    # and followed by a blank line
    @staticmethod
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
    @staticmethod
    def __get_loss_per_example__(out_lines):
        average_loss_dict = {}
        since_last_dict = {}
        '''Parse Vowpal Wabbit output, looking for a table of average loss scores, organized by numbers of examples.
        Logs these to the given run as a table row.
        
        Arguments:
        run -- Run object to capture logging.
        out_lines -- List of output lines from Vowpal Wabbit.
        '''
        counter_lines = Vw.__extract_counter_lines__(out_lines)
        for counter_line in counter_lines:
            count, average_loss, since_last = counter_line[2], counter_line[0], counter_line[1]
            average_loss_dict[count] = average_loss
            since_last_dict[count] = since_last
        return average_loss_dict, since_last_dict

    @staticmethod
    def __get_final_metrics__(out_lines):
        '''Parse Vowpal Wabbit output, logging any detected metrics to the given Run.
        Looks for text of the form 'metric = value' and logs.
        Treats the metric 'average loss' specially, logging as 'loss' since this is used as a primary metric for Hyperdrive.
        
        Arguments:
        run -- Run object to capture logging.
        out_lines -- List of output lines from Vowpal Wabbit.
        '''
        for line in out_lines:
            line = line.strip()
            if '=' in line:
                keyval = line.split('=')
                key = keyval[0].strip()
                val = keyval[1].strip()
                if key == 'average loss':
                    # Include the final loss as the primary metric
                    return Vw.__safe_to_float__(val, None)

    @staticmethod
    def __parse_vw_output__(txt):
        success = False
        lines = txt.split('\n')
        average_loss, since_last = Vw.__get_loss_per_example__(lines)
        loss = Vw.__get_final_metrics__(lines)
        success = loss is not None
        return {'loss_per_example': average_loss, 'since_last': since_last, 'loss': loss}, success

    @staticmethod
    def __to_str__(opts):
        return ' '.join(['{0} {1}'.format(key, opts[key]) if not key.startswith('#')
            else str(opts[key]) for key in sorted(opts.keys())])

    def __generate_command_line__(self, opts):
        command = '{0} {1}'.format(self.Path, Vw.__to_str__(opts))
        return re.sub(' +', ' ', command)

    def __populate__(self, prefix, opts_in, opts_out):
        sorted_opts_out = sorted(opts_out)
        seed = self.__generate_command_line__(opts_in) + '-'.join(sorted_opts_out)
        result = {}
        for o in opts_out:
            result[o] = self.Ws.__make_path__('{0}.{1}'.format(prefix, o), [seed])
        return result

    def __run__(self, command):
        self.Ws.Logger.debug('Executing: {0}'.format(command))
        process = subprocess.Popen(
            command.split(),
            universal_newlines=True,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        error = process.communicate()[1]
        self.Ws.Logger.debug(error)        
        parsed, success = Vw.__parse_vw_output__(error)
        if not success:
            self.Ws.Logger.critical('ERROR: {0}'.format(command))
            self.Ws.Logger.critical(error)
            raise Exception('Unsuccesful vw execution')
        return parsed

    def run(self, opts):
        cmd = self.__generate_command_line__(opts)
        return self.Ws.run(getattr(self, '__run__'), cmd)

    def __test__(self, inputs, opts_in, opts_out, input_mode):
        populated = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            self.Ws.Logger.info('Vw.Test: {0}, opts_in: {1}, opts_out: {2}'.format(inp, json.dumps(opts_in), json.dumps(opts_out)))
            current_opts = input_mode(opts_in, inp)
            populated[index] = self.__populate__('Vw.Test', current_opts, opts_out)
            current_opts = dict(current_opts, **populated[index])
            result = self.run(current_opts)
        return VwResult(result['loss'], populated, result)

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
        populated = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            self.Ws.Logger.info('Vw.Train: {0}, opts_in: {1}, opts_out: {2}'.format(inp, json.dumps(opts_in), json.dumps(opts_out)))
            current_opts = input_mode(opts_in, inp)
            if index > 0:
                current_opts['-i'] = populated[index - 1]['-f']
            populated[index] = self.__populate__('Vw.Train', current_opts, opts_out)
            current_opts = dict(current_opts, **populated[index])
            result = self.run(current_opts)
        return VwResult(result['loss'], populated, result)     

    def train(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            inputs = [inputs]
        if isinstance(opts_in, list):
            args = [(inputs, point, opts_out, input_mode) for point in opts_in]
            return self.Pool.map(self.__train__, args)            
        return self.__train__(inputs, opts_in, opts_out, input_mode)    
