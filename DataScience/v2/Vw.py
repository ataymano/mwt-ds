import sys
import subprocess
import re
import json

from Core import Workspace, DummyWorkspace

class VwInput:
    @staticmethod
    def cache(opts, i):
        return {'--cache_file': i, **opts}

    @staticmethod
    def raw(opts, i):
        return {'-d': i, **opts}

class VwResult:
    def __init__(self, loss, populated):
        self.Loss = loss
        self.Populated = populated

class Vw:
    def __init__(self, path, workspace = None):
        self.Path = path
        self.Ws = workspace if workspace else DummyWorkspace()

    @staticmethod
    def __safe_to_float__(str, default):
        try:
            return float(str)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def __parse_vw_output__(txt):
        result = {}
        success = False
        for line in txt.split('\n'):
            if '=' in line:
                index = line.find('=')
                key = line[0:index].strip()
                value = line[index + 1:].strip()
                if key == "average loss":
                    result[key] = Vw.__safe_to_float__(value, None)
                    success = result[key] is not None
        return result, success

    def __generate_command_line__(self, opts):
        keys = list(opts.keys())
        keys.sort()
        command = '{0} {1}'.format(self.Path,
                ' '.join(['{0} {1}'.format(key, opts[key]) if not key.startswith('#')
                else str(opts[key]) for key in keys]))

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

    def test(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            raise 'inputs should be list of paths'
        populated = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            self.Ws.Logger.info('Vw.Test: {0}, opts_in: {1}, opts_out: {2}'.format(inp, json.dumps(opts_in), json.dumps(opts_out)))
            current_opts = input_mode(opts_in, inp)
            populated[index] = self.__populate__('Vw.Test', current_opts, opts_out)
            current_opts = dict(current_opts, **populated[index])
            result = self.run(current_opts)
        return VwResult(result['average loss'], populated)

    def train(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            raise 'inputs should be list of paths'
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
        return VwResult(result['average loss'], populated)       