import logging
import time
import sys

from threading import Lock

class console_logger:
    def __init__(self, node_id, level='INFO'):
        self.node_id = node_id
        self.level = logging.getLevelName(level)
        self.lock = Lock()

    def debug(self, message):
        if self.level <= logging.DEBUG: self._trace(message)

    def info(self, message):
        if self.level <= logging.INFO: self._trace(message)

    def warning(self, message):
        if self.level <= logging.WARNING: self._trace(message)

    def error(self, message):
        if self.level <= logging.ERROR: self._trace(message)

    def critical(self, message):
        if self.level <= logging.CRITICAL: self._trace(message)

    def _trace(self, message):
        prefix = '[' + str(self.node_id) + '][' + time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time())) + ']'
        self.lock.acquire()
        print(prefix + message)
        self.lock.release()