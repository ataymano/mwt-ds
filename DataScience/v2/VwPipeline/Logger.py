import logging
import time
import sys

from threading import Lock

class console_logger:
    def __init__(self, level: str ='INFO'):
        self.level = logging.getLevelName(level)
        self.lock = Lock()

    def debug(self, message: str):
        if self.level <= logging.DEBUG: self._trace(message)

    def info(self, message: str):
        if self.level <= logging.INFO: self._trace(message)

    def warning(self, message: str):
        if self.level <= logging.WARNING: self._trace(message)

    def error(self, message: str):
        if self.level <= logging.ERROR: self._trace(message)

    def critical(self, message: str):
        if self.level <= logging.CRITICAL: self._trace(message)

    def _trace(self, message: str):
        prefix = f'[{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}] '
        self.lock.acquire()
        print(prefix + message)
        self.lock.release()