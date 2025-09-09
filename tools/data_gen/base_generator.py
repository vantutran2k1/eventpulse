import time
from abc import ABC, abstractmethod


class BaseGenerator(ABC):
    def __init__(self, tps: int = 1, duration: int = 10):
        self.tps = tps
        self.duration = duration

    @abstractmethod
    def generate_one(self):
        raise NotImplementedError("Must be implemented by subclass")

    def run(self):
        start = time.time()
        while time.time() - start < self.duration:
            self.generate_one()
            time.sleep(1 / self.tps)
