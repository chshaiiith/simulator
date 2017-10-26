from numpy.random import exponential

class Possion():
    def __init__(self, **kwargs):
        self.rate = kwargs["rate"]

    def next(self):
        return exponential(1/self.rate)