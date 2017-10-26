class Deterministic:
    def __init__(self, **kwargs):
        self.length = kwargs["rate"]

    def next(self):
        return self.length


