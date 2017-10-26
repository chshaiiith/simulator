from deterministic import Deterministic
from possion import Possion

class Distribution:
    @classmethod
    def get_distribution(cls, type,  **kwargs):
        if type == "deterministic":
            return Deterministic(**kwargs)

        if type == "poisson":
            return Possion(**kwargs)

        # XXX: add more types as per requirements here