from __future__ import annotations

from .AttachedPreprocesser import AttachedPreprocesser
from .FreqPreprocesser import FreqPreprocesser
from .HistPreprocesser import HistPreprocesser

class Preprocesser:
    def __init__(
            self,
            args,
            groups,
            always_attached
    ):
        self.args = args
        self.groups = groups
        self.preprocesser = None
        self.always_attached = always_attached

        self.prepare_preprocess()
        pass

    def prepare_preprocess(self):
        print("in preprocesser")
        if "preprocess_hist" in self.groups:
            self.preprocesser = HistPreprocesser(self.args)
        elif "preprocess_freq" in self.groups:
            # setting the preprocessor is only needed if there is a
            # preprocess_partitioned_tables function
            print("freak time")
            self.preprocesser = FreqPreprocesser(self.args)
        elif "always_attached" in self.groups and self.always_attached:
            AttachedPreprocesser(self.args, self.always_attached)



    def preprocess_partitioned_tables(self, tables, column=None):
        if not self.preprocesser:
            yield from tables
        else:
            yield from self.preprocesser.preprocess_partitioned_tables(tables, column)

