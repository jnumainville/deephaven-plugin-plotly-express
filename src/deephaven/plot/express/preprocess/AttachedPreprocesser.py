from __future__ import annotations


from .StyleManager import StyleManager
from ..shared import get_unique_names

class AttachedPreprocesser():
    def __init__(self, args, always_attached):
        self.args = args
        self.always_attached = always_attached
        self.prepare_preprocess()


    def prepare_preprocess(self):
        # create new columns
        table = self.args["table"]
        # these should always be using the values column
        values_col = self.args["values"]
        for var, (map, ls, new_col) in self.always_attached.items():
            manager_col = get_unique_names(table, [f"{new_col}_manager"])[f"{new_col}_manager"]
            style_manager = StyleManager(map=map, ls=ls)

            table = table.update([
                f"{manager_col}=style_manager",
                f"{new_col}={manager_col}.assign_style({values_col})"
            ])

        self.args["table"] = table

    def preprocess_partitioned_tables(self, tables, column=None):
        pass

