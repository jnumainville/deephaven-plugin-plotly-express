from __future__ import annotations

import json
import threading
from abc import abstractmethod
from collections.abc import Generator
from copy import copy, deepcopy
from typing import Callable, Any
from plotly.graph_objects import Figure

from ..shared import args_copy
from ..data_mapping import DataMapping

from deephaven.table import Table, PartitionedTable


class Reference:
    """A reference to an object

    Attributes:
        index: int: The index of the reference
        obj: object: The object that the reference points to
    """

    def __init__(
            self: Reference,
            index: int,
            obj: object
    ):
        self.index = index
        self.obj = obj


class Exporter:

    def __init__(
            self,
    ):
        self.references = {}
        pass

    def reference(self: Exporter, obj: object) -> Reference:
        """Creates a reference for an object, ensuring that it is exported for
            use on the client. Each time this is called, a new reference will be
            returned, with the index of the export in the data to be sent to the
            client.

            Args:
            obj: object: The object to create a reference for

            Returns:
                Reference: The reference to the object

            """
        if obj not in self.references:
            self.references[obj] = Reference(len(self.references), obj)
        return self.references[obj]

    def reference_list(self: Exporter) -> list[Any]:
        """Creates a list of references for a list of objects

            Args:
              objs: list[object]: The list of objects to create references for

            Returns:
                list[Reference]: The list of references to the objects

            """
        return list(self.references.keys())


def export_figure(
        exporter: Exporter,
        figure: DeephavenFigure
) -> bytes:
    """Helper to export a DeephavenFigure as json

    Args:
      exporter: Exporter: The exporter to use
      figure: DeephavenFigure: The figure to export

    Returns:
      bytes: The figure as bytes

    """
    return figure.to_json(exporter).encode()


def has_color_args(
        call_args: dict[str, Any]
) -> bool:
    """Check if any of the color args are in call_args

    Args:
      call_args: dict[str, Any]: A dictionary of args

    Returns:
      bool: True if color args are in, false otherwise

    """
    for arg in ["color_discrete_sequence_line",
                "color_discrete_sequence_marker"]:
        # convert to bool to ensure empty lists don't prevent removal of
        # colors on traces
        if arg in call_args and bool(call_args[arg]):
            return True
    return False


def has_arg(
        call_args: dict[str, Any],
        check: str | Callable
) -> bool:
    """Given either a string to check for in call_args or function to check,
    return True if the arg is in the call_args

    Args:
      call_args: dict[str, Any]: A dictionary of args
      check: str | Callable: Either a string or a function that takes call_args

    Returns:
        bool: True if the call_args passes the check, False otherwise
    """
    if call_args:
        if isinstance(check, str) and check in call_args:
            return bool(call_args[check])
        elif isinstance(check, Callable):
            return check(call_args)
    return False
    # check is either a function or string

class DeephavenNode:
    @abstractmethod
    def recreate_figure(self):
        pass

    @abstractmethod
    def copy(self, parent, partitioned_tables):
        pass

    @abstractmethod
    def get_figure(self):
        pass



class DeephavenFigureNode(DeephavenNode):
    def __init__(self, parent, exec_ctx, args, table, func):
        # function, execution ctx, args, table
        # if partitioned then add a listener linked to this node
        self.parent = parent
        self.exec_ctx = exec_ctx
        self.args = args
        self.table = table
        self.func = func
        self.cached_figure = None
        self.node_lock = threading.Lock()
        pass

    def recreate_figure(
            self,
            update_parent=True
    ):
        """
        Recreate the figure. This is called when the underlying partition
        changes

        Returns:

        """
        with self.exec_ctx, self.node_lock:

            table = self.table
            self.args["args"]["table"] = table
            self.cached_figure = self.func(**self.args)

        if update_parent:
            self.parent.recreate_figure()

    def copy(self, parent, partitioned_tables):
        # args need to be copied as the table key is modified
        new_args = args_copy(self.args)
        new_node = DeephavenFigureNode(self.parent, self.exec_ctx, new_args, self.table, self.func)
        if id(self) in partitioned_tables:
            table, _ = partitioned_tables[id(self)]
            partitioned_tables.pop(id(self))
            partitioned_tables[id(new_node)] = (table, new_node)

        new_node.cached_figure = self.cached_figure
        new_node.parent = parent
        return new_node

    def get_figure(self):
        if not self.cached_figure:
            self.recreate_figure(update_parent=False)

        return self.cached_figure


class DeephavenLayerNode(DeephavenNode):
    def __init__(self, layer_func, args, exec_ctx):
        self.parent = None
        self.nodes = []
        self.layer_func = layer_func
        self.args = args
        self.figs = []
        self.cached_figure = None
        self.exec_ctx = exec_ctx
        self.node_lock = threading.Lock()
        pass

    def recreate_figure(
            self,
            update_parent=True
    ):
        """
        Recreate the figure. This is called when the underlying partition or fil
        tered table changes

        Returns:

        """

        with self.exec_ctx, self.node_lock:

            figs = [node.cached_figure for node in self.nodes]
            self.cached_figure = self.layer_func(*figs, **self.args)

        if update_parent:
            self.parent.recreate_figure()

    def copy(
            self,
            parent,
            partitioned_tables
    ):
        new_node = DeephavenLayerNode(self.layer_func, self.args, self.exec_ctx)
        new_node.nodes = [node.copy(new_node, partitioned_tables) for node in self.nodes]
        new_node.cached_figure = self.cached_figure
        new_node.parent = parent
        return new_node

    def get_figure(self):
        """
        Get the figure for this node. This will be called by a child node when
        Returns:

        """
        if not self.cached_figure:
            for node in self.nodes:
                node.get_figure()
            self.recreate_figure(update_parent=False)

        return self.cached_figure

class DeephavenHeadNode:
    def __init__(
            self,

    ):
        # there is only one child node of the head, either a layer or a figure
        self.node = None
        # this should be a set of nodes
        self.partitioned_tables = {}

        self.cached_figure = None
        pass


    def copy_graph(self):
        new_head = DeephavenHeadNode()
        new_partitioned_tables = copy(self.partitioned_tables)
        new_head.node = self.node.copy(new_head, new_partitioned_tables)
        new_head.partitioned_tables = new_partitioned_tables
        return new_head


    def recreate_figure(
            self,
    ) -> None:
        """
        Recreate the overall figure. This will be called by a child node when
        it's figure is updated, and will update the figure for this node.

        Returns:

        """
        self.cached_figure = self.node.cached_figure

    def get_figure(self):
        """
        Get the figure for this node. This will be called by a listener to get
        the initial figure.
        Returns:

        """
        if not self.cached_figure:
            self.cached_figure = self.node.get_figure()
        return self.cached_figure



class DeephavenFigure:
    """A DeephavenFigure that contains a plotly figure and mapping from Deephaven
    data tables to the plotly figure

    Attributes:
        _plotly_fig: Figure: (Default value = None) The underlying plotly fig
        _call: Callable: (Default value = None) The (usually) px drawing
          function
        _call_args: dict[Any]: (Default value = None) The arguments that were
          used to call px
        _data_mappings: list[DataMapping]: (Default value = None) A list of data
          mappings from table column to corresponding plotly variable
        _has_template: bool: (Default value = False) If a template is used
        _has_color: bool: (Default value = False) True if color was manually
          applied via discrete_color_sequence
        _trace_generator: Generator[dict[str, Any]]: (Default value = None)
          A generator for modifications to traces
        _has_subplots: bool: (Default value = False) True if has subplots
    """

    def __init__(
            self: DeephavenFigure,
            fig: Figure = None,
            call: Callable = None,
            call_args: dict[Any] = None,
            data_mappings: list[DataMapping] = None,
            has_template: bool = False,
            has_color: bool = False,
            trace_generator: Generator[dict[str, Any]] = None,
            has_subplots: bool = False,
    ):
        # keep track of function that called this and it's args
        self._head_node = DeephavenHeadNode()

        # note: these variables might not be up to date with the latest
        # figure if the figure is updated
        self._plotly_fig = fig
        self._call = call
        self._call_args = call_args
        self._trace_generator = trace_generator

        self._has_template = has_template if has_template else \
            has_arg(call_args, "template")

        self._has_color = has_color if has_color else \
            has_arg(call_args, has_color_args)

        self._data_mappings = data_mappings if data_mappings else []

        self._has_subplots = has_subplots


    def copy_mappings(
            self: DeephavenFigure,
            offset: int = 0
    ) -> list[DataMapping]:
        """Copy all DataMappings within this figure, adding a specific offset

        Args:
          offset: int:  (Default value = 0) The offset to offset the copy by

        Returns:
          list[DataMapping]: The new DataMappings

        """
        return [mapping.copy(offset) for mapping in self._data_mappings]

    def get_json_links(
            self: DeephavenFigure,
            exporter: Exporter
    ) -> list[dict[str, str]]:
        """Convert the internal data mapping to the JSON data mapping with
        tables and proper plotly indices attached

        Args:
          exporter: Exporter: The exporter to use to send tables

        Returns:
          list[dict[str, str]]: The list of json links that map table columns
            to the plotly figure

        """
        return [links for mapping in self._data_mappings
                for links in mapping.get_links(exporter)]

    def to_dict(
            self: DeephavenFigure,
            exporter: Exporter
    ) -> dict[str, Any]:
        """Convert the DeephavenFigure to dict

        Args:
          exporter: Exporter: The exporter to use to send tables

        Returns:
          str: The DeephavenFigure as a dictionary

        """
        return json.loads(self.to_json(exporter))

    def to_json(
            self: DeephavenFigure,
            exporter: Exporter
    ) -> str:
        """Convert the DeephavenFigure to JSON

        Args:
          exporter: Exporter: The exporter to use to send tables

        Returns:
          str: The DeephavenFigure as a JSON string

        """
        plotly = json.loads(self._plotly_fig.to_json())
        mappings = self.get_json_links(exporter)
        deephaven = {
            "mappings": mappings,
            "is_user_set_template": self._has_template,
            "is_user_set_color": self._has_color
        }
        payload = {
            "plotly": plotly,
            "deephaven": deephaven
        }
        return json.dumps(payload)

    def get_head_node(self):
        return self._head_node

    def add_layer_to_graph(
            self,
            layer_func,
            args,
            exec_ctx
    ):
        # create a new layer node
        # add the layer node to the head node

        new_head = DeephavenHeadNode()
        self._head_node = new_head

        layer_node = DeephavenLayerNode(layer_func, args, exec_ctx)

        new_head.node = layer_node
        layer_node.parent = new_head

        figs = args.pop("figs")
        children = []
        partitioned_tables = {}
        for fig in figs:
            if isinstance(fig, Figure):
                new_node = DeephavenFigureNode(None, None, None, None, None)
                children.append(new_node)
                # this is a plotly figure, so we need to create a new node
                # but we don't need to recreate it ever
                new_node.cached_figure = fig
                pass
            elif isinstance(fig, DeephavenFigure):
                tmp_head = fig._head_node.copy_graph()
                children.append(tmp_head.node)
                partitioned_tables.update(tmp_head.partitioned_tables)

        layer_node.nodes = children
        new_head.partitioned_tables = partitioned_tables

        for node in children:
            node.parent = layer_node


    def add_figure_to_graph(
            self,
            exec_ctx,
            args,
            table,
            func
    ):

        node = DeephavenFigureNode(self._head_node, exec_ctx, args, table, func)

        partitioned_tables = {}
        if isinstance(table, PartitionedTable):
            partitioned_tables = {id(node): (table, node)}

        self._head_node.node = node
        self._head_node.partitioned_tables = partitioned_tables

    def get_figure(self):
        return self._head_node.get_figure()

    def get_plotly_fig(self):
        return self._head_node.cached_figure._plotly_fig




