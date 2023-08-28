from __future__ import annotations

import json
import threading
from typing import Any

from deephaven.plugin import Registration, Callback
from deephaven.plugin.object_type import FetchOnlyObjectType, BidirectionalObjectType, MessageStream
from deephaven import merge
from deephaven.table import PartitionedTable
from deephaven.table_listener import listen, TableListener

from .deephaven_figure import DeephavenFigure, export_figure
from .deephaven_figure.DeephavenFigure import Exporter

from .plots import (
    area,
    bar,
    frequency_bar,
    timeline,
    histogram,
    box,
    violin,
    strip,
    ohlc,
    candlestick,
    treemap,
    sunburst,
    icicle,
    funnel,
    funnel_area,
    line,
    line_polar,
    line_ternary,
    line_3d,
    scatter,
    scatter_3d,
    scatter_polar,
    scatter_ternary,
    pie,
    layer,
    make_subplots,
)

from .data import data_generators

__version__ = "0.0.7.dev0"

from .shared import args_copy

NAME = "deephaven.plot.express.DeephavenFigure"


class DeephavenFigureListener(TableListener):
    def __init__(self, figure, connection):
        self.connection: MessageStream = connection
        self.figure = figure
        self.exporter = None
        self.filters = None
        self._fig_lock = threading.Lock()

    def new_figure(self, table):
        new_args = args_copy(self.figure.orig_args)
        table = self.apply_filters()
        new_args["args"]["table"] = table
        new_fig, _ = self.figure.orig_func(**new_args)
        return new_fig

    def apply_filters(self):
        table = self.figure.table
        if self.filters:
            columns = None
            if isinstance(table, PartitionedTable):
                columns = table.key_columns

                # this merge, filter, and remerge is likely not ideal for many cases
                # but it is the easiest way to get the correct filtered partitions
                # this processing could possibly be done in the PartitionManager on
                # the original table, but if the original table is partitioned
                # anyways this workflow is still necessary
                # this will not work for histogram or frequency bar, as those
                # require the full table to filter
                table = merge(table.constituent_tables)

            new_table = table.where(self.filters)

            if columns:
                new_table = new_table.partition_by(columns)

            return new_table
        return table

    def on_update(self, update, is_replay):
        # because this is listening to the partitioned meta table, it will
        # always trigger a rerender
        with self.figure.exec_ctx:
            new_fig = self.new_figure(self.figure.table)

            if self.connection:
                # attempt to send
                message = {
                    "type": "NEW_FIGURE",
                    "figure": new_fig.to_dict(exporter=self.exporter)
                }

                self.connection.on_data(
                    json.dumps(message).encode(),
                    self.exporter.reference_list())

            return new_fig

    def _handle_filter_figure(
            self,
            message: dict[str, Any],
            references: list[Any]
    ) -> tuple[bytes, list[Any]]:
        # TODO: first check if filters are only for partitions
        # then, can just use that partition?

        with self._fig_lock:
            self.filters = message["filters"]
            return self._build_figure_message(self.new_figure(self.figure.table))

    def _handle_retrieve_figure(
            self,
            message: dict[str, Any],
            references: list[Any]
    ) -> tuple[bytes, list[Any]]:
        return self._build_figure_message(self.new_figure(self.figure.table))

    def _build_figure_message(self, figure) -> tuple[bytes, list[Any]]:
        message = {
            "type": "NEW_FIGURE",
            "figure": figure.to_dict(exporter=self.exporter)
        }

        return json.dumps(message).encode(), self.exporter.reference_list()

    def _process_message(
            self,
            payload: bytes,
            references: list[Any]
    ) -> tuple[bytes, list[Any]]:
        message = json.loads(payload.decode())
        if message["type"] == "FILTER":
            return self._handle_filter_figure(message, references)
            pass
        elif message["type"] == "RETRIEVE":
            return self._handle_retrieve_figure(message, references)
            pass


class DeephavenFigureConnection(MessageStream):
    def __init__(self, figure: DeephavenFigure, client_connection: MessageStream):
        super().__init__()
        self._listener = DeephavenFigureListener(figure, client_connection)
        table = figure.table
        if isinstance(table, PartitionedTable):
        # only need to listen if it is possible that partitions will be added
            listen(table.table, self._listener)
        self.client_connection = client_connection

    def on_data(self, payload: bytes, references: list[Any]) -> tuple[bytes, list[Any]]:
        """
        Args:
            payload: Payload to execute
            references: References to objects on the server

        Returns:
            None
        """
        return self._listener._process_message(payload, references)

    def on_close(self):
        pass


class DeephavenFigureType(FetchOnlyObjectType):
    """
    DeephavenFigureType for plugin registration

    """

    @property
    def name(self) -> str:
        """
        Returns the name of the plugin

        Returns:
            str: The name of the plugin

        """
        return NAME

    def is_type(self, obj: any) -> bool:
        """
        Check if an object is a DeephavenFigure

        Args:
          obj: any: The object to check

        Returns:
            bool: True if the object is of the correct type, False otherwise
        """
        return isinstance(obj, DeephavenFigure)

    def to_bytes(self, exporter: Exporter, figure: DeephavenFigure) -> bytes:
        """
        Converts a DeephavenFigure to bytes

        Args:
          exporter: Exporter: The exporter to use
          figure: DeephavenFigure: The figure to convert

        Returns:
            bytes: The Figure as bytes
        """
        return export_figure(exporter, figure)


class DeephavenFigureListenerType(BidirectionalObjectType):
    """
    DeephavenFigureType for plugin registration

    """

    @property
    def name(self) -> str:
        """
        Returns the name of the plugin

        Returns:
            str: The name of the plugin

        """
        return NAME + "New"

    def is_type(self, obj: any) -> bool:
        """
        Check if an object is a DeephavenFigure

        Args:
          obj: any: The object to check

        Returns:
            bool: True if the object is of the correct type, False otherwise
        """
        return isinstance(obj, DeephavenFigure)

    def create_client_connection(self, obj: DeephavenFigure, connection: MessageStream) -> MessageStream:
        """
        Create a client connection for the DeephavenFigure

        Args:
          obj: object: The object to create the connection for
          connection: MessageStream: The connection to use

        Returns:
            MessageStream: The client connection
        """
        figure_connection = DeephavenFigureConnection(obj, connection)
        initial_message = json.dumps({
            "type": "RETRIEVE",
        }).encode()
        payload, references = figure_connection.on_data(initial_message, [])
        connection.on_data(payload, references)
        return figure_connection


class ChartRegistration(Registration):
    """
    Register the DeephavenFigureType

    """

    @classmethod
    def register_into(cls, callback: Callback) -> None:
        """
        Register the DeephavenFigureType

        Args:
          callback: Registration.Callback:
            A function to call after registration

        """
        callback.register(DeephavenFigureType)
        callback.register(DeephavenFigureListenerType)
