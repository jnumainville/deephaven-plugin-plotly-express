from __future__ import annotations

import json
import threading
from typing import Any

from deephaven.plugin import Registration, Callback
from deephaven.plugin.object_type import BidirectionalObjectType, MessageStream
from deephaven.table import Table, PartitionedTable
from deephaven.table_listener import listen, TableListener
from deephaven.execution_context import get_exec_ctx

from .deephaven_figure import DeephavenFigure, export_figure, Exporter


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
        self.exporter = Exporter()
        self.fig_lock = threading.Lock()

    def new_figure(
            self,
            table: Table | PartitionedTable
    ):
        new_args = args_copy(self.figure.orig_args)
        new_args["args"]["table"] = table
        new_fig, _ = self.figure.orig_func(**new_args)
        return new_fig

    def on_update(self, update, is_replay):
        # because this is listening to the partitioned meta table, it will
        # always trigger a rerender
        print("sending update")
        with self.figure.exec_ctx:
            new_fig = self.new_figure(self.figure.table)

            if self.connection:
                self.connection.on_data(*self._build_figure_message(new_fig))

            return new_fig

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
        if message["type"] == "RETRIEVE":
            return self._handle_retrieve_figure(message, references)
            pass

def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")
    print(f"is_replay: {is_replay}")


class DeephavenFigureConnection(MessageStream):
    def __init__(self, figure: DeephavenFigure, client_connection: MessageStream):
        super().__init__()
        self._listener = DeephavenFigureListener(figure, client_connection)
        table = figure.table
        if isinstance(table, PartitionedTable):
            listen(table.table, self._listener)
            figure.listener = self._listener
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
        callback.register(DeephavenFigureListenerType)
