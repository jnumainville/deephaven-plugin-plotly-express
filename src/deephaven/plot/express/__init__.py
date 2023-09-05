from __future__ import annotations

import json
from functools import partial
from typing import Any

from deephaven.plugin import Registration, Callback
from deephaven.plugin.object_type import BidirectionalObjectType, MessageStream
from deephaven.table_listener import listen

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


class DeephavenFigureListener:
    def __init__(self, figure, connection):
        self.connection: MessageStream = connection
        self.figure = figure
        self.exporter = Exporter()

        self.listeners = []

        head_node = figure.get_head_node()
        self.partitioned_tables = head_node.partitioned_tables

        for table, node in self.partitioned_tables.values():
            listener = partial(self.on_update, node)
            var = listen(table.table, listener)
            self.listeners.append(var)

        self.figure.listener = self


    def get_figure(
            self,
    ):
        return self.figure.get_figure()

    def on_update(self, node, update, is_replay):
        print("on update")
        # because this is listening to the partitioned meta table, it will
        # always trigger a rerender
        if self.connection:
            node.recreate_figure()
            self.connection.on_data(*self._build_figure_message(self.get_figure()))

    def _handle_retrieve_figure(
            self,
            message: dict[str, Any],
            references: list[Any]
    ) -> tuple[bytes, list[Any]]:
        """

        Args:
            message:
            references:

        Returns:

        """
        return self._build_figure_message(self.get_figure())

    def _build_figure_message(self, figure) -> tuple[bytes, list[Any]]:
        """

        Args:
            figure:

        Returns:

        """
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
        """

        Args:
            payload:
            references:

        Returns:

        """
        message = json.loads(payload.decode())
        if message["type"] == "RETRIEVE":
            return self._handle_retrieve_figure(message, references)

class DeephavenFigureConnection(MessageStream):
    def __init__(self, figure: DeephavenFigure, client_connection: MessageStream):
        super().__init__()
        self._listener = DeephavenFigureListener(figure, client_connection)
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

    def on_close(self) -> None:
        """
        Close the connection
        """
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
