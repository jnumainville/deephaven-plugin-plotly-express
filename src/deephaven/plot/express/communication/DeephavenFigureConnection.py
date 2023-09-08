from __future__ import annotations

from typing import Any

from deephaven.plugin.object_type import MessageStream

from src.deephaven.plot.express import DeephavenFigure
from .DeephavenFigureListener import DeephavenFigureListener


class DeephavenFigureConnection(MessageStream):
    """
    Connection for DeephavenFigure
    """
    def __init__(self, figure: DeephavenFigure, client_connection: MessageStream):
        super().__init__()
        self._listener = DeephavenFigureListener(figure, client_connection)
        self._client_connection = client_connection

    def on_data(self, payload: bytes, references: list[Any]) -> tuple[bytes, list[Any]]:
        """
        Args:
            payload: Payload to execute
            references: References to objects on the server
        """
        return self._listener.process_message(payload, references)

    def on_close(self) -> None:
        """
        Close the connection
        """
        pass
