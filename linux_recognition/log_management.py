import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from typing import Any, Self

import json_log_formatter
from anyio import Path

from linux_recognition.configuration import LoggingSettings


class CustomizedJSONFormatter(json_log_formatter.JSONFormatter):

    def __init__(
            self, include_time: bool = True,attributes: list[str] | None = None, *args: Any, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._include_time = include_time
        self._attributes = attributes if attributes is not None else []

    def json_record(self, message: str, extra: dict[str, Any], record: logging.LogRecord) -> dict[str, Any]:
        body = {}
        if self._include_time:
            body['time'] = datetime.fromtimestamp(record.created, tz=timezone.utc)
        for attr in self._attributes:
            if hasattr(record, attr):
                body[attr] = getattr(record, attr)
        task_name = getattr(record, 'taskName', None)
        if task_name is not None:
            body['task_name'] = task_name
        body['message'] = message
        if isinstance(extra, dict):
            body.update(extra)
        if record.exc_info:
            body['exc_info'] = self.formatException(record.exc_info)
        if record.stack_info:
            body['stack_info'] = record.stack_info
        return body


class CustomizedListener(QueueListener):

    def __init__(
            self, queue: Queue, *handlers: logging.Handler, **kwargs
    ) -> None:
        super().__init__(queue, *handlers, **kwargs)

    @contextmanager
    def started(self) -> Generator[Self, None, None]:
        self.start()
        try:
            yield self
        finally:
            self.stop()


def init_logging(
        logging_settings: LoggingSettings,
        project_directory: Path
) -> tuple[logging.Logger, CustomizedListener]:
    handlers = []
    if logging_settings.log_to_console:
        handlers.append(logging.StreamHandler())
    if logging_settings.file_handler.use:
        log_path = project_directory.parent / 'logs' / logging_settings.file_handler.filename
        handlers.append(logging.FileHandler(log_path))
    if not handlers:
        handlers.append(logging.NullHandler())
    include_time = logging_settings.attributes.include_time
    attributes_to_log = logging_settings.attributes.other
    formatter = CustomizedJSONFormatter(include_time, attributes_to_log)
    queue = Queue(-1)
    listener = CustomizedListener(queue, *handlers)
    logger = logging.getLogger()
    queue_handler = QueueHandler(queue)
    queue_handler.setFormatter(formatter)
    logger.addHandler(queue_handler)
    logger.setLevel(logging_settings.level)
    return logger, listener


def get_error_details(exception: Exception) -> dict[str, Any]:
    return {
        "error_type": type(exception).__name__,
        "error_details": str(exception)
    }
