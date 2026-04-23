"""Multiplex logging architecture and wrapper subclass for Incorporator."""

import asyncio
import json
import logging
import os
import queue
from logging.handlers import QueueHandler, QueueListener
from typing import Any, Dict, List, Type, TypeVar, Union

# Import the Incorporator base class
from incorporator.base import Incorporator, IncorporatorList

TLoggedIncorporator = TypeVar("TLoggedIncorporator", bound="LoggedIncorporator")

# Global registry to prevent duplicate background threads if a class is dynamically rebuilt
_ACTIVE_LISTENERS: Dict[str, QueueListener] = {}


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON Lines for easy dynamic parsing."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "level": record.levelname,
            "msg": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
        }
        if hasattr(record, 'meta'):
            log_obj['meta'] = getattr(record, 'meta')
        if record.exc_info:
            log_obj['exc_info'] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


class APIFilter(logging.Filter):
    """Ensures only API-tagged traffic reaches the api.log."""

    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, 'is_api', False))


class StandardFilter(logging.Filter):
    """Prevents API-tagged traffic from cluttering the main error.log."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not bool(getattr(record, 'is_api', False))


def setup_class_logger(cls: Type[Any]) -> None:
    """Configures JSON-formatted, non-blocking logging for a dynamic subclass."""
    cls_name = getattr(cls, '__name__', 'UnknownClass')
    logger = logging.getLogger(cls_name)

    # Prevent duplicate handlers if the dynamic class is generated multiple times
    if cls_name in _ACTIVE_LISTENERS:
        return

    logger.setLevel(logging.DEBUG)
    formatter = JSONFormatter()

    # 1. Debug File Handler (Captures everything)
    debug_fh = logging.FileHandler(f"{cls_name}_debug.log", encoding='utf-8')
    debug_fh.setLevel(logging.DEBUG)
    debug_fh.setFormatter(formatter)

    # 2. Error/Main File Handler (Captures INFO and ERROR)
    error_fh = logging.FileHandler(f"{cls_name}_error.log", encoding='utf-8')
    error_fh.setLevel(logging.INFO)
    error_fh.addFilter(StandardFilter())
    error_fh.setFormatter(formatter)

    # 3. API File Handler (Captures only Web Traffic)
    api_fh = logging.FileHandler(f"{cls_name}_api.log", encoding='utf-8')
    api_fh.setLevel(logging.INFO)
    api_fh.addFilter(APIFilter())
    api_fh.setFormatter(formatter)

    # 4. Multi-Threading Queue Setup (Non-Blocking Event Loop)
    log_queue: queue.Queue[Any] = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    listener = QueueListener(log_queue, debug_fh, error_fh, api_fh, respect_handler_level=True)
    listener.start()
    _ACTIVE_LISTENERS[cls_name] = listener


class LoggingMixin:
    """Provides targeted logging methods and error retrieval to instances and classes."""

    @classmethod
    async def getError(cls) -> List[Dict[str, Any]]:
        """Reads the {cls.__name__}_error.log JSON file and formats the logs."""

        def _read_disk() -> List[Dict[str, Any]]:
            filepath = f"{cls.__name__}_error.log"
            if not os.path.exists(filepath):
                return []

            errors: List[Dict[str, Any]] = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            errors.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            return errors

        # Execute disk read in a background worker thread (Pillar E)
        return await asyncio.to_thread(_read_disk)

    # --- CLASS-LEVEL LOGGING (For Factory Methods like export) ---

    @classmethod
    def _get_cls_logger(cls) -> logging.Logger:
        return logging.getLogger(cls.__name__)

    @classmethod
    def log_cls_info(cls, msg: str) -> None:
        meta_str = f'class:"{cls.__name__}"'
        cls._get_cls_logger().info(msg, extra={'meta': meta_str, 'is_api': False})

    @classmethod
    def log_cls_error(cls, msg: str, exc_info: bool = False) -> None:
        meta_str = f'class:"{cls.__name__}"'
        cls._get_cls_logger().error(msg, exc_info=exc_info, extra={'meta': meta_str, 'is_api': False})

    # --- INSTANCE-LEVEL LOGGING ---

    def log_meta(self) -> str:
        """Generates a meta string detailing the class origin and instance identity."""
        cls = self.__class__
        cls_name = getattr(cls, '__name__', 'UnknownClass')
        return (
            f'class:"{cls_name}", name:"{cls_name}", '
            f'self:"{getattr(self, "code", None)}", name:"{getattr(self, "name", None)}", '
            f'file: "{getattr(cls, "file", None)}", url: "{getattr(cls, "url", None)}"'
        )

    def _get_logger(self) -> logging.Logger:
        return logging.getLogger(self.__class__.__name__)

    def log_debug(self, msg: str) -> None:
        self._get_logger().debug(msg, extra={'meta': self.log_meta(), 'is_api': False})

    def log_info(self, msg: str) -> None:
        self._get_logger().info(msg, extra={'meta': self.log_meta(), 'is_api': False})

    def log_error(self, msg: str, exc_info: bool = False) -> None:
        self._get_logger().error(msg, exc_info=exc_info, extra={'meta': self.log_meta(), 'is_api': False})

    def log_api(self, msg: str) -> None:
        self._get_logger().info(msg, extra={'meta': self.log_meta(), 'is_api': True})


class LoggedIncorporator(LoggingMixin, Incorporator):
    """The Incorporator Logging Wrapper Subclass."""

    @classmethod
    async def incorp(
            cls: Type[TLoggedIncorporator],
            *args: Any,
            enable_logging: bool = False,
            **kwargs: Any
    ) -> Union[TLoggedIncorporator, IncorporatorList[TLoggedIncorporator]]:
        """Declarative factory that sets up class-specific logging before generation."""
        result = await super().incorp(*args, **kwargs)

        if enable_logging:
            if isinstance(result, list):
                if result:
                    setup_class_logger(result[0].__class__)
            else:
                setup_class_logger(result.__class__)

        return result

    @classmethod
    async def refresh(
            cls: Type[TLoggedIncorporator],
            *args: Any,
            enable_logging: bool = False,
            **kwargs: Any
    ) -> Union[TLoggedIncorporator, IncorporatorList[TLoggedIncorporator]]:
        """Hydrates an existing instance with new data, optionally enabling logs."""
        result = await super().refresh(*args, **kwargs)

        if enable_logging:
            if isinstance(result, list):
                if result:
                    setup_class_logger(result[0].__class__)
            else:
                setup_class_logger(result.__class__)

        return result

    @classmethod
    async def export(
            cls: Type[TLoggedIncorporator],
            *args: Any,
            **kwargs: Any
    ) -> None:
        """Exports the class data, wrapping the process in observability logs."""
        setup_class_logger(cls)

        cls.log_cls_info(f"Initiating export process with args={args}, kwargs={kwargs}")
        try:
            await super().export(*args, **kwargs)
            cls.log_cls_info("Export process completed successfully.")
        except Exception as e:
            cls.log_cls_error(f"Export process failed: {str(e)}", exc_info=True)
            raise