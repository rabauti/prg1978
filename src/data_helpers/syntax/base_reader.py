import logging


class BaseReader:
    def __init__(self, logger=None):
        self._logger = logger or logging.getLogger(f"Reader.{self.__class__.__name__}")
        self._logger.propagate = False

        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            self._logger.addHandler(handler)

    def log_info(self, message):
        self._logger.info(message)

    def log_error(self, message):
        self._logger.error(message)


__all__ = ["BaseReader"]
