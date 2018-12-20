"""日志模块。

Create:   2018-9-29
Modified: 2018-10-7
"""

import logging

ERROR = logging.ERROR
WARNING = logging.WARNING
WARN = logging.WARNING
INFO = logging.INFO
DEBUG = logging.DEBUG


class SingletonMeta(type):
    def __init__(cls, *args, **kwargs):
        cls._instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class MyLog(metaclass=SingletonMeta):
    __slots__ = ('_logger',)

    def __init__(self, level: int=INFO) -> None:
        self._logger = logging.getLogger('MyRequests')

        self._set_logger(level)

    def _set_logger(self, level: int) -> None:
        self._logger.setLevel(level)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            # 还有 %A 代表星期
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        cmd_handler = logging.StreamHandler()
        cmd_handler.setFormatter(formatter)
        self._logger.addHandler(cmd_handler)

    def debug(self, message: str) -> None:
        self._logger.debug(message)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def error(self, message: str) -> None:
        self._logger.error(message)

    def set_log_level(self, level: int) -> None:
        self._logger.setLevel(level)


logger = MyLog()


def test() -> None:
    logger2 = MyLog()
    print(logger is logger2)
    logger.set_log_level(DEBUG)
    logger.debug('this is a logger debug message')
    logger.info('this is a logger info message')
    logger.warning('this is a logger warning message')
    logger.error('this is a logger error message')


if __name__ == '__main__':
    test()
