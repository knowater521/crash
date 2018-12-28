"""任务分发方式。

create:   2018-12-28
modified:
"""

import queue

from . import db
from .types import *


class QueueMixin:

    # 任务队列，分发任务
    q: Optional[queue.Queue] = None

    @classmethod
    def create_task_list(cls, mysql_config: MysqlConfig, sql: str) -> None:
        """
        从 MySQL 中读取任务，
        放入一个全局变量 `q` 队列中，
        供多个线程使用。
        """

        cls.q = queue.Queue()

        for row in db.read_data(mysql_config, sql):
            cls.q.put(row)
