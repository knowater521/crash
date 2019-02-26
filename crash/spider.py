"""通用爬虫类，数据存储在 MySQL 中。

create:   2018-12-12
modified: 2018-12-20
"""

import atexit
import threading

import pymysql

from . import sessions
from .types import *


class MultiThreadSpider(threading.Thread):

    # 如果请求 html，用这个头部
    headers_html: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;'
                  'q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    # 如果请求 json，用这个头部
    headers_json: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/json; charset=UTF-8',
        'x-requested-with': 'XMLHttpRequest'  # 代表是 ajax 请求
    }

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save: str,
                 daemon: bool = True) -> None:
        super().__init__(name=name, daemon=daemon)

        self._running = True

        atexit.register(self.close)  # 注册清理函数，线程结束时自动调用

        self.mysql_conn = pymysql.connect(
            host=mysql_config['host'], port=mysql_config['port'],
            user=mysql_config['user'], password=mysql_config['pwd'],
            db=mysql_config['db'], autocommit=True
        )
        self.cursor = self.mysql_conn.cursor()

        self.session = sessions.Session()
        self.session.headers.update(self.headers_html)  # 默认 html 头部

        self.table_save = table_save

        self.sql_insert: Optional[str] = None
        self.sql_update: Optional[str] = None

    def run(self) -> None:
        """抽象方法，由子类继承创建。"""

        raise NotImplementedError

    def insert(self, item: Dict) -> None:
        if not self.sql_insert:  # 只构建一次，提高性能
            self.sql_insert = 'INSERT INTO {} ({}) VALUES ({})'.format(
                self.table_save,
                ', '.join(item),
                ', '.join(f'%({k})s' for k in item)
            )

        try:
            self.cursor.execute(self.sql_insert, item)
        except pymysql.IntegrityError:
            pass
        except pymysql.err.Warning:  # 过滤不合法 mysql 类型
            pass

    def update(self, where: str, item: Dict) -> None:
        if not self.sql_update:
            self.sql_update = 'UPDATE {} SET {} WHERE {{}}'.format(
                self.table_save,
                ', '.join(f'{k} = %({k})s' for k in item)
            )

        try:
            self.cursor.execute(self.sql_update.format(where), item)
        except pymysql.IntegrityError:
            pass
        except pymysql.err.Warning:
            pass

    def terminate(self) -> None:
        self._running = False

    def close(self) -> None:
        self.session.close()
        self.cursor.close()
        self.mysql_conn.close()


def run_spider(
        spider_class: Type[MultiThreadSpider],
        table_save:  str,
        thread_num: int,
        mysql_config: MysqlConfig) -> None:

    thread_list: List[MultiThreadSpider] = []
    for i in range(thread_num):
        t = spider_class(
            f'thread{i+1}', mysql_config, table_save
        )
        thread_list.append(t)

    for t in thread_list:
        t.start()

    try:
        for t in thread_list:
            t.join()
    except KeyboardInterrupt:  # 只有主线程能收到键盘中断
        for t in thread_list:  # 防止下面在保存完 `row` 后，线程又请求一个新 `row`
            t.terminate()
        exit(1)
