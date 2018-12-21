"""使用多线程库抓取 ebd 眼镜数据。

create_time: 2018-12-13
modified_time:
"""

import warnings
import queue
import re

from lxml import etree

from crash import spider, log, db
from crash.types import *

from config import *

# warnings.filterwarnings('error')  # 将警告提升为异常，可以过滤掉不符合字段类型的数据

log.logger.set_log_level(log.DEBUG)


class EbdProductListSpider(spider.MultiThreadSpider):

    url_temp = 'https://www.eyebuydirect.com/eyeglasses?' \
               'attrs%5B%5D={}&page={}&view-column=1&pagesize=120'

    shape_list = [
        'rectangle', 'wayfarer', 'round', 'square', 'oval', 'horn', 'browline', 'aviator'
    ]

    # 提取总产品数
    re_product_count = re.compile(r'<strong>(\d+)</strong>')

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str,
                 daemon: bool = True) -> None:
        super().__init__(name, mysql_config, table_save, daemon)

        # 改成抓取 json 数据的头部
        self.session.headers.update(self.headers_json)

    def run(self) -> None:
        for shape in self.shape_list:
            url_base = self.url_temp.format(shape, '{}')
            url = url_base.format(1)
            r = self.session.get(url)
            product_count = int(self.re_product_count.findall(r.json()['data']['pageHtml'])[0])
            for i in range(1, product_count // 120 + 2):
                url = url_base.format(i)
                r = self.session.get(url)
                selector = etree.HTML(r.json()['data']['list'])
                product_ele_list = selector.xpath('.//ul/li[contains(@class,"item")]')
                for product_ele in product_ele_list:
                    product_name = product_ele.xpath('./@data-product-name')[0]
                    product_id = product_ele.xpath('./@data-pid')[0]
                    product_code = product_ele.xpath('./@data-product-code')[0]
                    product_price = product_ele.xpath('./@data-product-price')[0]
                    product_type = product_ele.xpath('./@data-product-type')[0]
                    product_url = 'https://www.eyebuydirect.com' + \
                                  product_ele.xpath('.//a[@class="event-list-link"]/@href')[0]

                    self.insert({
                        'product_name': product_name,
                        'product_id': product_id,
                        'product_code': product_code,
                        'url': product_url,
                        'shape': shape,
                        'price': product_price,
                        'type': product_type,
                    })


class EbdProductDetailSpider(spider.MultiThreadSpider):

    # 任务队列，分发任务
    q = queue.Queue()

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str,
                 daemon: bool = True) -> None:
        super().__init__(name, mysql_config, table_save, daemon)

    def run(self) -> None:

        while self._running:
            try:
                _id, url = self.q.get_nowait()
            except queue.Empty:
                break

            r = self.session.get(url)

            selector = etree.HTML(r.text)

            description = selector.xpath(
                './/p[@itemprop="description" and @class="text-section"]/text()'
            )[0].strip()

            review_count = 0
            review_count_container = selector.xpath(
                './/meta[@itemprop="reviewCount"]/@content'
            )
            if review_count_container:
                review_count = review_count_container[0]

            review_url = None
            review_url_container = selector.xpath(
                './/span[@class="btn btn-box btn-all-reviews"]/@data-href'
            )
            if review_url_container:
                review_url = 'https://www.eyebuydirect.com' + review_url_container[0]

            material = selector.xpath(
                './/a[@data-event-label="Materials"]/text()'
            )[0]

            self.update(f'id = {_id}', {
                'description': description,
                'review_count': review_count,
                'review_url': review_url,
                'material': material
            })

    @classmethod
    def create_task_list(cls, mysql_config: MysqlConfig, sql: str) -> None:
        """
        从 MySQL 中读取任务，
        放入一个全局变量 `q` 队列中，
        供多个线程使用。
        """

        for row in db.read_data(mysql_config, sql):
            cls.q.put(row)


def main() -> None:
    # 从产品列表页抓取部分数据
    spider.run_spider(
        EbdProductListSpider,
        MYSQL_TABLE_SAVE_EBD,
        1,
        MYSQL_CONFIG
    )

    # 从产品详情页抓取其余数据
    mysql_sql = 'SELECT id, url FROM {}'.format(MYSQL_TABLE_SAVE_EBD)
    EbdProductDetailSpider.create_task_list(MYSQL_CONFIG, mysql_sql)
    spider.run_spider(
        EbdProductDetailSpider,
        MYSQL_TABLE_SAVE_EBD,
        THREAD_NUM,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()
