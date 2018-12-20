"""改进 requests `Session` 类的 `requests` 方法。

Create:   2018-8-28
Modified: 2018-10-7
"""

import time

from requests import Session as _Session
from requests.models import Request, Response
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError

from . import env
from .log import logger
from .types import Optional


class Session(_Session):

    def request(self, method, url,
                params=None,
                data=None,
                headers=None,
                cookies=None,
                files=None,
                auth=None,
                timeout=120,  # 等待响应，超时时间
                allow_redirects=True,
                proxies=None,
                hooks=None,
                stream=None,
                verify=None,
                cert=None,
                json=None) -> Optional[Response]:

        req = Request(
            method=method.upper(),
            url=url,
            headers=headers,
            files=files,
            data=data or {},
            json=json,
            params=params or {},
            auth=auth,
            cookies=cookies,
            hooks=hooks,
        )
        prep = self.prepare_request(req)

        proxies = proxies or {}

        settings = self.merge_environment_settings(
            prep.url, proxies, stream, verify, cert
        )

        send_kwargs = {
            'timeout': timeout,
            'allow_redirects': allow_redirects,
        }
        send_kwargs.update(settings)

        message = '%s: %s' % (method, prep.url)
        logger.info(message)
        for i in range(env.PER_REQUEST_TRY_COUNT + 1):
            try:
                r = self.send(prep, **send_kwargs)
                return r
            except Timeout:
                why = 'Timeout'
            except ConnectionError:
                why = 'ConnectionError'
            except ChunkedEncodingError:  # 读到的字节数与实际字节数不符
                why = 'ChunkedEncodingError'
            if i != env.PER_REQUEST_TRY_COUNT:
                logger.warning('%s, retry %d >>> %s' % (why, i + 1, message))
                time.sleep(3)
        logger.error('%s, %s' % (why, message))
