import argparse
import logging
import aiohttp
import asyncio
from cachetools import TTLCache
from aiohttp.server import ServerHttpProtocol

loop = asyncio.get_event_loop()

logging.basicConfig(level=logging.NOTSET, format="%(asctime)s %(threadName)-20s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class RequestCache:
    """
    It is a simple implementation of global request cache with pattern Singleton.
    """

    _cache = None

    def __new__(cls, maxsize, ttl):
        if not cls._cache:
            cls._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        return cls

    @classmethod
    def get_value(cls, method, path):
        key = cls._hash_func(method, path)
        logger.debug("Get value by key: '{}'".format(key))
        return cls._cache.get(key)

    @classmethod
    def cache_value(cls, method, path, data):
        if not cls.contains(method, path):
            cls._set_value_to_cache(method, path, data)

    @classmethod
    def contains(cls, method, path):
        key = cls._hash_func(method, path)
        return key in cls._cache.keys()

    @classmethod
    def _set_value_to_cache(cls, method, path, data):
        key = cls._hash_func(method, path)
        logger.debug("Set value by key: '{}'".format(key))
        cls._cache[key] = data

    @staticmethod
    def _hash_func(method, path):
        return hash(method + path)


class ProxyRequestHandler(ServerHttpProtocol):

    def __init__(self, proxy_path, ttl, cache_size, **kwargs):
        super().__init__(**kwargs)
        self.proxy_path = proxy_path
        self._request_cache = RequestCache(ttl=ttl, maxsize=cache_size)

    async def _do_call(self, method, path, headers=()):
        if self._request_cache.contains(method, path):
            return self._request_cache.get_value(method, path)

        logger.info("Send request to {}".format(self.proxy_path + path))
        async with aiohttp.ClientSession(loop=loop) as session:
            response = await session.request(method=method, url=self.proxy_path + path, headers=headers)
            data = await response.content.read()
            await response.release()
            self._request_cache.cache_value(method, path, data=(response, data))
            return response, data

    async def handle_request(self, message, payload):

        if message.method != 'GET':
            logger.warning("Received unsupported method")
            loop.create_task(self.send_response(status=501, http_version=message.version))
            return

        request_headers = self._filter_headers(headers=message.headers,
                                               headers_for_skipping=["Host"])
        proxy_req_resp, data = await self._do_call(method=message.method,
                                                   path=message.path,
                                                   headers=request_headers)

        response_headers = self._filter_headers(headers=proxy_req_resp.headers,
                                                headers_for_skipping=['Content-Encoding',
                                                                      'Transfer-Encoding',
                                                                      'Content-Length'])
        # calculating real content length
        response_headers.append(("Content-Length", str(len(data))))
        await self.send_response(status=proxy_req_resp.status,
                                 headers=response_headers,
                                 http_version=message.version,
                                 data=data)

    async def send_response(self, status, http_version, headers=list(), data=b""):
        response = aiohttp.Response(transport=self.writer,
                                    status=status,
                                    http_version=http_version)
        response.add_headers(*headers)
        response.send_headers()
        response.write(data)
        await response.write_eof()

    @staticmethod
    def _filter_headers(headers, headers_for_skipping):
        return [(key, headers.get(key)) for key in headers if key not in headers_for_skipping]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("proxy_to", default="https://www.google.com.ua", help="Proxy requests to host.")
    parser.add_argument("port", default=8000, help="Proxy server port.")
    parser.add_argument("cache_size", type=int, default=100, help="Proxy server port.")
    parser.add_argument("ttl", type=int, default=600, help="Request cache TTL")
    args = parser.parse_args()

    try:
        server_task = loop.create_server(lambda: ProxyRequestHandler(proxy_path=args.proxy_to, ttl=args.ttl, cache_size=args.cache_size),
                                         host="127.0.0.1",
                                         port=args.port)
        server = loop.run_until_complete(server_task)
        logger.info('Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()

        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
        loop.close()
    except OSError as e:
        logger.error(e)
        exit(1)
