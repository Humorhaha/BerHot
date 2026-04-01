"""
知乎爬虫反爬中间件
- User-Agent 轮换
- 指数退避重试
- 代理池支持（可选）
"""

import random
import logging

from twisted.internet import defer, reactor

logger = logging.getLogger(__name__)


class RandomUserAgentMiddleware:
    """随机轮换 User-Agent"""

    USER_AGENTS = [
        # macOS Chrome
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        # Windows Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        # macOS Safari
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
        # Windows Edge
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.92",
        # Linux Chrome
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        # Firefox
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    ]

    def process_request(self, request, spider=None):
        request.headers["User-Agent"] = random.choice(self.USER_AGENTS)
        return None


class ExponentialBackoffMiddleware:
    """遇到 403/429 时指数退避"""

    MAX_RETRY_COUNT = 5
    BASE_DELAY = 5  # 基础延迟秒数

    def process_response(self, request, response, spider=None):
        if response.status in (403, 429):
            retry_count = request.meta.get("retry_count", 0)
            if retry_count < self.MAX_RETRY_COUNT:
                # 指数退避: 5s → 10s → 20s → 40s → 80s
                delay = min(2 ** retry_count * self.BASE_DELAY, 120)
                logger.warning(
                    f"Got {response.status}, backing off {delay}s (retry {retry_count + 1}/{self.MAX_RETRY_COUNT})"
                )
                request.meta["retry_count"] = retry_count + 1

                # 通过 Twisted reactor 延迟调度
                d = defer.Deferred()
                reactor.callLater(delay, d.callback, None)
                return d.addCallback(lambda _: request)
            else:
                logger.error(
                    f"Max retries ({self.MAX_RETRY_COUNT}) exceeded for {request.url}"
                )
        return response


class ZhihuHeadersMiddleware:
    """添加知乎所需的请求头"""
    
    def __init__(self):
        self._request_count = 0

    def process_request(self, request, spider=None):
        # 基础请求头
        request.headers.setdefault("Accept", "application/json, text/plain, */*")
        request.headers.setdefault("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
        request.headers.setdefault("Accept-Encoding", "gzip, deflate, br")
        request.headers.setdefault("Referer", "https://www.zhihu.com/")
        request.headers.setdefault("X-Requested-With", "XMLHttpRequest")
        request.headers.setdefault("Origin", "https://www.zhihu.com")
        
        # 模拟知乎的 API 请求头
        request.headers.setdefault("x-zse-93", "101_3_3.0")
        request.headers.setdefault("x-api-version", "3.0.91")
        request.headers.setdefault("x-app-za", "OS=Web")
        
        # 随机 sec-ch-ua
        import random
        sec_ua_values = [
            '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            '"Chromium";v="121", "Not(A:Brand";v="24", "Google Chrome";v="121"',
            '"Google Chrome";v="122", "Chromium";v="122", "Not=A?Brand";v="24"',
        ]
        request.headers.setdefault("sec-ch-ua", random.choice(sec_ua_values))
        request.headers.setdefault("sec-ch-ua-mobile", "?0")
        request.headers.setdefault("sec-ch-ua-platform", '"macOS"')
        request.headers.setdefault("sec-fetch-dest", "empty")
        request.headers.setdefault("sec-fetch-mode", "cors")
        request.headers.setdefault("sec-fetch-site", "same-origin")
        
        # 添加 DNT (Do Not Track)
        request.headers.setdefault("DNT", "1")
        
        self._request_count += 1
        return None


class ProxyMiddleware:
    """代理池支持（可选）"""

    def process_request(self, request, spider=None):
        if spider:
            proxy_url = spider.settings.get("PROXY_POOL_URL")
            if proxy_url:
                try:
                    import requests
                    proxy = requests.get(proxy_url, timeout=5).text.strip()
                    if proxy:
                        request.meta["proxy"] = f"http://{proxy}"
                        logger.debug(f"Using proxy: {proxy}")
                except Exception as e:
                    logger.warning(f"Failed to get proxy: {e}")
        return None


class ZhihuCookieMiddleware:
    """知乎 Cookie 处理"""

    def process_request(self, request, spider=None):
        if not spider:
            return None
            
        # 检查是否启用了 Cookie
        if not spider.settings.getbool("COOKIES_ENABLED", False):
            return None
        
        # 获取配置的 Cookie
        cookies_str = spider.settings.get("ZHIHU_COOKIES", "")
        if not cookies_str:
            return None
        
        # 解析 Cookie 字符串并设置到请求
        try:
            cookies = {}
            for cookie in cookies_str.split(";"):
                cookie = cookie.strip()
                if "=" in cookie:
                    key, value = cookie.split("=", 1)
                    cookies[key.strip()] = value.strip()
            
            # 合并到现有 cookies
            if cookies:
                request.cookies = request.cookies or {}
                request.cookies.update(cookies)
                logger.debug(f"Added {len(cookies)} cookies to request")
        except Exception as e:
            logger.warning(f"Failed to parse cookies: {e}")
        
        return None
