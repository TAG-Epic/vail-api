from typing import Any, Callable
from aiohttp.web import Request, json_response
from collections import defaultdict
from slowstack.asynchronous.times_per import BaseRateLimiter
from functools import wraps

from slowstack.common.errors import RateLimitedError

from vail_scraper.errors import APIErrorCode


def rate_limit_http(constructor: Callable[[], BaseRateLimiter]):
    rate_limiters: defaultdict[str, BaseRateLimiter] = defaultdict(constructor)

    def wrapper(original_handler: Callable[[Request], Any]) -> Callable[[Request], Any]:
        @wraps(original_handler)
        async def handler(request: Request):
            ip = request.remote or "127.0.0.1"

            rate_limiter = rate_limiters[ip]

            try:
                async with rate_limiter.acquire(wait=False):
                    return await original_handler(request)
            except RateLimitedError:
                return json_response(
                    {"detail": "rate limited", "code": APIErrorCode.RATE_LIMITED},
                    status=429,
                )

        return handler

    return wrapper
