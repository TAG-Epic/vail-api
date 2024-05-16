import json
import base64
from logging import getLogger
import time
import typing

import aiohttp
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ..config import ScraperConfig
from ..utils.circuit_breaker import CircuitBreaker
from ..errors import ExternalServiceError, Service

_logger = getLogger(__name__)

TOKEN_MARGIN_SECONDS: float = 2


def get_token_expiry_in_seconds(token: str) -> float:
    header, body, signature = token.split(".")
    del header, signature
    body_data = json.loads(base64.b64decode(body + "=="))

    expire_time = body_data["exp"] - time.time()
    _logger.debug("token expires in %s", expire_time)
    return expire_time


class BaseService:
    def __init__(self, config: ScraperConfig) -> None:
        self._config: ScraperConfig = config
        self._http_session: aiohttp.ClientSession | None = None
        self._circuit_breaker: CircuitBreaker = CircuitBreaker(5, 60)
        self._rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(
            config.rate_limiter.times, config.rate_limiter.per
        )

    async def get_session(self) -> aiohttp.ClientSession:
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession(
                headers={"Bot": "true", "User-Agent": self._config.user_agent}
            )
        return self._http_session

    async def request(
        self,
        method: typing.Literal["GET", "POST", "PATCH", "DELETE"],
        url: str,
        *,
        priority: int = 0,
        **kwargs: typing.Any,
    ) -> aiohttp.ClientResponse:
        session = await self.get_session()
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=priority):
                return await session.request(method, url, **kwargs)

    async def raise_for_status(self, response: aiohttp.ClientResponse):
        if not response.ok:
            raise ExternalServiceError(
                Service.get_from_url(response.url),
                response.status,
                await response.text(),
            )
