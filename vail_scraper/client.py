import json
from logging import getLogger
import typing
import time
import hashlib
from uuid import uuid4
import base64
import secrets
import asyncio
from yarl import URL
from urllib.parse import quote
from pprint import pprint

from slowstack.asynchronous.times_per import TimesPerRateLimiter
import aiohttp

from vail_scraper.enums import RequestPriority

from .errors import AccelByteErrorCode, ExternalServiceError, NoContentPageBug, Service
from .models import (
    AccelByteLeaderboardPage,
    AccelByteLeaderboardPlayer,
    AccelBytePlayerInfo,
    AccelBytePlayerStatItemsPage,
    AexlabLeaderboardPage,
    AexlabLeaderboardPlayer,
    AccelByteStatCode,
    AexlabStatCode,
)
from .utils.circuit_breaker import CircuitBreaker
from .config import ScraperConfig

_CLIENT_ID: str = "8e1fbe68aef14404882e88358be5536b"
_TOKEN_MARGIN_SECONDS: float = 2
_logger = getLogger(__name__)


class VailClient:
    def __init__(self, scraper_config: ScraperConfig) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._config: ScraperConfig = scraper_config
        self._rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(
            scraper_config.rate_limiter.times, scraper_config.rate_limiter.per
        )
        self._circuit_breaker: CircuitBreaker = CircuitBreaker(5, 60)
        self._token_lock: asyncio.Lock = asyncio.Lock()
        self._refresh_token: str | None = None
        self._access_token: str | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={"Bot": "true", "User-Agent": self._config.user_agent}
            )
        return self._session

    async def _request(
        self,
        method: typing.Literal["GET", "POST", "PATCH", "DELETE"],
        url: str,
        *,
        priority: int = 0,
        **kwargs: typing.Any,
    ) -> aiohttp.ClientResponse:
        session = await self._get_session()
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=priority):
                return await session.request(method, url, **kwargs)

    async def _get_refresh_token(self) -> tuple[str, str]:
        session = await self._get_session()

        challenge_verifier = secrets.token_urlsafe(50)
        challenge = (
            base64.urlsafe_b64encode(
                hashlib.sha256(challenge_verifier.encode()).digest()
            )
            .decode()
            .rstrip("=")
        )

        _logger.debug(
            "made challenge: %s (%s) verifier: %s (%s)",
            challenge,
            len(challenge),
            challenge_verifier,
            len(challenge_verifier),
        )

        csrf = str(uuid4())

        state = {"csrf": csrf, "payload": {"path": "/account/overview"}}

        # Get request id
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=RequestPriority.HIGH):
                response = await session.get(
                    "https://login.vailvr.com/iam/v3/oauth/authorize",
                    params={
                        "response_type": "code",
                        "client_id": _CLIENT_ID,
                        "redirect_url": "https://login.vailvr.com",
                        "code_challenge": challenge,
                        "code_challenge_method": "S256",
                        "createHeadless": "false",
                        "state": json.dumps(state),
                    },
                    allow_redirects=False,
                )
        response.raise_for_status()
        url = URL(response.headers["Location"])
        _logger.debug("authorize query: %s", url.query_string)
        request_id = url.query.get("request_id")
        # TODO: Error handling
        assert request_id is not None, "no request id :("

        session.cookie_jar.update_cookies({"request_id": request_id})
        response = await session.post(
            "https://login.vailvr.com/iam/v3/authenticate",
            data={
                "request_id": request_id,
                "redirect_uri": "https://login.vailvr.com",
                "client_id": _CLIENT_ID,
                "user_name": self._config.user.email,
                "password": self._config.user.password,
            },
            allow_redirects=False,
        )
        url = URL(response.headers["Location"])
        _logger.debug("authenticate query: %s", url.query_string)
        code = url.query.get("code")
        # TODO: Error handling
        assert code is not None, "code missing :("
        _logger.debug("got code: %s", code)

        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=RequestPriority.HIGH):
                response = await session.post(
                    "https://login.vailvr.com/iam/v3/oauth/token",
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "code_verifier": challenge_verifier,
                        "client_id": _CLIENT_ID,
                    },
                    allow_redirects=False,
                )

            if not response.ok:
                raise AssertionError(f"fuck: {await response.text()}")

        data = await response.json()
        _logger.debug("code data: %s", data)

        return data["refresh_token"], data["access_token"]

    def _get_token_expiry_in_seconds(self, token: str) -> float:
        header, body, signature = token.split(".")
        del header, signature
        body_data = json.loads(base64.b64decode(body + "=="))

        expire_time = body_data["exp"] - time.time()
        _logger.debug("token expires in %s", expire_time)
        return expire_time

    async def _get_token(self) -> str:
        async with self._token_lock:
            if self._refresh_token is None or self._access_token is None:
                _logger.debug("creating new refresh token due to none being saved")
                self._refresh_token, self._access_token = (
                    await self._get_refresh_token()
                )
            if (
                self._get_token_expiry_in_seconds(self._refresh_token)
                < _TOKEN_MARGIN_SECONDS
            ):
                _logger.debug("creating new refresh token as current expired")
                self._refresh_token, self._access_token = (
                    await self._get_refresh_token()
                )
            if (
                self._get_token_expiry_in_seconds(self._access_token)
                < _TOKEN_MARGIN_SECONDS
            ):
                _logger.debug("creating new access token as current expired")
                # TODO: Actually refresh
                self._refresh_token, self._access_token = (
                    await self._get_refresh_token()
                )

        return self._access_token

    async def _raise_for_status(self, response: aiohttp.ClientResponse):
        if not response.ok:
            raise ExternalServiceError(
                Service.get_from_url(response.url),
                response.status,
                await response.text(),
            )

    async def _do_authenticated_request(
        self,
        method: typing.Literal["GET", "POST", "PATCH", "PUT", "DELETE"],
        *args,
        priority: int = 0,
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        headers = headers or {}

        acquired_rate_limiter_event: asyncio.Event = asyncio.Event()

        async def _do_request():
            with self._circuit_breaker:
                async with self._rate_limiter.acquire(priority=priority):
                    acquired_rate_limiter_event.set()
                    session = await self._get_session()
                    mixed_headers = {"Authorization": f"Bearer {token}", **headers}
                    return await session.request(
                        method, *args, **kwargs, headers=mixed_headers
                    )

        while True:
            token = await self._get_token()
            do_request_task = asyncio.create_task(_do_request())

            jwt_expires_after_seconds = self._get_token_expiry_in_seconds(token)
            wait_for_jwt_expire_task = asyncio.create_task(
                asyncio.sleep(jwt_expires_after_seconds - _TOKEN_MARGIN_SECONDS)
            )
            wait_for_rate_limit_spot_task = asyncio.create_task(
                acquired_rate_limiter_event.wait()
            )

            await asyncio.wait(
                [wait_for_rate_limit_spot_task, wait_for_jwt_expire_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if wait_for_rate_limit_spot_task.done():
                wait_for_jwt_expire_task.cancel()
                return await do_request_task
            else:
                do_request_task.cancel()
                wait_for_rate_limit_spot_task.cancel()

    async def get_aexlab_leaderboard_page(
        self,
        stat_code: AexlabStatCode,
        *,
        page_id: int = 0,
        page_size: int = 100,
        priority: int = 0,
    ) -> list[AexlabLeaderboardPlayer]:
        session = await self._get_session()
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=priority):
                response = await session.get(
                    "https://aexlab.com/api/leaderboard",
                    params={
                        "leaderboardCode": stat_code,
                        "page": page_id + 1,
                        "pageLimit": page_size,
                    },
                )
            if response.status == 204:
                _logger.warn("204 page!")
                raise NoContentPageBug()
            await self._raise_for_status(response)
            data = await response.text()
            page = AexlabLeaderboardPage.validate_json(data)
            return page

    async def get_accelbyte_leaderboard_page(
        self,
        stat_code: AccelByteStatCode,
        *,
        page_id: int = 0,
        page_size: int = 100,
        priority: int = 0,
    ) -> list[AccelByteLeaderboardPlayer]:
        response = await self._do_authenticated_request(
            "GET",
            f"https://login.vailvr.com/leaderboard/v3/public/namespaces/vailvr/leaderboards/{quote(stat_code)}/alltime",
            params={"limit": page_size, "offset": page_id * page_size},
            priority=priority,
        )
        await self._raise_for_status(response)
        data = await response.text()
        page = AccelByteLeaderboardPage.model_validate_json(data)
        return page.data

    async def get_accelbyte_user_stats(
        self, user_id: str, *, priority: int = 0
    ) -> dict[str, float] | None:
        response = await self._do_authenticated_request(
            "GET",
            f"https://login.vailvr.com/social/v1/public/namespaces/vailvr/users/{quote(user_id)}/statitems",
            params={"limit": 1000},
            priority=priority,
        )
        if not response.ok and response.content_type == "application/json":
            error_data = await response.json()
            error_code = error_data["errorCode"]

            if (
                error_code == AccelByteErrorCode.PLATFORM_USER_NOT_FOUND
                or error_code == AccelByteErrorCode.USER_ID_WRONG_FORMAT
            ):
                return None

        await self._raise_for_status(response)
        data = await response.text()

        page = AccelBytePlayerStatItemsPage.model_validate_json(data)
        return {stat_item.stat_code: stat_item.value for stat_item in page.data}

    async def get_accelbyte_user_info(
        self, user_id: str, *, priority: int = 0
    ) -> AccelBytePlayerInfo | None:
        response = await self._do_authenticated_request(
            "GET",
            f"https://login.vailvr.com/iam/v3/public/namespaces/vailvr/users/{quote(user_id)}",
            priority=priority,
        )
        if not response.ok and response.content_type == "application/json":
            error_data = await response.json()
            error_code = error_data["errorCode"]

            if (
                error_code == AccelByteErrorCode.PLATFORM_USER_NOT_FOUND
                or error_code == AccelByteErrorCode.USER_ID_WRONG_FORMAT
            ):
                return None

        await self._raise_for_status(response)
        data = await response.text()
        user = AccelBytePlayerInfo.model_validate_json(data)
        return user
