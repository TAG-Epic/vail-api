from enum import IntEnum, StrEnum
from typing import Final
from yarl import URL


class ConfigLoadError(Exception):
    def __init__(self) -> None:
        super().__init__("failed to load config from config.toml")


class NoContentPageBug(Exception):
    def __init__(self) -> None:
        super().__init__("204 from page!")


class APIErrorCode(StrEnum):
    USER_NOT_FOUND = "user_not_found"
    MISSING_QUERY_PARAMETER = "missing_query_parameter"
    RATE_LIMITED = "rate_limited"


class AccelByteErrorCode(IntEnum):
    PLATFORM_USER_NOT_FOUND = 10139
    USER_ID_WRONG_FORMAT = 20002


class Service(StrEnum):
    ACCELBYTE = "accelbyte"
    UNKNOWN = "unknown"

    @staticmethod
    def get_from_url(url: URL | str) -> "Service":
        parsed_url = URL(url)

        if parsed_url.host is None:
            return Service.UNKNOWN
        host_to_service: dict[str, Service] = {
            "login.vailvr.com": Service.ACCELBYTE,
        }
        return host_to_service.get(parsed_url.host, Service.UNKNOWN)


class ExternalServiceError(Exception):
    def __init__(self, service: Service, status: int, message: str) -> None:
        self.service: Final[Service] = service
        self.status: Final[int] = status
        self.message: Final[str] = message
        super().__init__(f"{service}({status}): {message}")
