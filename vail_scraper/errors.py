from enum import StrEnum


class ConfigLoadError(Exception):
    def __init__(self) -> None:
        super().__init__("failed to load config from config.toml")


class NoContentPageBug(Exception):
    def __init__(self) -> None:
        super().__init__("204 from page!")

class APIErrorCode(StrEnum):
    USER_NOT_FOUND = "user_not_found"
