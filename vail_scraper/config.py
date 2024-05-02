import typing

import toml
from pydantic import BaseModel

from .errors import ConfigLoadError


class RateLimitConfig(BaseModel):
    times: int
    per: float


class ScraperUserConfig(BaseModel):
    email: str
    password: str


class BansConfig(BaseModel):
    aexlab: bool = False
    accelbyte: bool = False


class ScraperConfig(BaseModel):
    mode: typing.Literal["scraper"] = "scraper"
    enabled: bool = True
    user_agent: str

    bans: BansConfig
    user: ScraperUserConfig
    rate_limiter: RateLimitConfig
    database_url: str


def load_config() -> ScraperConfig:
    try:
        with open("config.toml") as f:
            return ScraperConfig.model_validate(toml.load(f))
    except:
        raise ConfigLoadError()
