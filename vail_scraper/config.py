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
    accelbyte: bool = False

class SqliteConfig(BaseModel):
    url: str

class QuestConfig(BaseModel):
    http_url: str
    postgres_url: str

class MeiliSearchConfig(BaseModel):
    url: str

class DatabaseConfig(BaseModel):
    sqlite: SqliteConfig
    quest: QuestConfig
    meilisearch: MeiliSearchConfig

class WebhookAlertConfig(BaseModel):
    id: int
    token: str
    target_user: int

class ScraperConfig(BaseModel):
    mode: typing.Literal["scraper"] = "scraper"
    enabled: bool = True
    user_agent: str

    bans: BansConfig
    user: ScraperUserConfig
    rate_limiter: RateLimitConfig
    database: DatabaseConfig
    alert_webhook: WebhookAlertConfig | None = None


def load_config() -> ScraperConfig:
    try:
        with open("config.toml") as f:
            return ScraperConfig.model_validate(toml.load(f))
    except:
        raise ConfigLoadError()
