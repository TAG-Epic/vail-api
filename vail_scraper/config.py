import toml
from pydantic import BaseModel

from .errors import ConfigLoadError


class RateLimitConfig(BaseModel):
    times: int
    per: float


class Config(BaseModel):
    database_url: str
    user_agent: str
    rate_limiter: RateLimitConfig
    scrape: bool 


def load_config() -> Config:
    try:
        with open("config.toml") as f:
            return Config.model_validate(toml.load(f))
    except:
        raise ConfigLoadError()
