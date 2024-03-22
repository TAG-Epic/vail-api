import toml
from pydantic import BaseModel

from .errors import ConfigLoadError

class Config(BaseModel):
    database_url: str
    user_agent: str

def load_config() -> Config:
    try:
        with open("config.toml") as f:
            return Config.model_validate(toml.load(f))
    except:
        raise ConfigLoadError()
