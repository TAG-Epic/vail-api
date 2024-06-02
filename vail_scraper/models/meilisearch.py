from enum import StrEnum
import typing
from pydantic import BaseModel

class SearchResults(BaseModel):
    hits: list[typing.Any]
    estimatedTotalHits: int

class SearchIndex(StrEnum):
    USERS = "users"
