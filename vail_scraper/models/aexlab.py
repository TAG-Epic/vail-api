from enum import StrEnum
from pydantic import BaseModel, BeforeValidator, Field, TypeAdapter
from typing import Annotated, Any

def dash_for_0_float(input: Any) -> float:
    assert isinstance(input, str), "input wasn't a float"

    if input == "-":
        return 0.0
    return float(input)


class AexLabStatCode(StrEnum):
    SCORE = "score"
    KILLS = "kills"
    DEATHS = "deaths"
    WINS = "wins"
    CTO_STEALS = "cto-steals"
    CTO_RECOVERS = "cto-recovers"

class AexLabPlayerStats(BaseModel):
    won: int
    lost: int
    abandoned: int
    deaths: int
    assists: int
    draws: int
    kills: int
    point: int
    game_hours: Annotated[
        float, Field(alias="gameHours"), BeforeValidator(dash_for_0_float)
    ]

class AexLabLeaderboardPlayer(BaseModel):
    user_id: Annotated[str, Field(alias="userId")]
    rank: int
    avatar_url: Annotated[str, Field(alias="avatarUrl")]
    display_name: Annotated[str, Field(alias="displayName")]
    point: int
    stats: AexLabPlayerStats


AexlabLeaderboardPage = TypeAdapter(list[AexLabLeaderboardPlayer])

