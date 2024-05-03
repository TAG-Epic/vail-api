from aiohttp import web
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ..utils.rate_limit import rate_limit_http
from .. import app_keys

router = web.RouteTableDef()


@router.get("/metrics")
@rate_limit_http(lambda: TimesPerRateLimiter(1, 10))
async def get_metrics(request: web.Request) -> web.Response:
    lines = []
    database = request.app[app_keys.DATABASE]

    # Scrape info
    result = await database.execute("select count(*) from users")
    row = await result.fetchone()
    assert row is not None
    total_users = row[0]
    lines.append(f"scraper_users_found {total_users}")

    # Stats
    result = await database.execute(
        """
        select
            users.id, users.name, stats.code, stats.value, stats.updated_at
        from users
            join stats on users.id = stats.user_id
    """
    )
    rows = await result.fetchall()

    for row in rows:
        lines.append(f'stats_value{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}", code="{escape_prometheus(row[2])}"}} {row[3]}')
        lines.append(f'stats_updated_at{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}", code="{escape_prometheus(row[2])}"}} {row[4]}')
    return web.Response(text="\n".join(lines))


def escape_prometheus(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')
