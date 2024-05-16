import typing
from aiohttp.web import Request, StreamResponse


def api_cors(function: typing.Callable[[Request], typing.Awaitable[StreamResponse]]):
    async def wrapper(request: Request) -> StreamResponse:
        response = await function(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        return response

    return wrapper
