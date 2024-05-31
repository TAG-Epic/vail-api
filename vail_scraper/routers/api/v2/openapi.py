from pathlib import Path

from aiohttp import web
import yaml

router = web.RouteTableDef()

@router.get("/api/v2/openapi.yaml")
async def get_openapi(request: web.Request) -> web.StreamResponse:
    del request # Unused
    current_directory = Path(__file__).parent

    openapi_file = current_directory / "openapi.yaml"
    return web.FileResponse(openapi_file)
