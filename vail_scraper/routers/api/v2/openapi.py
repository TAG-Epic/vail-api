from pathlib import Path

from aiohttp import web
import yaml

from ....utils.cors import api_cors

router = web.RouteTableDef()

@router.get("/api/v2/openapi.yaml")
@api_cors
async def get_openapi(request: web.Request) -> web.StreamResponse:
    del request # Unused
    current_directory = Path(__file__).parent

    openapi_file = current_directory / "openapi.yaml"
    return web.FileResponse(openapi_file)
