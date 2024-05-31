from ....router import CombinerRouteTableDef
from .users import router as users_router
from .stats import router as stats_router
from .game_stats import router as game_stats_router
from .openapi import router as openapi_router

router = CombinerRouteTableDef()

router.add_router(users_router)
router.add_router(stats_router)
router.add_router(game_stats_router)
router.add_router(openapi_router)
