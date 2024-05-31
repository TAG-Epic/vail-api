from ....router import CombinerRouteTableDef
from .users import router as users_router
from .stats import router as stats_router

router = CombinerRouteTableDef()
router.add_router(users_router)
router.add_router(stats_router)
