from .stock import router as stock_router
from .indicator import router as indicator_router
from .admin import router as admin_router

__all__ = ["stock_router", "indicator_router", "admin_router"]
