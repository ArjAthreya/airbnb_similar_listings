from fastapi import APIRouter

from api.routes import request_types

router = APIRouter()
router.include_router(request_types.router, tags=["similarity"], prefix="/v1")