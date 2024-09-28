import json

import joblib
from fastapi import APIRouter, HTTPException
from app.models.airbnb_listing_db import AirbnbListingDB

router = APIRouter()

@router.get(
    "/listing/{listing_id}",
    response_model=dict,
    name="listing:get-by-id",
)
async def get_listing(listing_id: int):
    try:
        listing = AirbnbListingDB.get_by_id(listing_id)
        if listing:
            return {"id": listing.id, **listing.properties}
        else:
            raise HTTPException(status_code=404, detail="Listing not found")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Exception: {err}")

# @router.get(
#     "/health",
#     response_model=HealthResponse,
#     name="health:get-data",
# )
# async def health():
#     is_health = False
#     try:
#         settings = Settings()
#         test_input = MachineLearningDataInput(
#             **json.loads(open(INPUT_EXAMPLE, "r").read())
#         )
#         test_point = test_input.get_np_array()
#         get_prediction(test_point)
#         is_health = True
#         return HealthResponse(status=is_health)
#     except Exception:
#         raise HTTPException(status_code=404, detail="Unhealthy")
