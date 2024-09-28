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

@router.get(
    "/listing/{listing_id}/similar",
    response_model=list,
    name="listing:get-similar",
)
async def get_similar_listings(listing_id: int):
    try:
        # Get similar listings directly using the class method
        similar_listings = AirbnbListingDB.get_listings_in_cluster(listing_id)
        
        if not similar_listings:
            raise HTTPException(status_code=404, detail="No similar listings found")
        
        return similar_listings
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Exception: {err}")