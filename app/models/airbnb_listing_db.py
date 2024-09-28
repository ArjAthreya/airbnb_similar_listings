import os, sys
# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from typing import List, Dict, Optional
from app.core.database.db import AirbnbDatabase

class AirbnbListingDB:
    """
    This class is used to interact with the Airbnb listing database.
    """
    def __init__(self, id: int, **properties):
        self.id = id
        self.properties = properties

    @staticmethod
    def from_db_dict(db_dict: Dict) -> 'AirbnbListingDB':
        listing_id = db_dict.pop('id')
        return AirbnbListingDB(listing_id, **db_dict)

    @staticmethod
    def get_by_id(id: int, db: Optional[AirbnbDatabase] = None) -> Optional['AirbnbListingDB']:
        if not db:
            db = AirbnbDatabase()

        query = "SELECT * FROM listing WHERE id = :id"
        row = db.fetch_data(query, {'id': id}, fetch_all=False)

        if not row:
            return None

        return AirbnbListingDB.from_db_dict(row)

    @staticmethod
    def get_by_ids(ids: List[int], db: Optional[AirbnbDatabase] = None) -> List['AirbnbListingDB']:
        if not db:
            db = AirbnbDatabase()

        placeholders = ', '.join(':id' + str(i) for i in range(len(ids)))
        query = f"SELECT * FROM listing WHERE id IN ({placeholders})"
        params = {f'id{i}': id for i, id in enumerate(ids)}
        rows = db.fetch_data(query, params, fetch_all=True)

        return [AirbnbListingDB.from_db_dict(row) for row in rows]

    @staticmethod
    def get_all(skip: int = 0, limit: int = 10, db: Optional[AirbnbDatabase] = None) -> List['AirbnbListingDB']:
        if not db:
            db = AirbnbDatabase()

        query = "SELECT * FROM listing LIMIT :limit OFFSET :skip"
        rows = db.fetch_data(query, {'limit': limit, 'skip': skip}, fetch_all=True)

        return [AirbnbListingDB.from_db_dict(row) for row in rows]

    def save(self, db: Optional[AirbnbDatabase] = None):
        if not db:
            db = AirbnbDatabase()

        columns = ', '.join(self.properties.keys())
        placeholders = ', '.join(f':{key}' for key in self.properties.keys())
        query = f"""
        INSERT OR REPLACE INTO listing (id, {columns})
        VALUES (:id, {placeholders})
        """
        
        params = {'id': self.id, **self.properties}
        db.execute_query(query, params)

    def __str__(self):
        return f"Listing(id={self.id}, {', '.join(f'{k}={v}' for k, v in self.properties.items())})"

if __name__ == '__main__':
    # Example usage
    db = AirbnbDatabase()

    # Get a listing by ID
    listing = AirbnbListingDB.get_by_id(739333866230665371, db)
    print(listing)

    # Get multiple listings by IDs
    listings = AirbnbListingDB.get_by_ids([572612125615500056, 45267941], db)
    for listing in listings:
        print(listing)

    # Get all listings with pagination
    all_listings = AirbnbListingDB.get_all(skip=0, limit=5, db=db)
    for listing in all_listings:
        print(listing)

    # Create and save a new listing
    new_listing = AirbnbListingDB(
        id=9999,
        listing_url="https://example.com",
        price=100.0,
        property_type="Apartment",
        room_type="Entire home/apt",
        bathrooms_text="1 bath",
        bedrooms=2,
        beds=2,
        accommodates=4,
        latitude=40.7128,
        longitude=-74.0060,
        neighborhood_overview="Great neighborhood",
        neighbourhood_group_cleansed="Manhattan",
        neighbourhood_cleansed="Chelsea",
        description="A cozy apartment in the heart of NYC",
        amenities="WiFi, Kitchen, Air conditioning",
        host_about="Friendly host",
        review_scores_rating=4.5,
        review_scores_cleanliness=4.5,
        review_scores_checkin=4.5,
        review_scores_communication=4.5,
        review_scores_location=4.5,
        review_scores_value=4.5,
        number_of_reviews=10,
        description_summary="Cozy NYC apartment",
        property_outline="2-bedroom apartment",
        high_level_overview="Perfect for a NYC getaway",
        cluster=205,
        listings_in_cluster="100"
    )
    new_listing.save(db)

    # Verify the new listing was saved
    saved_listing = AirbnbListingDB.get_by_id(9999, db)
    print(saved_listing)

    db.close_connection()