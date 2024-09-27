from db import AirbnbDatabase, initialize_database
from listing_schema_utils import load_listings, Listing
from typing import List
import json
from ml.data.make_dataset import clean_data as make_dataset_clean_data

CREATE_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS listing (
    id INTEGER PRIMARY KEY,
    listing_url TEXT NOT NULL,
    price REAL NOT NULL,
    property_type TEXT,
    room_type TEXT NOT NULL,
    bathrooms_text TEXT NOT NULL,
    bedrooms REAL NOT NULL,
    beds REAL,
    accommodates INTEGER,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    neighborhood_overview TEXT NOT NULL,
    neighbourhood_group_cleansed TEXT NOT NULL,
    neighbourhood_cleansed TEXT NOT NULL,
    description TEXT NOT NULL,
    amenities TEXT NOT NULL,
    host_about TEXT NOT NULL,
    review_scores_rating REAL NOT NULL,
    review_scores_cleanliness REAL NOT NULL,
    review_scores_checkin REAL NOT NULL,
    review_scores_communication REAL NOT NULL,
    review_scores_location REAL NOT NULL,
    review_scores_value REAL NOT NULL,
    number_of_reviews INTEGER NOT NULL
)
'''

INSERT_LISTING_QUERY = '''
INSERT OR REPLACE INTO listing (
    id, listing_url, price, property_type, room_type, bathrooms_text,
    bedrooms, beds, accommodates, latitude, longitude, neighborhood_overview,
    neighbourhood_group_cleansed, neighbourhood_cleansed, description,
    amenities, host_about, review_scores_rating, review_scores_cleanliness,
    review_scores_checkin, review_scores_communication, review_scores_location,
    review_scores_value, number_of_reviews
) VALUES (
    :id, :listing_url, :price, :property_type, :room_type, :bathrooms_text,
    :bedrooms, :beds, :accommodates, :latitude, :longitude, :neighborhood_overview,
    :neighbourhood_group_cleansed, :neighbourhood_cleansed, :description,
    :amenities, :host_about, :review_scores_rating, :review_scores_cleanliness,
    :review_scores_checkin, :review_scores_communication, :review_scores_location,
    :review_scores_value, :number_of_reviews
)
'''

class LaunchDB:
    def __init__(self):
        self.db = AirbnbDatabase()

    def clean_data(self, df):
        # Use the clean_data function from make_dataset
        return make_dataset_clean_data(df)

    def load_data(self):
        # Initialize the database
        initialize_database()

        # Create the listing table
        self._create_listing_table()

        # Load NYC listings
        df = load_listings()

        # Clean the data
        df = self.clean_data(df)

        # Convert DataFrame to list of ListingItem objects
        listings = []
        for _, row in df.iterrows():
            listing = Listing(**row.to_dict())
            listings.append(listing)

        # Insert listings into the database
        self._insert_listings_into_db(listings)

    def _create_listing_table(self):
        '''
        Creates the listing table in the database
        '''
        self.db.execute_query(CREATE_TABLE_QUERY)

    def _insert_listings_into_db(self, listings: List[Listing]):
        '''
        Inserts listings into the database
        '''
        self.db.execute_queries(INSERT_LISTING_QUERY, [listing.dict() for listing in listings])

    def run(self):
        print("Loading data into the database...")
        self.load_data()
        print("Data loaded successfully.")

        # Fetch and print a sample listing
        sample_query = "SELECT * FROM listing LIMIT 1"
        sample_listing = self.db.fetch_data(sample_query, fetch_all=False)
        print("\nSample listing:")
        print(json.dumps(sample_listing, indent=2))

if __name__ == '__main__':
    launcher = LaunchDB()
    launcher.run()