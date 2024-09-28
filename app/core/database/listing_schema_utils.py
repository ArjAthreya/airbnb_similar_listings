import os
import sys
from pathlib import Path
import pandas as pd
import urllib.request
from pydantic import BaseModel, AnyUrl, HttpUrl
from typing import Union, List, Optional
from dotenv import load_dotenv

load_dotenv()
# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent))

LISTING_TABLE_SCHEMA = ('''
    id INTEGER PRIMARY KEY,
    listing_url TEXT NOT NULL,
    price REAL NOT NULL,
    property_type TEXT,
    room_type TEXT NOT NULL,
    bathrooms_text TEXT,
    bedrooms REAL NOT NULL,
    beds REAL ,
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
    number_of_reviews INTEGER NOT NULL,
    description_summary TEXT NOT NULL, 
    property_outline TEXT NOT NULL,
    high_level_overview TEXT NOT NULL,
    cluster INTEGER NOT NULL,
    listings_in_cluster TEXT NOT NULL
''')
                        
class Listing(BaseModel):
    id: int
    listing_url: str
    price: float
    property_type: Optional[str] = None
    room_type: str
    bathrooms_text: str
    bedrooms: float
    beds: Optional[float] = None
    accommodates: Optional[int] = None
    latitude: float
    longitude: float
    neighbourhood_group_cleansed: str
    neighbourhood_cleansed: str
    neighborhood_overview: Optional[str] = None
    description: str
    amenities: str
    host_about: Optional[str] = None
    review_scores_rating: float
    review_scores_cleanliness: float
    review_scores_checkin: float
    review_scores_communication: float
    review_scores_location: float
    review_scores_value: float
    number_of_reviews: int
    description_summary: str
    property_outline: str
    high_level_overview: str
    cluster: int
    listings_in_cluster: str

def load_listings(local_path: str = os.getenv('NYC_CSV_FILEPATH')) -> pd.DataFrame:
    '''
    loads listings from a url and returns a dataframe with the relevant columns
    '''
    print(local_path)
    data_df = pd.read_csv(local_path)

    return data_df

if __name__ == '__main__':
    df = load_listings(NYC_CSV_FILEPATH)
    print(f"Loaded {len(df)} listings")