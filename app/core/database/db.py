import os, sys

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(project_root)

import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Any, Generator
from app.core.database.listing_schema_utils import LISTING_TABLE_SCHEMA

class AirbnbDatabase:
    def __init__(self, db_path: str = "/Users/arjunathreya/Projects/airbnb_similar_listings/app/core/airbnb.db"):
        self.db_path = db_path

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Returns a connection to the database
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        try:
            yield conn
        finally:
            conn.close()
            self.connection = None

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> None:
        """
        Executes a query on the database
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            conn.commit()

    def execute_queries(self, query: str, param_list: List[Dict[str, Any]]) -> None:
        """
        Executes a list of queries on the database
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, param_list)
            conn.commit()

    def fetch_data(self, query: str, params: Dict[str, Any] = None, fetch_all: bool = True) -> List[Dict[str, Any]]:
        """
        Fetches data from the database
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            return cursor.fetchall() if fetch_all else cursor.fetchone()

    def fetch_many_data(self, query: str, params: Dict[str, Any] = None, size: int = 1) -> List[Dict[str, Any]]:
        """
        Fetches many data from the database
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            return cursor.fetchmany(size)

    def close_connection(self) -> None:
        """Close the database connection if it's open."""
        if self.connection:
            self.connection.close()
            self.connection = None

def initialize_database():
    # For now, we'll use a local path to the database, will update to be generic (env variable)
    if os.path.exists('/Users/arjunathreya/Projects/airbnb_similar_listings/app/core/airbnb.db'):
        print("Database already exists.")
        return

    os.makedirs(os.path.dirname('/Users/arjunathreya/Projects/airbnb_similar_listings/app/core/airbnb.db'), exist_ok=True)
    print('Initializing database...')

    db = AirbnbDatabase()
    db.execute_query(f'CREATE TABLE IF NOT EXISTS listing ({LISTING_TABLE_SCHEMA})')
    print("Database initialized successfully.")

if __name__ == '__main__':
    initialize_database()