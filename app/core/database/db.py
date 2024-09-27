import os, sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), os.pardir)))

import sqlite3
import os
from contextlib import contextmanager
from typing import List, Dict, Any, Generator
from listing_schema_utils import LISTING_TABLE_SCHEMA

from core.config import settings

class AirbnbDatabase:
    def __init__(self, db_path: str = settings.DB_PATH):
        self.db_path = db_path

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            conn.commit()

    def execute_queries(self, query: str, param_list: List[Dict[str, Any]]) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, param_list)
            conn.commit()

    def fetch_data(self, query: str, params: Dict[str, Any] = None, fetch_all: bool = True) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            return cursor.fetchall() if fetch_all else cursor.fetchone()

    def fetch_many_data(self, query: str, params: Dict[str, Any] = None, size: int = 1) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            return cursor.fetchmany(size)

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