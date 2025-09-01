import logging
from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from app.database.base import BaseDBComponent

logger = logging.getLogger(__name__)

''' QUERIES ON DB TABLE HERE'''

class MatchingStatus(str, Enum):
    """Enum for weightage matching status"""
    PENDING = 'pending'
    PROCESSING = 'processing'
    MATCHED = 'matched'
    UNMATCHED = 'unmatched'
    FAILED = 'failed'


class CbsDataSync(BaseDBComponent):
    """Handles database operations for maintaining status of CBS data synchronization."""

    TABLE_NAME = "cbs_data_sync"

    def __init__(self):
        super().__init__()

    def create_table(self) -> None:
        """Create the CBS data sync table if it doesn't exist."""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                last_updated TIMESTAMP DEFAULT NULL
            )
        """
        try:
            self.execute_query(query)
            self.logger.info(f"Created/verified table {self.TABLE_NAME}")
        except Exception as e:
            self.logger.error(f"Error creating table: {str(e)}")
            raise

    def get_last_update_time(self) -> datetime:
        """Fetch the last update time from the database."""
        query = f"SELECT last_updated FROM {self.TABLE_NAME} ORDER BY id DESC LIMIT 1"
        result = self.execute_query(query, fetch_one=True)

        if result and result["last_updated"]:
            return result["last_updated"]
        return None  
    
    def is_sync_needed(self) -> bool:
        """Check if CBS data sync is needed (more than 1 hour has passed)."""
        last_update = self.get_last_update_time()

        if not last_update:
            return True  

        one_hour_ago = datetime.now() - timedelta(hours=1)
        return last_update < one_hour_ago

    def update_last_sync_time(self) -> None:
        """Update the last sync time in the database."""
        now = datetime.now()
        query = f"""
            INSERT INTO {self.TABLE_NAME} (last_updated)
            VALUES (%s)
        """
        try:
            self.execute_query(query, (now,))
            self.logger.info(f"Updated last sync time to {now}")
        except Exception as e:
            self.logger.error(f"Error updating last sync time: {str(e)}")
            raise

