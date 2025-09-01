import logging
import psycopg2
import psycopg2.extras
from typing import Optional
from sqlalchemy import create_engine
from urllib.parse import quote_plus

from qrlib.QRUtils import get_secret
from qrlib.QRComponent import QRComponent


logger = logging.getLogger(__name__)

class BaseDBComponent(QRComponent):
    """Base database component with connection management"""

    def __init__(self):
        self.con: Optional[psycopg2.extensions.connection] = None
        self.cur: Optional[psycopg2.extras.RealDictCursor] = None
        self.engine = None

    def __load_bot_db_vault(self):
        vault = get_secret("bot_database")
        self._dbname = vault["dbname"]
        self._username = vault["username"]
        self._password = vault["password"]
        self._host = vault["host"]
        self._port = vault["port"]

    def __enter__(self):
        self.__load_bot_db_vault()
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Establish database connections"""
        self.con = psycopg2.connect(
            dbname=self._dbname,
            user=self._username,
            password=self._password,
            host=self._host,
            port=self._port,
        )
        self.con.autocommit = True
        self.cur = self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        password = quote_plus(self._password)
        self.engine = create_engine(
            f"postgresql+psycopg2://{self._username}:{password}@{self._host}/{self._dbname}"
        )

    def close(self):
        """Close database connections"""
        if self.cur:
            self.cur.close()
        if self.con:
            self.con.close()

    def execute_query(self, query: str, params: tuple = None, fetch_one: bool = False) -> Optional[list]:
        """Execute a query with error handling"""
        try:
            logger.info(f"Executing query: {query} with params: {params}")
            self.cur.execute(query, params)
            if query.strip().lower().startswith("select"):
                if fetch_one:
                    return self.cur.fetchone()
                
                result = self.cur.fetchall()
                logger.info(f"Query result: {result}")
                return result
            self.con.commit()
            return None
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            self.con.rollback()
            raise

    def fetch_data(self, query: str, params: tuple = None) -> list:
        """
        Fetch data from the database.

        Parameters:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters to pass to the query. Defaults to None.

        Returns:
            list: A list of tuples representing the fetched data.
        """
        try:
            with self.con.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
                return result
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            raise
