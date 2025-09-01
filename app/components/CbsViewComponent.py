import time
import logging
import oracledb
import pyodbc

from datetime import datetime
from typing import Optional, Dict, List, Any, Union

from qrlib.QRUtils import get_secret
from qrlib.QRComponent import QRComponent

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Base exception for database-related errors"""
    pass

class ConnectionError(DatabaseError):
    """Exception for database connection issues"""
    pass


class QueryError(DatabaseError):
    """Exception for database query issues"""
    pass


class BaseDBComponent(QRComponent):
    """Base class for database components"""
    
    def __init__(self):
        super().__init__()
        self.connection = None
        self.cursor = None
        
    def _log_error(self, message: str, error: Exception) -> None:
        """Log error with consistent format"""        
        logger.error(f"{message}: {str(error)}")
        logger.error("Stack trace:", exc_info=True)

    def disconnect(self) -> None:
        """Safely disconnect from database"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            logger.error(f"Error disconnecting from database: {e}")
        finally:
            self.cursor = None
            self.connection = None


class SQLServerComponent(BaseDBComponent):
    """Component for SQL Server database operations"""

    def __init__(self):
        super().__init__()
        self._load_credentials()

    def _load_credentials(self) -> None:
        """Load SQL Server credentials"""
        creds = get_secret("sqlserver_cred")
        if not creds:
            raise ConnectionError("SQL Server credentials not found")

        self.server = creds.get('server')
        self.database = creds.get('database')
        self.username = creds.get('username')
        self.password = creds.get('password')
        self.port = creds.get('port', '1433')
        self.corporate_view_table = 'VIEW_CLA_CORPORATE'
        self.individual_view_table = 'VIEW_CLA_INDIVIDUAL'

    def connect(self) -> None:
        """Connect to SQL Server database"""
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
            self.connection = pyodbc.connect(conn_str)
            self.cursor = self.connection.cursor()
            self.logger.info("Successfully connected to SQL Server database")

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            self.logger.error(f"Failed to connect to SQL Server database: {sqlstate}")
            self.disconnect()
            raise ConnectionError(f"Failed to connect to SQL Server database: {str(ex)}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(f"Error during database operation: {exc_type}")
        self.disconnect()
        return not exc_type

    def _execute_query(self, query: str, params: tuple = None, fetch_one: bool = False) -> List[Dict]:
        """Execute query and return results as list of dictionaries"""
        try:
            start_time = time.time()
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if query.strip().lower().startswith("select"):
                if fetch_one:
                    row = self.cursor.fetchone()
                    if row:
                        columns = [column[0] for column in self.cursor.description]
                        return [dict(zip(columns, row))]
                    return []

                rows = self.cursor.fetchall()
                if not rows:
                    return []

                columns = [column[0] for column in self.cursor.description]
                result = [dict(zip(columns, row)) for row in rows]

                end_time = time.time()
                execution_time = end_time - start_time
                self.logger.info(f"Query executed in {execution_time:.4f} seconds")
                return result
            
            self.connection.commit()
            return []

        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {e}")
            raise Exception(f"Error executing query: {str(e)}")

    def fetch_corporate_data(self) -> Optional[List[Dict]]:
        """Fetch data from the corporate view"""
        try:
            self.logger.info("Initiating corporate data fetch from SQL Server...")
            start_time = time.time()

            query = f"SELECT * FROM {self.corporate_view_table}"
            results = self._execute_query(query)

            elapsed_time = time.time() - start_time
            self.logger.info(f"Corporate data fetch completed in {elapsed_time:.2f} seconds")

            if not results:
                self.logger.warning("No corporate data found")
                return []
            return results
        except Exception as e:
            self.logger.error(f"Error fetching corporate data: {e}")
            raise

    def fetch_individual_data(self) -> Optional[List[Dict]]:
        """Fetch data from the individual view"""
        try:
            self.logger.info("Initiating individual data fetch from SQL Server...")
            start_time = time.time()

            query = f"SELECT * FROM {self.individual_view_table}"
            results = self._execute_query(query)

            elapsed_time = time.time() - start_time
            self.logger.info(f"Individual data fetch completed in {elapsed_time:.2f} seconds")

            if not results:
                self.logger.warning("No individual data found")
                return []
            return results
        except Exception as e:
            self.logger.error(f"Error fetching individual data: {e}")
            raise

    
class OracleComponent(BaseDBComponent):
    """Component for Oracle database operations"""

    def __init__(self):
        super().__init__()
        self._load_credentials()

    def _load_credentials(self) -> None:
        """Load Oracle credentials"""
        creds = get_secret("oracle_cred")
        if not creds:
            raise ConnectionError("Oracle credentials not found")
        
        self.username = creds.get('username', 'RPA')
        self.password = creds.get('password', '24#RP$')
        self.host = creds.get('host','192.168.207.24')
        self.port = '1527'
        self.service = 'orcl'
        self.acc_view_table = 'delchnl.vw_account_rpa'
        self.customer_view_table = 'delchnl.vw_customer_details_rpa'
        self.customer_service_table = 'delchnl.vw_rpa_customer_services'
        self.freeze_status = 'delchnl.vw_acct_det_kap'
        self.corporate_table = 'delchnl.vw_corporate_cif_rpa'

    def connect(self) -> None:
        """Connect to Oracle database"""        
        try:
            self.connection = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=f"{self.host}:{self.port}/{self.service}",
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Oracle database")
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle database: {e}")
            self.disconnect()
            raise ConnectionError(f"Failed to connect to Oracle database: {str(e)}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error(f"Error during database operation: {exc_type}")
        self.disconnect()
        return not exc_type

    def _format_date(self, date_str: str) -> str:
        """Format date string to YYYY-MM-DD"""
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")

    def _execute_query(self, query: str,  params: tuple = None, fetch_one: bool = False) -> List[Dict]:
        """Execute query and return results as list of dictionaries"""
        try:
            start_time = time.time()
            self.cursor.execute(query, params)

            if query.strip().lower().startswith("select"):
                if fetch_one:
                    return self.cursor.fetchone()
                
                rows = self.cursor.fetchall()
                if not rows:
                    return []
                
            result = []
            for row in rows:
                row_dict = {}
                for col, value in zip(self.cursor.description, row):
                    row_dict[col[0]] = value
                result.append(row_dict)

            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"Query executed in {execution_time:.4f} seconds")
            self.connection.commit()
            return result
            
        except Exception as e:
            logger.error(f"Error executing query", e)
            raise QueryError(f"Error executing query: {str(e)}")

    def fetch_customer_data(self) -> Optional[Dict]:
        """Fetch sample customer data"""
        try:
            query = f"""
                select * from {self.customer_view_table} FETCH FIRST 3 ROWS ONLY
            """ 
            results = self._execute_query(query)
            logger.info(f"Fetched sample customer data: {len(results)} records")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching sample customer data: {e}")
            raise

    def fetch_account_data(self) -> Optional[Dict]:
        """Fetch  account data"""
        try:
            query = f"""
                select * from {self.acc_view_table}
            """ 
            results = self._execute_query(query)
            logger.info(f"Fetched sample account data: {len(results)} records")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching account data : {e}")
            raise Exception(f"Error fetching account data record : {e}")


    def fetch_customer_service_data(self, ACCT_NUM: str) -> Optional[Dict]:
        try:
            query = f"""
                select * from {self.customer_service_table} where ACCT_NUM = :ACCT_NUM
            """ 
            params = {"ACCT_NUM": ACCT_NUM}
            
            results = self._execute_query(query, params)
            if not results:
                logger.warning(f"No customer service data found for account number: {ACCT_NUM}")
                return None
            return results[0]
        except Exception as e:
            logger.error(f"Error fetching customer service account data: {e}")
            raise Exception(f"Error fetching customer service account data : {e}")
        
    def fetch_freeze_data(self, ACCT_NUM: str) -> Optional[Dict]:
        """Fetch  account data"""
        try:
            query = f"""
                select * from {self.freeze_status} where ACCT_NUM = :ACCT_NUM
            """ 
            params = {"ACCT_NUM": ACCT_NUM}
            
            results = self._execute_query(query, params)
            if not results:
                logger.warning(f"No customer service data found for account number: {ACCT_NUM}")
                return None
            return results[0]
        except Exception as e:
            logger.error(f"Error fetching account data : {e}")
            raise Exception(f"Error fetching account data record : {e}")

    def fetch_cbs_data(self) -> Optional[List[Dict]]:
        """Fetch CBS data for account and cheque number"""        
        try:
            logger.info("Initiating CBS data fetch from Oracle...")
            start_time = time.time()
            
            query = f"""
            SELECT 
                c.CIF_ID,
                c.CUST_FIRST_NAME,
                c.CUST_MIDDLE_NAME,
                c.CUST_LAST_NAME,
                c.FATHERS_NAME as CUST_FATHERS_NAME,
                c.SPOUSE_NAME as CUST_SPOUSE_NAME,
                c.GRAND_FATHERS_NAME as CUST_GRANDFATHERS_NAME,
                c.CUST_DOB,
                c.CIF_OPN_DATE AS CUST_CIF_OPN_DATE,
                c.CTZ_NUMBER,
                c.ISSUE_DATE AS CTZ_ISSUE_DATE,
                c.CTZ_ISSUED_DISTRICT,
                c.NID_NUMBER,
                c.NID_ISSUEDDISTRICT,
                c.PP_NUMBER,
                c.PP_EXPDATE,
                c.MOBILE_NO,
                c.KYC_STATUS,
                c.PERM_ADDDRES,
                a.ACCT_NUMBER,
                a.ACCT_NAME,
                a.ACCT_OPN_DATE AS ACCT_OPN_DATE,
                a.ACCT_STATUS,
                a.FREZ_CODE
            FROM 
                {self.customer_view_table} c
            JOIN 
                {self.acc_view_table} a ON c.CIF_ID = a.CIF_ID
            ORDER BY 
                c.CIF_ID, a.ACCT_NUMBER
            """
            results = self._execute_query(query)
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"CBS data fetch completed in {elapsed_time:.2f} seconds")
            
            if not results:
                self.logger.warning("No CBS data found")
                return []
            return results
            
        except Exception as e:
            logger.error(f"Error fetching CBS data: {e}")
            raise Exception(f"Error fetching CBS data : {e}")
        
    def fetch_corporate_data(self) -> Optional[List[Dict]]:
        """Fetch CBS data for account and cheque number"""        
        try:
            logger.info("Initiating CBS data fetch from Oracle...")
            start_time = time.time()
            
            query = f"""
            SELECT 
                *
            FROM 
                {self.corporate_table}
            """
            results = self._execute_query(query)
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"CBS Corporate data fetch completed in {elapsed_time:.2f} seconds")
            
            if not results:
                self.logger.warning("No CBS data found")
                return []
            return results
        except Exception as e:
            logger.error(f"Error fetching CBS data: {e}")
            raise Exception(f"Error fetching CBS data : {e}")


#     def get_sync_status(self) -> Dict:
#         """Get current CBS data sync status"""
#         with self.cbs_table as cbdb:
#             results = cbdb.get_sync_status()
#         return results
    
#     def force_sync_cbs_data(self) -> Dict:
#         """Force immediate synchronization of CBS data"""
#         logger.info("Manual CBS data sync initiated")
#         return self.cbs_table.force_sync(self)
    
#     def update_sync_interval(self, hours: int) -> None:
#         """Update the CBS data sync interval"""
#         return self.cbs_table.update_sync_interval(hours)