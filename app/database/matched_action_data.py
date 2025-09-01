import pandas as pd
from enum import Enum
from typing import Any, Optional, Dict, List

from app.database.base import BaseDBComponent

class MatchedAction(Enum):
    PENDING = "pending"          # File is newly added but not processed
    PROCESSING = "processing"    # File is in processing stage
    COMPLETED = "completed"      # File processing completed successfully
    FAILED = "failed"            # File processing failed


class MatchedExcelTable(BaseDBComponent):
    """Handles database operations for reviewed excel documents for matched records"""

    TABLE_NAME = "matched_excel_table"

    def __init__(self):
        super().__init__()

    def create_excel_review_table(self):
        """Create the excel review table if it does not already exist"""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME}(
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) UNIQUE NOT NULL,   
            file_path VARCHAR(255),  -- Optional file path column
            status VARCHAR(55) NOT NULL DEFAULT 'pending',
            error_message TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_processed_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );

            -- Create index on filename for faster lookups
            CREATE INDEX IF NOT EXISTS idx_reviewed_excel_filename
            ON {self.TABLE_NAME}(filename);

            -- Create index on status for filtering by status
            CREATE INDEX IF NOT EXISTS idx_reviewed_excel_status
            ON {self.TABLE_NAME}(status);
        """
        self.execute_query(query)
        self.logger.info(f"Created/verified table {self.TABLE_NAME}")

    def get_excel_file(self, filename: str) -> Optional[Dict]:
        """Get file details including processing status by filename"""
        query = f"""
            SELECT 
                id, filename, file_path, status, error_message, attempt_count, 
                last_processed_at, created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE filename = %s
        """
        result = self.execute_query(query, (filename,), fetch_one=True)
        if result:
            return {
                "id": result['id'],
                "filename": result['filename'],
                "file_path": result.get('file_path', ''),
                "status": result['status'],
                "error_message": result.get('error_message', ''),
                "attempt_count": result['attempt_count'],
                "last_processed_at": result.get('last_processed_at'),
                "created_at": result['created_at'],
                "updated_at": result['updated_at']
            }
        return None
    
    def get_excel_by_id(self, id: int) -> Optional[Dict]:
        """Get file details including processing status by filename"""
        query = f"""
            SELECT 
                id, filename, file_path, status, error_message, attempt_count, 
                last_processed_at, created_at, updated_at
            FROM {self.TABLE_NAME}
            WHERE id = %s
        """
        result = self.execute_query(query, (id,), fetch_one=True)
        if result:
            return {
                "id": result['id'],
                "filename": result['filename'],
                "file_path": result.get('file_path', ''),
                "status": result['status'],
                "error_message": result.get('error_message', ''),
                "attempt_count": result['attempt_count'],
                "last_processed_at": result.get('last_processed_at'),
                "created_at": result['created_at'],
                "updated_at": result['updated_at']
            }
        return None

    def get_file_by_status(self, status: MatchedAction) -> List[Dict]:
        """Get all files by processing status"""
        query = f"""
            SELECT id, filename, status, attempt_count, created_at
            FROM {self.TABLE_NAME}
            WHERE status = %s
            ORDER BY created_at ASC
        """
        rows = self.execute_query(query, (status,))
        return [{
            "id": row['id'],
            "filename": row['filename'],
            "status": row['status'],
            "attempt_count": row['attempt_count'],
            "created_at": row['created_at']
        } for row in rows]

    def insert_excel_file(self, filename: str, file_path: Optional[str] = None, 
                          status: MatchedAction = MatchedAction.PENDING):
        """Insert new file with initial status and optional file path"""
        try:
            insert_query = f"""
                INSERT INTO {self.TABLE_NAME} 
                (filename, file_path, status, created_at, updated_at)
                VALUES (%s, %s, %s, NOW(), NOW())
            """
            self.execute_query(insert_query, (filename, file_path, status))
        except Exception as e:
            self.logger.error(f"Error updating file {id}: {e}")
            raise Exception(f"Error updating file {id}: {e}")

    def update_excel_file(self, id: int, status: MatchedAction, error_message: Optional[str] = None):
        """Update file status and retry count"""
        try:
            query = f"""
                UPDATE {self.TABLE_NAME}
                SET 
                    status = %s,
                    error_message = %s,
                    attempt_count = CASE 
                        WHEN status = 'failed' THEN attempt_count
                        ELSE attempt_count + 1
                    END,
                    last_processed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
            """
            self.execute_query(query, (status, error_message, id))
        except Exception as e:
            self.logger.error(f"Error updating file {id}: {e}")
            raise Exception(f"Error updating file {id}: {e}")
    
    def mark_as_failed(self, id: int, error_message: str):
        """Mark a file as failed with an error message"""
        self.update_excel_file(id, MatchedAction.FAILED, error_message)

    def mark_as_completed(self, id: int):
        """Mark a file as successfully completed"""
        self.update_excel_file(id, MatchedAction.COMPLETED)

    def get_failed_files(self) -> List[Dict]:
        """Get all failed files"""
        query = f"""
            SELECT id, filename, error_message, attempt_count, last_processed_at
            FROM {self.TABLE_NAME}
            WHERE status = 'failed'
            ORDER BY updated_at DESC
        """
        rows = self.execute_query(query)
        return [{
            "id": row['id'],
            "filename": row['filename'],
            "error_message": row['error_message'],
            "attempt_count": row['attempt_count'],
            "last_processed_at": row['last_processed_at']
        } for row in rows]

    def delete_excel_file(self, id: str):
        """Delete file tracking record by filename"""
        try:
            query = f"""
                DELETE FROM {self.TABLE_NAME}
                WHERE id = %s
            """
            self.execute_query(query, (id,))
        except Exception as e:
            self.logger.error(f"Error deleting file {id}: {e}")
            raise Exception(f"Error deleting file {id}: {e}")


class MatchedActionTable(BaseDBComponent):
    """Handles database operations for matched actions between source tickets and CIF data"""

    TABLE_NAME = "matched_action_table"

    def __init__(self):
        super().__init__()

    def create_action_table(self) -> None:
        """Create the matched action table if it doesn't exist"""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                document_id INTEGER REFERENCES matched_excel_table(id), 
                cif_id VARCHAR(255),
                source_entity_type VARCHAR(50),
                acct_name VARCHAR(255),
                source_name VARCHAR(255),
                name_score FLOAT,
                fathers_name VARCHAR(255),
                source_fathers_name VARCHAR(255),
                fathers_name_score FLOAT,
                grand_fathers_name VARCHAR(255),
                source_grandfathers_name VARCHAR(255),
                grandfathers_name_score FLOAT,
                cust_dob DATE,
                ctz_number VARCHAR(255),
                source_citizenship_no VARCHAR(255),
                citizenship_no_score FLOAT,
                ctz_issue_date DATE,
                source_issue_date DATE,
                ctz_issued_district VARCHAR(255),
                nid_number VARCHAR(255),
                acct_number VARCHAR(255),
                acct_status VARCHAR(50),
                frez_code VARCHAR(50),
                source_account_no VARCHAR(255),
                source_address TEXT,
                source_pan_no VARCHAR(255),
                source_nid VARCHAR(255),
                source_company_name VARCHAR(255),
                source_company_pan_no VARCHAR(255),
                source_company_registration_no VARCHAR(255),
                total_score FLOAT,
                source_subject TEXT,
                source_chalani_no VARCHAR(255),
                source_letter_date TIMESTAMP,
                source_institution TEXT,
                source_enforcement_request TEXT,
                source_suspicious_activity TEXT,
                action VARCHAR(100),
                status VARCHAR(50) DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """
        try:
            self.execute_query(query)
            self.logger.info(f"Created/verified table {self.TABLE_NAME}")
        except Exception as e:
            self.logger.error(f"Error creating table: {str(e)}")
            raise
        
    def insert_data(self, data: Dict[str, Any]) -> None:  # Use Dict[str, Any]
        """Insert matched action record"""
        columns_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{self.TABLE_NAME}';
        """
        try:
            columns_result = self.fetch_data(columns_query)  # Use fetch_data instead of execute_query
            table_columns = [row[0] for row in columns_result]  # Extract column name
        except Exception as e:
            self.logger.error(f"Error fetching column names: {str(e)}")
            raise

        valid_data = {k: v for k, v in data.items() if k in table_columns}

        for key, value in valid_data.items():
            if isinstance(value, pd.Timestamp):  # Assuming you are using pandas
                valid_data[key] = value.to_pydatetime()
            if pd.isna(value):
                valid_data[key] = None

        if not valid_data:
            raise ValueError("No valid columns found in the provided data for this table.")

        fields_str = ", ".join(k.lower() for k in valid_data.keys())  # Make this lower case!
        placeholders = ", ".join([f"%({field})s" for field in valid_data.keys()])

        query = f"""
            INSERT INTO {self.TABLE_NAME}
            ({fields_str}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
            RETURNING id;
        """
        try:
            self.execute_query(query, valid_data)
            self.logger.info(
                f"Successfully inserted matched action record with id: {valid_data.get('acc_name', 'N/A')}")  # Use GET to handle missing key
        except Exception as e:
            self.logger.error(f"Error inserting matched action: {str(e)}")
            raise

    def update_status(self, record_id: int, new_status: str, error_message: str = None) -> None:
        """Update the status of the matched action record"""
        valid_statuses = ['pending', 'processing', 'completed', 'failed']
        if new_status not in valid_statuses:
            raise ValueError(f"Invalid status: {new_status}. Must be one of {valid_statuses}")

        if new_status == 'Failed' and not error_message:
            raise ValueError("Error message is required when status is 'Failed'")

        query = f"""
            UPDATE {self.TABLE_NAME}
            SET status = %s, 
                error_message = %s,
                retry_count = CASE WHEN %s = 'Failed' THEN retry_count + 1 ELSE retry_count END,
                updated_at = NOW()
            WHERE id = %s
        """
        try:
            self.execute_query(query, (new_status, error_message, new_status, record_id))
            self.logger.info(f"Updated status of matched action {record_id} to {new_status}")
        except Exception as e:
            self.logger.error(f"Error updating matched action status: {str(e)}")
            raise

    def get_records_by_status(self, status: str) -> List[Dict]:
        """Get matched action records for a specific status"""
        valid_statuses = ['pending', 'processing', 'completed', 'failed']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")

        query = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE status = %s
            ORDER BY created_at ASC
        """
        try:
            results = self.execute_query(query, (status,))
            return [self._format_result(result) for result in results]
        except Exception as e:
            self.logger.error(f"Error getting matched actions: {str(e)}")
            raise

    def get_records_by_cif_id(self, cif_id: str) -> List[Dict]:
        """Get matched action records for a specific CIF_ID"""
        query = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE CIF_ID = %s
            ORDER BY created_at DESC
        """
        try:
            results = self.execute_query(query, (cif_id,))
            return [self._format_result(result) for result in results]
        except Exception as e:
            self.logger.error(f"Error getting matched actions for CIF_ID {cif_id}: {str(e)}")
            raise

    def get_failed_records(self, max_retry_count: int = 3) -> List[Dict]:
        """Get all failed matched action records that haven't exceeded max retry count"""
        query = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE status = 'Failed' AND retry_count < %s
            ORDER BY updated_at DESC
        """
        try:
            results = self.execute_query(query, (max_retry_count,))
            return [self._format_result(result) for result in results]
        except Exception as e:
            self.logger.error(f"Error getting failed matched actions: {str(e)}")
            raise

    def get_all_records(self) -> List[Dict]:
        """Get all matched action records"""
        query = f"""
            SELECT * FROM {self.TABLE_NAME}
            ORDER BY created_at DESC
        """
        try:
            results = self.execute_query(query)
            return [self._format_result(result) for result in results]
        except Exception as e:
            self.logger.error(f"Error getting all matched actions: {str(e)}")
            raise

    def update_action(self, record_id: int, new_action: str) -> None:
        """Update the action of a matched action record"""
        query = f"""
            UPDATE {self.TABLE_NAME}
            SET action = %s, updated_at = NOW()
            WHERE id = %s
        """
        try:
            self.execute_query(query, (new_action, record_id))
            self.logger.info(f"Updated action of matched action {record_id} to {new_action}")
        except Exception as e:
            self.logger.error(f"Error updating matched action: {str(e)}")
            raise

    def delete_record(self, record_id: int) -> None:
        """Delete matched action record"""
        query = f"""
            DELETE FROM {self.TABLE_NAME}
            WHERE id = %s
        """
        try:
            self.execute_query(query, (record_id,))
            self.logger.info(f"Successfully deleted matched action record with id: {record_id}")
        except Exception as e:
            self.logger.error(f"Error deleting matched action: {str(e)}")
            raise

    def _format_result(self, result: Dict) -> Dict:
        """Format database result into a consistent dictionary format"""
        formatted_result = {}

        for key, value in result.items():
            formatted_result[key.lower()] = value

        return formatted_result