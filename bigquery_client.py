from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
from google.oauth2 import service_account
import pandas as pd
import logging
from typing import List, Dict, Optional, Union

logger = logging.getLogger(__name__)

class BigQueryClient:
    """Generic BigQuery client - simple and direct"""
    
    def __init__(self, project_id: str, dataset_id: str, credentials_path: Optional[str] = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Initialize BigQuery client
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            self.client = bigquery.Client(project=project_id)
    
    def insert_data(self, table_name: str, data: List[Dict]) -> bool:
        """Insert data - let BigQuery handle validation"""
        try:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            errors = self.client.insert_rows_json(full_table_id, data)
            
            if errors:
                logger.error(f"Insert errors: {errors}")
                return False
            
            logger.info(f"Inserted {len(data)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            return False
    
    def get_data(self, table_name: str, filters: Optional[Dict] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data with optional filters and limit"""
        try:
            query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            
            # Add filters if provided
            if filters:
                where_conditions = []
                for key, value in filters.items():
                    if isinstance(value, str):
                        where_conditions.append(f"`{key}` = '{value}'")
                    else:
                        where_conditions.append(f"`{key}` = {value}")
                
                if where_conditions:
                    query += " WHERE " + " AND ".join(where_conditions)
            
            # Add limit if provided
            if limit:
                query += f" LIMIT {limit}"
            
            return self.client.query(query).to_dataframe()
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom SQL query"""
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            logger.error(f"Custom query failed: {e}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            self.client.get_table(full_table_id)
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[List[Dict]]:
        """Get table schema information"""
        try:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = self.client.get_table(full_table_id)
            schema = []
            for field in table.schema:
                schema.append({
                    'name': field.name,
                    'type': field.field_type,
                    'mode': field.mode,
                    'description': field.description
                })
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema: {e}")
            return None