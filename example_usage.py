#!/usr/bin/env python3
"""
Example usage of the BigQuery client
"""

from bigquery_client.bigquery_client import BigQueryClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    # Initialize the client
    # You can either use service account credentials or default credentials
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID", "your-project-id")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "your-dataset-id")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # Optional
    
    client = BigQueryClient(
        project_id=project_id,
        dataset_id=dataset_id,
        credentials_path=credentials_path
    )
    
    # Example 1: Check if a table exists
    table_name = "example_table"
    if client.table_exists(table_name):
        print(f"Table {table_name} exists")
        
        # Get table schema
        schema = client.get_table_schema(table_name)
        if schema:
            print("Table schema:")
            for field in schema:
                print(f"  - {field['name']}: {field['type']} ({field['mode']})")
        
        # Get data with filters
        data = client.get_data(
            table_name=table_name,
            filters={"status": "active"},
            limit=100
        )
        print(f"Retrieved {len(data)} rows")
        
    else:
        print(f"Table {table_name} does not exist")
    
    # Example 2: Execute a custom query
    custom_query = f"""
    SELECT COUNT(*) as total_count
    FROM `{project_id}.{dataset_id}.{table_name}`
    WHERE created_date >= '2024-01-01'
    """
    
    result = client.execute_query(custom_query)
    if not result.empty:
        print(f"Total count: {result.iloc[0]['total_count']}")
    
    # Example 3: Insert data (if you have write permissions)
    sample_data = [
        {"id": 1, "name": "John Doe", "email": "john@example.com"},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
    ]
    
    # Uncomment if you want to test insert functionality
    # success = client.insert_data("test_table", sample_data)
    # if success:
    #     print("Data inserted successfully")
    # else:
    #     print("Failed to insert data")

if __name__ == "__main__":
    main()
