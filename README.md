# BigQuery Client

A simple and direct BigQuery client for data operations and machine learning workflows.

## Features

- **Simple Authentication**: Support for both service account credentials and default credentials
- **Data Operations**: Insert, query, and manage BigQuery tables
- **Error Handling**: Comprehensive error handling with logging
- **Type Safety**: Full type hints for better development experience
- **Flexible Queries**: Support for custom SQL queries and filtered data retrieval

## Installation

```bash
pip install -r requirements.txt
```

## Setup

1. **Set up Google Cloud credentials**:
   - Option 1: Use service account key file
   - Option 2: Use Application Default Credentials (ADC)

2. **Environment variables** (optional):
   ```bash
   export GOOGLE_CLOUD_PROJECT_ID="your-project-id"
   export BIGQUERY_DATASET_ID="your-dataset-id"
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
   ```

## Usage

### Basic Setup

```python
from bigquery_client import BigQueryClient

# Initialize with service account credentials
client = BigQueryClient(
    project_id="your-project-id",
    dataset_id="your-dataset-id",
    credentials_path="path/to/service-account-key.json"
)

# Or use default credentials
client = BigQueryClient(
    project_id="your-project-id",
    dataset_id="your-dataset-id"
)
```

### Data Operations

#### Check if table exists
```python
if client.table_exists("my_table"):
    print("Table exists!")
```

#### Get table schema
```python
schema = client.get_table_schema("my_table")
for field in schema:
    print(f"{field['name']}: {field['type']}")
```

#### Query data with filters
```python
data = client.get_data(
    table_name="my_table",
    filters={"status": "active", "category": "premium"},
    limit=100
)
```

#### Execute custom queries
```python
query = "SELECT COUNT(*) as count FROM `project.dataset.table` WHERE date > '2024-01-01'"
result = client.execute_query(query)
```

#### Insert data
```python
data_to_insert = [
    {"id": 1, "name": "John", "email": "john@example.com"},
    {"id": 2, "name": "Jane", "email": "jane@example.com"}
]

success = client.insert_data("my_table", data_to_insert)
if success:
    print("Data inserted successfully!")
```

## Error Handling

The client includes comprehensive error handling:
- All methods return safe defaults on failure
- Errors are logged with detailed information
- Insert operations return boolean success indicators
- Query operations return empty DataFrames on failure

## Requirements

- Python 3.8+
- google-cloud-bigquery >= 3.0.0
- pandas >= 1.5.0
- pydantic >= 2.0.0
- python-dotenv >= 1.0.0

## Security Notes

- Never commit service account keys to version control
- Use environment variables for sensitive configuration
- Consider using Application Default Credentials in production
- Ensure proper IAM permissions for your BigQuery operations
