from setuptools import setup, find_packages

setup(
    name="bigquery_client",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery>=3.0.0",
        "pandas>=1.5.0",
        "pydantic>=2.0.0",
        "python-dotenv>=1.0.0",
    ],
    description="BigQuery client for data operations and ML",
    python_requires=">=3.8",
)