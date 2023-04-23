# New York Times ETL with Airflow, S3, and Redshift

This project is an ETL pipeline using Apache Airflow to fetch the most popular articles from the New York Times API, store the raw JSON data in an S3 bucket, perform basic transformations using Python, store the transformed data as a CSV file in the processed/ folder of the S3 bucket, and load the data into a Redshift table. The data can then be queried from a source table in Amazon Athena. The Airflow server runs on an Ubuntu EC2 instance (t2.medium) and is scheduled to run daily at 3am UTC.

The pipeline supports both production (prod) and quality assurance (qa) environments, with separate S3 buckets and Redshift databases for each environment.

## Key Components

1. **New York Times API**: Source of the most popular articles data.
2. **Amazon S3**: Storage for the raw JSON data and transformed CSV files. Separate buckets are used for prod and qa environments.
3. **Apache Airflow**: Orchestration of the ETL pipeline, running on an Ubuntu EC2 instance (t2.medium).
4. **Amazon Redshift**: Destination data warehouse for storing the processed data. Separate databases are used for prod and qa environments.
5. **Amazon Athena**: Querying platform to analyze the stored data.

## ETL Workflow

1. Call the New York Times API and fetch the most popular articles.
2. Store the raw JSON response in the `raw/` folder of an S3 bucket, using separate buckets for prod and qa environments.
3. Perform basic transformations on the raw data using Python:
    - Extract relevant fields such as title, abstract, published_date, section, and URL.
4. Save the transformed data as a CSV file in the `processed/` folder of the S3 bucket.
5. Use the Redshift COPY command to load the CSV data from the S3 bucket into a Redshift table, using separate databases for prod and qa environments.
6. Query the source table in Amazon Athena to analyze the top articles data.

The ETL pipeline is scheduled to run daily at 3am UTC using Apache Airflow.

## Environment Handling

The pipeline handles different environments by using Airflow variables to switch between prod and qa environments. This includes separate S3 buckets for raw and processed data, as well as separate Redshift databases for each environment.

## Getting Started

1. Set up an AWS account with access to S3, Redshift, and Athena services.
2. Create an EC2 instance (t2.medium) running Ubuntu and install Apache Airflow.
3. Create a New York Times Developer account and obtain an API key.
4. Set up the necessary Airflow connections and variables (e.g., AWS credentials, Redshift connection, S3 bucket names for prod and qa, New York Times API key, etc.).
5. Deploy the DAG code to the Airflow instance and enable the DAG in the Airflow UI.


