# End-to-End Serverless Data Pipeline for Real-Time Weather Forecasting

This project is a robust serverless data pipeline designed to automate the collection, processing, and visualization of weather forecast data. By leveraging AWS Lambda, Amazon EventBridge, Snowflake, and Looker Studio, this architecture provides real-time weather insights with minimal overhead.

## Overview

This pipeline fetches real-time weather data from the Open-Meteo API and automatically ingests it into Snowflake. It then leverages Looker Studio to provide interactive dashboards for real-time weather monitoring.

## Architecture

### 1. Orchestration
- **Amazon EventBridge**: Triggers the AWS Lambda function every day at 1 PM to fetch weather data.

### 2. Serverless ETL
- **AWS Lambda** (Python): Fetches data from the Open-Meteo API, processes it, and stores both the raw data and the processed CSV file in AWS S3.

### 3. Data Ingestion
- **Snowflake Snowpipe**: Automatically ingests the processed CSV files into Snowflake from the S3 `/load` folder as soon as they are uploaded.

### 4. Data Exploration & Monitoring
- **Looker Studio**: Connected to Snowflake, enabling interactive exploration and monitoring of the hourly forecast data through a user-friendly dashboard.

## Core Technologies

- **Python**: Used for the Lambda function that fetches and processes the weather data.
- **AWS Lambda**: Serverless compute service to run the Python function.
- **Amazon EventBridge**: Schedules the Lambda function execution.
- **AWS S3**: Stores raw and processed weather data files (CSV).
- **Snowflake (Snowpipe)**: Automatically ingests the data from S3 into the Snowflake warehouse.
- **Looker Studio**: Provides an interactive dashboard to visualize and monitor the weather forecast.

## Pipeline Flow

1. **EventBridge** triggers the Lambda function at the scheduled time (1 PM daily).
2. The **Lambda function** fetches data from the Open-Meteo API and stores the raw data and the processed CSV file in AWS **S3**.
3. **Snowpipe** automatically ingests the newly uploaded CSV files from the S3 `/load` folder into **Snowflake**.
4. **Looker Studio** connects to Snowflake, enabling the creation of interactive dashboards for real-time weather monitoring.

## Setup Instructions

### Prerequisites
- AWS account with permissions to use Lambda, EventBridge, S3, and Snowflake.
- Snowflake account with Snowpipe configured for automatic ingestion from S3.
- Looker Studio account to connect to Snowflake and create dashboards.

### 1. Lambda Setup
- Create an AWS Lambda function with Python runtime.
- Grant the Lambda function necessary permissions for accessing the Open-Meteo API and AWS S3.
- Set up **Amazon EventBridge** to trigger the Lambda function at the required time (1 PM daily).

### 2. Snowflake Setup
- Set up Snowpipe to automatically ingest the CSV files from the S3 `/load` folder into Snowflake.
- Ensure the Snowflake schema is prepared for storing the weather data.

### 3. Looker Studio Setup
- Connect Looker Studio to the Snowflake warehouse.
- Create an interactive dashboard to monitor the weather data (hourly forecasts).


Feel free to clone, fork, or contribute to this repository. If you have any questions or suggestions, feel free to open an issue or pull request.

