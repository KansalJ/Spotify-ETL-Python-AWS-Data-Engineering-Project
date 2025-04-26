# Spotify ETL Pipeline Using AWS

## Introduction
This project focuses on building an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS. The pipeline retrieves data from the Spotify API, transforms it into the desired format, and loads it into AWS Data Store for further analysis.

## Architecture Diagram
![This is an image](https://github.com/KansalJ/Spotify-ETL-Python-AWS-Data-Engineering-Project/blob/main/Spotify_AWS_Data_Pipeline%20.jpeg)

## About the Dataset/API
The Spotify API provides extensive information about music artists, albums, and songs. This data serves as the core input for the ETL pipeline.

## AWS Services Used
- **Amazon S3 (Simple Storage Service):** Scalable object storage service for storing raw and transformed data securely.
- **AWS Lambda:** Serverless computing service used to execute ETL logic in response to trigger events.
- **Amazon CloudWatch:** Monitoring service for metrics, log collection, and alarm notifications.
- **AWS Glue Crawler:** Tool for automatic schema inference and metadata catalog creation.
- **AWS Glue Data Catalog:** Managed metadata repository integrated with various AWS analytics tools.
- **Amazon Athena:** Interactive query service for analyzing data stored in Amazon S3 using SQL.

## Dependencies
Install the required Python packages:

```bash
pip install pandas
pip install numpy
pip install spotipy
```

Project Execution Flow
- Extract Data:- AWS Lambda triggers the extraction process every minute.
- Data is fetched from the Spotify API.

- Store Raw Data:- Retrieved data is stored in Amazon S3 (raw format).

- Transform Data:- A separate AWS Lambda function transforms raw data into a structured format.

- Load Transformed Data:- Transformed data is stored back into Amazon S3.

- Query and Analyze:- Utilize Amazon Athena to perform SQL queries for data analysis.


Key Features
- Serverless and Scalable: Leveraging AWS Lambda for a cost-effective, event-driven architecture.
- Real-Time Data Extraction: Automates the collection and transformation processes with minimal manual intervention.
- Schema Management: Uses AWS Glue Crawler and Data Catalog for efficient schema handling.
- On-Demand Analysis: Simple SQL queries through Athena for quick insights.

Benefits
- Fully automated pipeline for data integration and analysis.
- High reliability and scalability with AWS services.
- Simplified and efficient querying process using SQL.




