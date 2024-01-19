
# Project Overview
This project involves extracting, transforming, and loading cryptocurrency price data from a Rapid API endpoint to an AWS Redshift cluster. The process includes understanding the API documentation, extracting data, transforming data types, and loading the data into an S3 bucket before copying it to a Redshift cluster.

# Workflow
## 1. Understanding Rapid API Documentation
Before starting the project, carefully review the Rapid API documentation to understand the available endpoints, request parameters, and response format. Ensure you have the necessary API key for authentication.

## 2. Data Extraction
Use Python to make a GET request to the Rapid API endpoint, extracting cryptocurrency price data. Parse the JSON response to obtain the required data fields.

## 3. Data Transformation
Implement a data transformation step to convert the datatype of the price from string to float or perform any other necessary transformations.

## 4. Data Loading to S3
Upload the transformed data to an S3 bucket. Ensure you have the necessary AWS credentials configured.

## 5. Redshift Data Copy
Copy the data from the S3 bucket to your Redshift cluster. Ensure you have an IAM role with the required permissions.

## 6. IAM Role Configuration
Create an IAM role in the AWS Management Console.
Attach a policy granting necessary permissions for S3 read and Redshift write operations.
When using the IAM role in Python, ensure the AWS CLI credentials are configured with the IAM role's access key and secret key.

