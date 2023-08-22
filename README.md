# The Data Engineering Project

## Data Engineering: Totes Bags Company

This project showcases a finished Extract Transform Load (ETL) pipeline using AWS lambda's, AWS S3 buckets and python.
The data platform extracts data from an operational database, archives it in a data lake, and makes it availabale in a remodelled online analytical processing (OLAP) data warehouse.
Changes to the database are reflected in the data warehouse is under 30 minutes.

The project is delivers the following:
1) Two S3 buckets (one for ingested data and one for processed data)
2) An "Extraction" Python application (deployed in an AWS Lambda) that continually ingests all tables from the business database. The data is saved in files in the "ingestion" S3 bucket in a CSV format. The ingestion application:
      - operates automatically on an easy to change schedule
      - logs progress to Cloudwatch
      - trigger email alerts in the event of failures
      - follows good security practices (for example, preventing SQL injection and maintaining password security)
3) A "Transformation" Python application (AWS Lambda) that remodels the data into a predefined schema suitable for a data warehouse and stores the data in `parquet` format in the "processed" S3 bucket. 
    The application:
      - triggers automatically when it detects the completion of an ingested data job
      - is logged and monitored
      - populates the dimension and fact tables of a single "star" schema in the warehouse
4) A "Load" Python application (AWS Lambda) that loads the data into a prepared data warehouse. The application:
      - is triggered via completion of the "Transformation" Lambda finishing writing the files to the "processed" S3 bucket
      - is logged and monitored
      - follows good security practices (for example, preventing SQL injection and maintaining password security)
5) Business insights were generated via a Quicksight dashboard that allows users to view useful data in the warehouse

## Project Development

The project was carried out utilising agile practices (daily scrums, kanban boards) and was completed in just over a two week period.

## CICD and Infrastructure as Code

Development and deployment was carried out via terraform and github actions

## Python Code Testing and Development

All Python code should be thoroughly tested, PEP8 compliant, and tested for security vulnerabilities with the `safety` and `bandit` packages. Test coverage average around 90%. Implementation and testing of these can be carried out via Make file and via upload to Github.


## The Data

The primary data source for the project is a moderately complex (but not very large) database called `totesys` which is meant to simulate the back end data of a commercial application. Data is inserted and updated into this database several times a day.

The full ERD for the database is detailed [here](https://dbdiagram.io/d/6332fecf7b3d2034ffcaaa92).

The data is remodelled for this warehouse into three overlapping star schemas. You can find the ERDs for these star schemas:
 - ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
 - ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
 - ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca)
