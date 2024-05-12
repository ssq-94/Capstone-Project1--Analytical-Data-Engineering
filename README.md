# Capstone-Project1--Analytical-Data-Engineering
The project aims to perform Analytical Data Engineering by ingesting data from various sources, loading it into the Snowflake data warehouse, and preparing it for Business Intelligence (BI) usage with the help of the Metabase tool.

** 1. Project Overview
The project is divided into three main parts: data ingestion, data transformation, and data analysis/dashboard.

![image](https://github.com/ssq-94/Capstone-Project1--Analytical-Data-Engineering/assets/78969075/c5ac009b-be42-4657-b76a-8221ad2b1707)

** 2. About Data
TPCDS (Retail Sales) is a well-known dataset for database testing. It contains sales records from websites and catalogs, inventory levels, and 15 dimensional tables (customers, warehouses, items, etc.).

- RDS: All tables, except for the inventory tables, are stored in the Postgres DB in AWS RDS. These tables are refreshed every day with the latest sales data, requiring daily ETL processes.
- S3 Bucket: The single Inventory table is stored in a S3 bucket. Each day, a new file containing the most recent data is deposited into the S3 bucket. Note: The inventory table typically registers data at the end of each week, leading to one entry per item per warehouse on a weekly basis.


** 3. Project Requirements
A. Database Setup
- Set up a Snowflake database.
- Create the TPCDS database.
- Create a RAW schema.
- Create the inventory table.
  
B. EC2 Instances
- Set up two EC2 instances.
- Use a t2.large instance for Airbyte.
- Use a t2.small instance for Metabase.

C.Docker Installation
- Install Docker on both created instances.


** 4. Project Infrastructure
- Servers: Create several servers on the AWS cloud.
- Tools: Airbyte for data ingestion, and Metabase as the BI tool for building dashboards.
- Cloud Data Warehouse: Snowflake.
- AWS Lambda: Use AWS Lambda, to ingest data from AWS data storage (S3).


** 5. Project Steps
Part One: Data Ingestion
The first part of the project involves Data Ingestion. It entails connecting to two data sources: the Postgres database and the AWS S3 bucket. Utilizing Airbyte, establish a connection to the raw_st schema of the Postgres database on AWS RDS, and transfer all tables to the Snowflake data warehouse. In addition, leverage AWS Lambda to connect to the AWS S3 bucket and transfer the file named inventory.csv from the S3 bucket to the Snowflake data warehouse.

Part Two: Data Transformation
The next stage of the project focuses on data transformation within the Snowflake data warehouse. This involves reshaping tables from their original structure to the desired format. Throughout this phase, tasks include creating a data model, developing ETL scripts, and establishing a schedule for the data loading process.

Part Three: Data Analysis
In the last part of this project, establish a connection between the Snowflake data warehouse and the BI tool (Metabase). This connection allows for the creation and display of dashboards and reports in Metabase, utilizing the data stored in Snowflake.








