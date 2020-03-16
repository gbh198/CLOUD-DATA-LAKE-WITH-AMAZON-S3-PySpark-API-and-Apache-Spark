Gia-Bao HUYNH | [LinkedIN](https://www.linkedin.com/in/gbh198/) | In a "windy" winter day | [UDACITY Data Engineering NanoDegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

# **Cloud Data Lake with S3, PySpark and Apache Spark**

## Table of Contents
-	Preamble
-	Introduction
-	Data Source
-	Setting Up Your AWS EMR
- Schema Design
-	Project Template
-	How To Use
- Potential Bugs

## Preamble

This repo is my work for the Cloud Data Lake project in UDACITY [Data Engineering NanoDegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). Thank you Udacity team for all the support you have provided. 
## Introduction
It is always a story of Sparkify, a music streaming startup who decided to store and process their data onto the cloud. When user base and song database are increasingly bigger, a Data Lake appears to be a good candidate for them to solve this problem.

Besides available databases and other than Data Warehouse, Data Lake serves as a magical place that welcomes all types of data. Utilizing on-top Big Data technologies like Spark in a world of low-cost data storage, Data Lake could host all Big Data operations such as: Training Machine Learning Models, Queries with Schema-on-Read...

Thanks to advances in Big Data and Cloud technologies recently, building a Data Lake is much more time-saving than ever. This allows companies to utilize Data Lake's functionalities for discovering precious insights hidden in messy data. People, with freedom, could come, creativly join, query or get raw data as it is. 

A Data Lake is consituted by two components: one technology that stores data (S3 in our case) and another perfomming data processing (Spark in our case).
Data Lake would never be magical place without the aid of Spark. That Spark also supports both declarative programming (SQL) and imperative programming (DataFrame) would make life simplified. I went for both types of programming in this project.

It is my job, as a data engineer, to build ETL pipelines that **Extract** data from data sources (S3), then **Transform** them into a pre-defined schema (EMR-cluster), and finally, **Load** (write them back in Parquet format) to Data Lake (S3). 

## Data Source
Data sources, a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/), are comprised of two S3 buckets. Two S3 buckets are used to separate two types of available data: **Song Data** (*info about songs and artists*), **Log Data** (*info about user’s activities*). 
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data
- Log data JSON path: s3://udacity-dend/log_json_path.json

*This Log data JSON path file recursively goes through all the paths in folder to extract data.**

All the files available in these two S3 buckets are in JSON format. For example:
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![Image of Yaktocat](https://github.com/gbh198/Cloud-Data-Warehouse-With-Amazon-S3-and-Redshift/blob/master/log-data.png)

## Setting up your AWS EMR

The most and foremost thing to remember is that, the mode of using AWS is essentially renting its services to meet our demands. Therefore, cost of renting plays an important role for any decision of using AWS. 
**Do not forget to kill your Cluster once you finish project.**

**1. Working environment**

It is possible to choose a working environment that suits us best. In most of cases, administering AWS using Management Console is preferable. 
Sometimes, [Insfrastructure as Code (IaC)](https://en.wikipedia.org/wiki/Infrastructure_as_code) is more convenient for Developers. 
If so, Python3 + AWS SDK [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) libraries and their dependencies are good ways to go.

**2. Authorisation**

To get access to your alive Cluster, it is neccesary to supply AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY in your configuration file.
[Check out AWS ceredentials](https://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html)

**3. EMR Machine Set-up**

I decided to configure in high-performance mode as follow:

1 Master Node and 2 "worker nodes" which are all configured with the most recent version of EMR (emr-5.29.0). EC2 machine with type "m" is for multipurposes (storage and RAM optimized at the same time). 

- **Cluster**: 3 x m5.xlarge nodes. 
- **Location**: US-West-Oregon.
- **Configuration**: Hadoop 2.8.5, Hive 2.3.6, Pig 0.17.0, Hue 4.4.0, Spark 2.4.4.

It is recommended to go for "Go to advanced options" rather than "Quick options" while configuring your cluster. If not, some of important programs could be "forgotten". This could potentially generate run-time bugs when interacting with data.

## Schema Design

Sparkify Date Lake will create few folders to store tables which together form a star schema. 
Star design means that it has one Fact Table surrounded by Dimension Tables.
The Fact Table will answer a business question. In case of Sparkify, Fact Tables is about: **“What songs are users listening to?”**.

![Image of Yaktocat](https://github.com/gbh198/Cloud-Data-Warehouse-With-Amazon-S3-and-Redshift/blob/master/Star%20Schema.png)

## Project Template
 
-	**etl.py** – ETL process to Extract data from S3, Create tables, Write in Parquet files and Load back to Data Lake
-	**dl.cfg** - Configuration file giving acces to EMR cluster

## How To Use
**Required libraries and programs**
- configparser
- psycopg2
- Pyspark
- Spark & Hadoop installed 

1. Setting up your EMR cluster. Wait until its status turns to be "Waiting".
2. Setting your file system to root, navigate to the folders that contains python files, and execute following command in terminal:
**python etl.py** 
3.	Delete your EMR cluster, S3 storage (if you want just to test) as soon as you finish.

## Potential Bugs

1. For connection stage, if it throws a bug, it could be configuration file is not correctly intepreted. 
2. For run-time bugs, it could be caused by Spark Session not configured as it is in your EMR Cluster.

