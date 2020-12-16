# Project: Data Modeling with Apache Cassandra

_This repository contains the results of the "Data Modelling with Apache Cassandra" Project which is part of the Udacity Data Engineering Nanodegree._

## Introduction
A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query the data to generate the results, since the data resides in a directory of CSV files on user activity on the app.

They'd like a data engineer to create a Apache Cassandra database with tables designed to optimize queries on song play analysis, and bring you on the project.


## Project Description
In this project, I will apply what I learned in data modelling with Apache Cassandra and build an ETL pipeline using Python. To complete the project, I defined the fact and dimension tables for a star scheme for a particular analytical focus, and write an ETL pipeline that transfers data from files in two local directories to these tables in Apache Cassandra using Python and SQL. I are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.


## Data

The data sources (available locally), shared by Sparkify, are the following:

### Events Dataset
For this project, I'll be working with one dataset: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Data Model

## ETL Process

## Project repository structure
The following folders and files are contained in the project repository:

```
postgres-data-modeling
│   README.md                          # Project description and documentation
│   .gitignore                         # Files and extension ignored in commited
│   docker-compose.yml                 # Container for postgres and pgadmin
│   requirements.txt        
│
└───data                               # Locally data source (json files)
│   
└───resources                          # Project resources (images, others...)
│     │   schema-design.png        
│   
└───src                                # Source code
│   └───notebooks                      # Jupyter notebooks
│   │   |   ETL-pipeline.ipynb
│   └───scripts                        # Python code
│       |   create_tables.py
│       |   etl.py 
│       |   cql_queries.py
```

<br/>

The main files: