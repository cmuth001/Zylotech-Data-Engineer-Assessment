# Zylotech-Data-Engineer-Assessment

## Installation
Please install below packages to run the code
1. Install PostgreSQL database drivers
   > pip install psycopg2
2. Install a package called requests
   > pip install requests
3. Install a cassandra  driver to run the Apache Cassandra queries
   > pip install cassandra-driver
4. Install schedule library, a simple library to use for scheduling jobs.
   > pip install schedule


## Dataset
```
API endpoints to collect data
   https://reqres.in/api/users?page=1
   https://reqres.in/api/users?page=2
   https://reqres.in/api/users?page=3
   https://reqres.in/api/users?page=4
```
And below is the sample JSON response from the endpoint API when parameter, page=1:
```
{
   "page":1,
   "per_page":3,
   "total":12,
   "total_pages":4,
   "data":[
                        {
                           "id":1,
                           "email":"george.bluth@reqres.in",
                           "first_name":"George",
                           "last_name":"Bluth",
                           "avatar":"https://s3.amazonaws.com/uifaces/faces/twitter/calebogden/128.jpg"
                        },
                        {
                           "id":2,
                           "email":"janet.weaver@reqres.in",
                           "first_name":"Janet",
                           "last_name":"Weaver",
                           "avatar":"https://s3.amazonaws.com/uifaces/faces/twitter/josephstein/128.jpg"
                        }
                        {
                           "id":3,
                           "email":"emma.wong@reqres.in",
                           "first_name":"Emma",
                           "last_name":"Wong",
                           "avatar":"https://s3.amazonaws.com/uifaces/faces/twitter/olegpogodaev/128.jpg"
                        }
           ]
  }
```

## Project Template
The Assessment workspace includes 4 files:
1. ***create_tables.py***:This script is used to drop and create tables. Run prior to executing ETL scripts.
2. ***sql_queries.py***: Contains all sql queries, and is imported whenever required.
3. ***etl.ipynb***: This notebook contains step by step execution of ETL process with out schedulers job.
4. ***etl.py***: This file feteches data by calling endpoint APIs to create **SQL** and **NoSQL** data model.

## Assessment Steps

Below are the steps I  followed to complete the assessment:

   ### Database Selection:
   Reasons for selecting the databases.
   
      1. NoSQL: Apache Cassandra
         - High Availability:
            * Supports multiple master model, when a single node is lost the ability of the cluster writes are not affected during the crash.
            * 100% uptime and no downtime.
         - Scalability: 
            * Multiple master model can write on any server node.
            * Scalability is directly proportional to the number of nodes in the servers in a cluster.
         - The learning curve for CQL is very minimal as it is similar to SQL.
      2. SQL: PostgreSQL 
         - Performance: PostgreSQL performs well in OLTP/OLAP systems when read/write speeds are required and extensive data analysis is needed.
         - It is better suited for Data Warehousing and data analysis applications that require fast read/write speeds.
         - Supports a wide variety of programming languages.
   ### Create Tables
      1. Created tables using CREATE statements in sql_queries.py.
      2. Existing tables are droped using DROP statements in sql_queries.py.
      3. Ran create_tables.py to create database and tables.
      4. Inserted records into tables using INSERT statements in sql_queries.py.
      5. Aggregation is achieved through SELECT statements in sql_queries.py.
      
   ### ETL Pipeline
   #### RDBMS (PostgreSQL) data modeling
      1. Connected to the created zylotechdb database.
      2. Using requests library collected the data from API endpoints and inserted into relational database.
      3. Validated the data after insertion.
      4. Ran aggregation metrics queries to get the required result.
   #### NoSQL (Apache Cassandra) data modeling
      1. Connected to the created zylotechdb2 KEYSPACE.
      2. Created tables in Apache Cassandra.
      3. Using requests library collected the data from API endpoints and inserted into NoSQL tables.
      4. SELECT statements in sql_queries.py  for aggregation metrics.
   ### Scheduling Job
      Scheduling a job can be done in multiple ways:
         - CRON: Schedule jobs to run ETL scripts periodically at fixed times, dates, or intervals using `crontab`.
         - Flask server: Can Schedule ETL script periodically at certain intervals using `threading.Timer`. We can use this for smaller data sets.
         - Schedule: Can schedule ETL pipeline periodically at pre-determined intervals using `schedule` library.
         - Jenkins: Can start scheduled or manual runs and track their progress through the Jenkins web UI. This jenkins scheduler can be used mostly for high volume datasets. 
         - Apache Airflow: Schedule, monitor and visualize workflows, mostly used in production envirnment.
         
      As it is a small dataset I used schedule library to schedule ETL script which will run for every 12 hours.
      1. Job function is called to run the ETL pipeline every 12 hours using schedule library.
      2. Every 12 hours ETL pipeline will be executed and can view the given aggregation metrics result.
  ### Data Check:
      1.  Data Type Check:Column data type defination as per the data model design specification.
      2.  Data Length Check: Database columns are as per the data model design specifications.
      3. Index/Constraint Check: 
            - Added 'NOT NULL' constraint for the required columns.
            - Unique key columns are Indexed for required column to avoid duplicate entries.

   **DATA COMPLETENESS CHECK:**
   
      1 . Record Count Validation: Compared endpoint records to the inserted records.

## Execute files in the below order each time before running ETL pipeline
   1. create_tables.py
      > python3 create_tables.py
   2. etl.py
      > python3 etl.py

## References:
1. [stackOverflow](https://stackoverflow.com/questions/8856384/sql-select-first-letter-of-a-word)
2. [HTTP requests and JSON parsing in Python](https://stackoverflow.com/questions/6386308/http-requests-and-json-parsing-in-python)
3. [scheduling](https://pypi.org/project/schedule/)
4. [generic logging to my scheduled jobs](https://schedule.readthedocs.io/en/stable/faq.html#what-if-my-task-throws-an-exception)
5. [ETL Testing](http://www.datagaps.com/concepts/etl-testing)
6. [PostgreSQL](https://www.2ndquadrant.com/en/postgresql/postgresql-vs-mysql/)
7. [Apache Cassandra](https://scalegrid.io/blog/cassandra-vs-mongodb/)
