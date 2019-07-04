# Zylotech-Data-Engineer-Assessment

## Installation
Please install below packages to run the code
1. Install the PostgreSQL database drivers
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
And below is an example of what a single endpoint data(json), page=1, look like. 
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
1. ***create_tables.py***:This script is used to drops and creates tables. Run this file to reset tables before each time before run ETL scripts.
2. ***sql_queries.py***: Contains all sql queries, and is imported into required files.
3. ***etl.py***: Collect the data from the endpoints and create a **SQL** and **NoSQL** datamodel
4. ***README.md***: Step by step instructions to run the code.

## Assessment Steps
Below are steps I  followed to complete the assessment:
   #### Create Tables
      1. Wrote CREATE statements in sql_queries.py to create tables.
      2. Wrote DROP statements in sql_queries.py to drop each table if it exists.
      3. Run create_tables.py to create database and tables.
   #### Insert and Aggregation Queries
      1. Wrote INSERT statements in sql_queries.py to insert records into table.
      2. Wrote SELECT statements in sql_queries.py  for aggregation metrics.
   #### ETL Pipeline
      1. Connected to the created zylotechdb database.
      2. Using requests library collected the data from API endpoints and inserted into relational database(PostgreSQL).
      3. Validated the data after insertion.
      4. Ran aggregation metrics queries to get the required result.
      
