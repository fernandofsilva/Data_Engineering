****## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Project Description
The task is build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

### Staging Tables

The tables below will be used to hold the information from the S3 json files before the information be processed.

* staging_events
* staging_songs

### Analytics Tables

Below the tables the Analytic Team will process their queries after the processing the information.

* Fact Table
  * songplays: records in event data associated with song plays.

* Dimension Tables
  * users
  * songs
  * artists
  * time table

### Files information

The files belows are used in the project.

create_table.py create your fact and dimension tables for the star schema in Redshift.
etl.py is load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
sql_queries.pySQL statements, which will be imported into the two other files above.
dwh.cfg configuration file with the password, keys and information of the Redshift Cluster
(optional) create_redshift create the redshift cluster and export the ENDPOINT and ARN information in the dwh.cfg file
(optional) delete_redshift delete the redshift cluster (only if necessary)

### Extract, Transform and Load
Created songs, artist dimension tables from extracting songs_data by selected columns.
Created users, time dimension tables from extracting log_data by selected columns.
Created the most important table fact table from the dimensison tables and log_data called songplays.


### Usage
sql_queries.py: contains all SQL queries of the project.
create_tables.py: run this file after writing for creating tables for the project.

### Libraries used
```
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries
```

### Functions 
create_database: This function helps in droping existing database, create new database and return the connection.
drop_tables: Used to drop the existing tables.
create_tables: This helps in creating above mentioned fact table and dimension tables.

## Notebook
etl.ipynb: A detailed step by step instruction to run and debug your code befor moving to etl.py. it reads and processes a single file from song_data and log_data and loads the data into your tables.

## ETL script
etl.py: read and process files from song_data and log_data and load them to tables.

### Functions
process_song_file: This function is used to read the song file and insert details with selected columns into song and artist dimension table.
process_log_file: read one by one log file and insert details with selected columns into user, time and songplays tables.
process_data: This will call above two functions and show the status of file processed on terminal.
main: used to call the process_data function.
test.py: This will help you to check inserted records in tables.It displays the first 5 rows of each table.

## Usage
```
create_tables.py
   $ python create_tables.py
   $ python etl.py
```

Test the results using
etl.ipynb/et.py
test.ipynb

## Example

Below some examples of query to check the results

Most played songs
```
SELECT COUNT(songplay_id), song_id  FROM songplays GROUP BY song_id ORDER BY COUNT(songplay_id) DESC
```

Artist with more songs
```
SELECT COUNT(artist_id), name  FROM artists GROUP BY name ORDER BY COUNT(artist_id) DESC
```