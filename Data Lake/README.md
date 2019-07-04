# Project: Data Lake

## Introduction
A music streaming startup, Sparkify, has grown their user base and song 
database even more and want to move their data warehouse to a data lake. 
Their data resides in S3, in a directory of JSON logs on user activity on 
the app, as well as a directory with JSON metadata on the songs in their app.

## Project Description
The task is build an Data Lake that extracts data from S3, transforms 
data into a set of dimensional tables and write it back into S3 
for their analytics team to continue finding insights in what songs 
their users are listening to. 

### Analytics Tables

Below the tables the Analytic Team can be use to extract insights.

* Fact Table
  * songplays: records in event data associated with song plays.

```
|-- songplay_id: long (nullable = false)
|-- start_time: string (nullable = true)
|-- user_id: string (nullable = true)
|-- level: string (nullable = true)
|-- song_id: string (nullable = true)
|-- artist_id: string (nullable = true)
|-- session_id: long (nullable = true)
|-- location: string (nullable = true)
|-- user_agent: string (nullable = true)
|-- month: integer (nullable = true)
|-- year: integer (nullable = true)
```


* Dimension Tables
  * users

```
|-- user_id: string (nullable = true)
|-- first_name: string (nullable = true)
|-- last_name: string (nullable = true)
|-- gender: string (nullable = true)
|-- level: string (nullable = true)
```

  * songs
  
```
|-- song_id: string (nullable = true)
|-- title: string (nullable = true)
|-- artist_id: string (nullable = true)
|-- year: long (nullable = true)
|-- duration: double (nullable = true)
```

  * artists

```
|-- artist_id: string (nullable = true)
|-- name: string (nullable = true)
|-- location: string (nullable = true)
|-- latitude: double (nullable = true)
|-- longitude: double (nullable = true)
```

  * time

```
|-- start_time: string (nullable = true)
|-- hour: integer (nullable = true)
|-- day: integer (nullable = true)
|-- week: integer (nullable = true)
|-- month: integer (nullable = true)
|-- year: integer (nullable = true)
|-- weekday: integer (nullable = true)
```

### Files information

The files belows are used in the project.

- etl.py is load data from S3 process into Spark and write it back to S3.
- dl.cfg configuration file with the password, keys and information of the Redshift Cluster

### Usage

Run the script below:

- etl.py

### Libraries used
```
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
```

### Examples

Below there are some samples can be use by the Analytic Team, using 
createOrReplaceTempView it is possible to reuse the same queries 
of Data Warehouse with Spark, so it is a smooth migration to the 
Analytic Team to the Data Lake

#### Top 10 Artist Played

```
songplays = spark.read.parquet(os.path.join(output_data, 'songplays'))
songplays.createOrReplaceTempView("songplays")
artists = spark.read.parquet(os.path.join(output_data, 'artists'))
artists.createOrReplaceTempView("artists")

top_artist = spark.sql("""
                          SELECT artists.name,
                                 Count(songplays.user_id) as Count
                          FROM   songplays
                                 LEFT JOIN artists
                                        ON ( songplays.artist_id = artists.artist_id )
                          WHERE  songplays.artist_id IS NOT NULL
                          GROUP  BY artists.name
                          ORDER  BY Count(songplays.user_id) DESC
                          LIMIT  10
                       """)
```

#### Top 10 Music Played

```
songplays = spark.read.parquet(os.path.join(output_data, 'songplays'))
songplays.createOrReplaceTempView("songplays")
songs = spark.read.parquet(os.path.join(output_data, 'songs'))
songs.createOrReplaceTempView("artists")

top_songs = spark.sql("""
                         SELECT songs.title,
                                Count(songplays.user_id) as Count
                         FROM   songplays
                                LEFT JOIN songs
                                       ON ( songplays.song_id = songs.song_id )
                         WHERE  songplays.artist_id IS NOT NULL
                         GROUP  BY songs.title
                         ORDER  BY Count(songplays.user_id) DESC
                         LIMIT  10
                      """)
```