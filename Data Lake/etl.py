import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Load Keys file
config = configparser.ConfigParser()
config.read("dl.cfg")

# Set Keys to S3 using enviroment variables
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """

    This function creates a spark session

    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """

    This function load song dataset process, extract
    songs and artist tables and writes it back to S3,
    the mode overwrite it use to avoid errors if there
    are files with the save name in the folders

    :params spark: Spark session
    :params input_data: Path to the song data
    :params output_data: Path to save tables

    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"))

    # extract columns to create artists table
    artists_table = (df.selectExpr("artist_id        as artist_id",
                                   "artist_name      as name",
                                   "artist_location  as location",
                                   "artist_latitude  as latitude",
                                   "artist_longitude as longitude")
                     .dropDuplicates())

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    """

    This function load log dataset process, extract
    songplay, user and time tables and writes it back to S3,
    the mode overwrite it use to avoid errors if there are files
    with the save name in the folders

    :params spark: Spark session
    :params input_data: Path to the log data
    :params output_data: Path to save tables

    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    users_table = (df.selectExpr("userId    as user_id",
                                 "firstName as first_name",
                                 "lastName  as last_name",
                                 "gender",
                                 "level")
                   .dropDuplicates())

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # extract columns to create time table
    time_table = (df.selectExpr("timestamp             as start_time",
                                "hour(timestamp)       as hour",
                                "day(timestamp)        as day",
                                "weekofyear(timestamp) as week",
                                "month(timestamp)      as month",
                                "year(timestamp)       as year",
                                "weekday(timestamp)    as weekday")
                  .dropDuplicates())

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(output_data, "time"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs"))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df,
                              (df.song == song_df.title)
                              & (df.artist == song_df.artist_id),
                              "left").selectExpr("monotonically_increasing_id() as songplay_id",
                                                 "timestamp                     as start_time",
                                                 "userId                        as user_id",
                                                 "level",
                                                 "song_id",
                                                 "artist_id",
                                                 "sessionId                     as session_id",
                                                 "location",
                                                 "userAgent                     as user_agent",
                                                 "month(timestamp)              as month",
                                                 "year(timestamp)               as year")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(output_data, "songplays"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://bucket-udacity-dend"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
