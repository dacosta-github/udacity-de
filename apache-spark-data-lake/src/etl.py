"""
    This module reads and processes files from events and songs data and loads them into our AWS
    cluster.
"""
## Libraries
import configparser
from datetime import datetime
import os
import sys
## Spark SQL Functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def create_spark_session():
    """
    This Function:
        - Creates an Spark Session based on the configuration required
    
    Args:
        - None

    Returns:
        - spark (object): An activate local Spark Session - hadoop-aws:2.7.0
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function:
        - Create songs and artists tables from song data stored on a S3 Bucket
        - The function loads the data from S3, process it into songs and artists tables 
        - Write to partitioned parquet files on S3
    
    Args:
        - param: spark (spark.session): An active local Spark session
        - param: input_data (string): S3 bucket with input data
        - param: output_data (string): S3 bucket to store output tables (parquet files)

    Returns:
        - None
    """

    #print('Starting process song data ...')

    ## get filepath to song data file
    #print('Get filepath to song data file...')
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    ## read song data file
    #print('Read song data file...')
    df = spark.read.json(song_data)

    ## extract columns to create songs table
    #print('Extract columns to create songs table...')
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']

    ## write songs table to parquet files partitioned by year and artist
    #print('Write songs table to parquet files partitioned by year and artist...')
    songs_path = os.path.join(output_data, 'songs')
    songs_table.withColumn('_year', df.year).withColumn('_artist_id', df.artist_id) \
        .write.partitionBy(['_year', '_artist_id']).parquet(songs_path, mode='overwrite')
    

    ## extract columns to create artists table
    #print('Extract columns to create artists table...')
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    ## write artists table to parquet files
    #print('Write artists table to parquet files...')
    artists_path = os.path.join(output_data, 'artists')
    artists_table.write.parquet(artists_path, mode='overwrite')

    #print('Process Song Data Completed.')


def process_log_data(spark, input_data, output_data):
    """
    This Function:
        - Create users, time and songplay tables from log data stored on a S3 Bucket
        - Load the data from S3, process it into songs and artists tables 
        - Write to partitioned parquet files on S3
    
    Args:
        - param: spark (object session): An active Spark session
        - param: input_data (string): S3 bucket with input data
        - param: output_data (string): S3 bucket to store output tables

    Returns:
        - None
    """

    #print('Starting Process Log Data ...')

    ## get filepath to log data file
    #print('Get filepath to log data file...')
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    ## read log data file
    #print('Read log data file...')
    df = spark.read.json(log_data)

    ## filter by actions for song plays
    #print('Filter by actions for song plays...')
    df = df.filter(df.page == 'NextSong')

    ## extract columns for users table
    #print('Extract columns for users table...')
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.drop_duplicates(subset='userId')

    ## write users table to parquet files
    #print('Write users table to parquet files...')
    users_path = os.path.join(output_data, 'users')
    users_table.write.parquet(users_path, mode='overwrite')

    ## create datetime column from timestamp column
    #print('Create datetime column from timestamp column...')
    get_datetime = F.udf(lambda ts: datetime.fromtimestamp(ts // 1000), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))

    ## extract columns to create time table
    #print('Extract columns to create time table...')
    time_table = df.select(
        F.col('datetime').alias('start_time'),
        F.hour('datetime').alias('hour'),
        F.dayofmonth('datetime').alias('day'),
        F.weekofyear('datetime').alias('week'),
        F.month('datetime').alias('month'),
        F.year('datetime').alias('year'),
        F.date_format('datetime', 'u').alias('weekday')
    )
    
    ## remove duplicated values
    #print('Remove duplicated values...')
    time_table = time_table.drop_duplicates(subset=['start_time'])

    ## write time table to parquet files partitioned by year and month
    #print('write time table to parquet files partitioned by year and month')
    time_path = os.path.join(output_data, 'time')
    time_table.write.partitionBy(['year', 'month']).parquet(time_path, mode='overwrite')

    ## read in song data to use for songplays table
    #print('Read in song data to use for songplays table...')
    song_path = os.path.join(output_data, 'songs/_year=*/_artist_id=*/*.parquet')
    song_df = spark.read.parquet(song_path)

    ## extract columns from joined song and log datasets to create songplays table
    #print('Extract columns from joined song and log datasets to create songplays table...')
    df = df['datetime', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']

    log_song_df = df.join(song_df, df.song == song_df.title)

    songplays_table = log_song_df.select(
        F.monotonically_increasing_id().alias('songplay_id'),
        F.col('datetime').alias('start_time'),
        F.year('datetime').alias('year'),
        F.month('datetime').alias('month'),
        F.col('userId').alias('user_id'),
        F.col('level').alias('level'),
        F.col('song_id').alias('song_id'),
        F.col('artist_id').alias('artist_id'),
        F.col('sessionId').alias('session_id'),
        F.col('location').alias('location'),
        F.col('userAgent').alias('user_agent')
    )

    ## write songplays table to parquet files partitioned by year and month
    #print('Write songplays table to parquet files partitioned by year and month...')
    songplays_path = os.path.join(output_data, 'songplays')
    songplays_table.write.partitionBy(['year', 'month']).parquet(songplays_path, mode='overwrite')


def main():
    """
    Main Function:
        - Extract songs and events data from S3
        - Transform it into dimensional tables format
        - Load it back to S3 in Parquet format
    
    Args:
        - None

    Returns:
        - None
    """   
    if len(sys.argv) == 3:
        ## aws emr cluster mode
        input_data = sys.argv[1]
        output_data = sys.argv[2]

    else:
        ## local exectuion mode
        config = configparser.ConfigParser()
        config.read('dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        input_data = config['DATALAKE']['input_data']
        output_data = config['DATALAKE']['output_data']


    ## Initiate Spark Session
    #print('ETL Step 1 - Creating Spark Session: ' + str(datetime.now()))
    spark_session = create_spark_session()
    #print('ETL Step 1 - Spark Session Created: ' + str(datetime.now()))


    ## Initiate Song Data Processing on Sapark
    #print('ETL Step 2 - Song Data Processing Started: ' + str(datetime.now()))
    process_song_data(spark_session, input_data, output_data)
    #print('ETL Step 2 - Song Data Processing Completed: ' + str(datetime.now()))


    ## Initiate log Data Processing on Sapark  
    #print('ETL Step 3 - Log Data Processing Started: ' + str(datetime.now()))
    process_log_data(spark_session, input_data, output_data)
    #print('ETL Step 3 - Log Data Processing Completed: ' + str(datetime.now()))


if __name__ == "__main__":
    main()