import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    
    return spark

 
def process_song_data(spark, input_data, output_data):
    
    """
    Processes song data and creates the songs  Dimentional table
    Parameters:
         spark        : SparkSession
        input_data    : path to input files (location of song_data json files with the songs metadata)
         output_data  : path to store results of the input_data
   """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")

    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = spark.sql(df)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql(artists_table)
    
    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists_table.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function reads the logs JSON files from S3 and processes them in Spark.Then again loaded back to S3. These tables are saved back to the output folder indicated by output_data parameter.
    
    parameter:
            spark       : Spark Session
            input_data  : location of log_data json files with the relates data
            output_data : Directory where to save parquet files
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').cache()

    # extract columns for users table    
    users_table = spark.sql(users_table)
    
    # write users table to parquet files
    users_table.write.parquet(
        output_data + "users_table.parquet", mode="overwrite")


    # create timestamp column from original timestamp column
    
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda d: str(datetime.fromtimestamp(int(d) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = spark.sql(time_table)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "time_table.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ("""
    select log.ts ,log.userid,log.level,songs.song_id,songs.artist_id,log.sessionid,
    log.location,log.useragent,year (log.timestamp) as year,Month (log.timestamp) as month
    from log join songs on log.song = songs.title
    
    """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./Results/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()