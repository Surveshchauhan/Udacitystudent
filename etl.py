
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creating Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loading input data for songs (song-data.zip).
    Reading JSON files.
    Processing data to extract info needed to build songs and artists tables.
    """
    # get filepath to song data file
    song_data = input_data + '/song_data/A/A/A/*.json'
    #print("song")
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration",'artist_name').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path=output_data+"/songs/songs.parquet", mode="overwrite")
    #print(df.show())
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
            Loading input data for logs (log-data.zip).
            Reading JSON files.
            Processing data to extract info needed to build users and time tables.
            """
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level", "sessionId", "location", "userAgent").dropDuplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    # """
    # Not used since it is enough with timestamp column
    # """
    # get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    # df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
     # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))
    df = df.withColumn("year", year("timestamp"))

    # write time table to parquet files partitioned by year and month
    #print(df.show())
    time_table = df.select(col("ts"), col("hour"), col("day"), col("week"), col("month"), col("year"), col("weekday")).distinct()
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time_table/')

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(output_data + 'songs/')
    #print(df_songs.show())
    #print(df.show())
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(df_songs, (df.song == df_songs.title) & (df.artist == df_songs.artist_name) & (df.length == df_songs.duration), 'left_outer'). \
    select( \
           df.timestamp, \
           df.userId, \
           df.level, \
           df_songs.song_id, \
           df_songs.artist_id, \
           df.sessionId, \
           df.location, \
           df.userAgent,  \
           df.year, \
           df.month)    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'songplays_table')


def main():
    spark = create_spark_session()
    #print("hi")
    input_data = "s3a://udacity-dend/"
    output_data = "./Resutls/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()