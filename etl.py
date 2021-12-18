import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek,monotonically_increasing_id


#config = configparser.ConfigParser()
#config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']='AKIAQB43RGWZVEXKZK6Y'
os.environ['AWS_SECRET_ACCESS_KEY']='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'


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


 

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data =  "s3a://udacity-dend/song_data/A/B/B/*.json"
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    song_data_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_data_df.select('song_id', 'title', 'artist_id', \
                                      'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, \
                     'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = song_data_df.select('artist_id', 'artist_name', \
                                        'artist_location','artist_latitude', \
                                        'artist_longitude')
    artists_table.withColumnRenamed('artist_name', 'name') \
                .withColumnRenamed('artist_location', 'location') \
                .withColumnRenamed('artist_latitude', 'latitude') \
                .withColumnRenamed('artist_longitude', 'longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, \
                               'artists'), 'overwrite')
    song_data_df.createOrReplaceTempView("song_df_table")#overwrite helps

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_data_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_data_df = log_data_df.filter(log_data_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_data_df.select('userId', 'firstName', 'lastName',\
                            'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_data_df = log_data_df.withColumn('timestamp', get_timestamp(log_data_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    log_data_df = log_data_df.withColumn('datetime', get_datetime(log_data_df.ts))
    
    # extract columns to create time table
    time_table = log_data_df.select('datetime') \
                       .withColumn('start_time', log_data_df.datetime) \
                       .withColumn('hour', hour('datetime')) \
                       .withColumn('day', dayofmonth('datetime')) \
                       .withColumn('week', weekofyear('datetime')) \
                       .withColumn('month', month('datetime')) \
                       .withColumn('year', year('datetime')) \
                       .withColumn('weekday', dayofweek('datetime')) \
                       
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,'time/time.parquet'),\
                    'overwrite')

    # read in song data to use for songplays table
    #song_data_df = spark.read.json(song_data)

    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table") 
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_data_df.join(song_df, song_df.artist_name == log_data_df.artist, "inner").distinct()


    songplays_table = songplays_table.select(
            monotonically_increasing_id().alias('songplay_id'),
            col('datetime').alias('start_time'),
            col('userId').alias('user_id'),
            col('level').alias('level'),
            col('song_id').alias('song_id'),
            col('artist_id').alias('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('location'), 
            col('userAgent').alias('user_agent'),
            year('datetime').alias('year'),
            month('datetime').alias('month'))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month')\
                    .parquet(os.path.join(output_data,\
                    'songplays/songplays.parquet'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://lehuyhiep/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()