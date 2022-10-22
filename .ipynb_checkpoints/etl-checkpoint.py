import configparser
from datetime import datetime
import os
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    That function to create spark session having hadoop package loaded
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function get spark, input and output path
    - read data from the source
    - transform songs and artists data 
    - loaded to the s3 storage
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration"))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data  + "song_table")

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), 
                            col('artist_name').alias('name'), 
                            col('artist_location').alias('location'), 
                            col('artist_latitude').alias('latitude'), 
                            col('artist_longitude').alias('longitude')).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data  + "artist_table")


def process_log_data(spark, input_data, output_data):
    """
    This function get spark, input and output path
    - read data from the source
    - transform time, users and songplays data 
    - loaded to the s3 storage
    """
    # get filepath to log data file
    log_data = input_data + "log_data/"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table    
    window = Window.partitionBy("userId").orderBy(F.desc("ts"))
    user_table = df.withColumn("rank", F.rank().over(window))\
                .where(col("rank") == 1)\
                .where(col('userId').isNotNull())\
                .select(col('userId').alias('user_id'), 
                        col('firstName').alias('first_name'), 
                        col('lastName').alias('last_name'), 
                        col('gender'), 
                        col('level'))

    
    # write artists table to parquet files
    user_table.write.mode("overwrite").parquet(output_data  + "user_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda row: datetime.fromtimestamp(row / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda row: datetime.fromtimestamp(row / 1000).date(), DateType())
    df = df.withColumn("date", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select('ts', 
                           'timestamp', 
                           'date', 
                           F.hour('timestamp').alias('hour'),
                           F.dayofmonth('timestamp').alias('day'),
                           F.weekofyear('timestamp').alias('week'),
                           F.month('timestamp').alias('month'),
                           F.year('timestamp').alias("year"),
                           F.dayofweek('timestamp').alias("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data  + "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'song_table') 

    # extract columns from joined song and log datasets to create songplays table 
    cond = [df.song == song_df.title, 
           df.length == song_df.duration]
    songplays_table = df.join(song_df, on=cond, how="left").select(col('userId').alias("user_id"),
                                                                'ts',
                                                                F.year('timestamp').alias("year"),
                                                                F.month('timestamp').alias('month'),
                                                                'level',
                                                                'song_id',
                                                                'artist_id',
                                                                col('sessionId').alias("session_id"),
                                                                'location',
                                                                col("userAgent").alias("user_agent")) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data  + "songplays_table")


def main():
    """
    This is a function to execute all processes
    - Create spark session
    - configure data path
    - processing song data
    - processing log data
    """
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3a://udacity-longnpp/sparkify/"
    input_data = './data/'
    output_data = "./data/sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
