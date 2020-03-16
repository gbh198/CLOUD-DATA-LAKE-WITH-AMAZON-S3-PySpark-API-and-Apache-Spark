import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
                                  

#configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

#get access to AWS EMR
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

print("Successfully Connected...")



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

print("Successfully created SPARK SESSION")




def process_song_data(spark, input_data, output_data):
    """
        Description: This function creates songs_table and artirst_table and writes their Parquet formats to a given S3
        address.
        
        Parameterized with 3 arguments:
        1. spark : Spark Session.
        2. input_data : S3 address where data flows out.
        3. output_data : S3 address where data flows in.
            
    """
    
    #===== STEP 1: CONNECTING AND LOADING song_data =====#
    print("Processing song_data...")

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    print("Loading song_data files from {}...".format(song_data))
    df_songdata = spark.read.json(song_data)
    print("Successfully loaded songdata to DataFrame")
    
    # created song view to write SQL Queries
    df_songdata.createOrReplaceTempView("song_df_table")
    print("Successfully creted view")
    
    
    
    #===== STEP 2: CREATING AND WRITING SONGS_TABLE =====#
    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT sdf.song_id, 
                            sdf.title,
                            sdf.artist_id,
                            sdf.year,
                            sdf.duration
                            FROM song_df_table sdf
                            WHERE song_id IS NOT NULL
                            ORDER BY song_id
                        """)
    print("songs_table schema:")
    songs_table.printSchema()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/songs_table.parquet')
    print("Successfully written to parquet")
    
    
    
    #===== STEP 3: CREATING AND WRITING ARTIRSTS_TABLE =====#
    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT sdf.artist_id, 
                                sdf.artist_name AS name,
                                sdf.artist_location AS location,
                                sdf.artist_latitude AS latitude,
                                sdf.artist_longitude AS longtitude
                                FROM song_df_table sdf
                                WHERE sdf.artist_id IS NOT NULL
                                ORDER BY sdf.artist_id desc
                            """)
    print("artists_table schema:")
    artists_table.printSchema()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/artists_table.parquet')
    print("Successfully written to parquet")
    
    

def process_log_data(spark, input_data, output_data):
    """
        Description: This function creates users_table, time_table, songplays_table and writes their Parquet formats to a given   
        S3 address.
        
        Parameterized with 3 arguments:
        1. spark : Spark Session.
        2. input_data : S3 address where data flows out.
        3. output_data : S3 address where data flows in.
            
    """ 
    
    #===== STEP 4: CONNECTING AND LOADING log_data =====#
    print("Processing log_data...")
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    print("Loading log_data files from {}...".format(log_data))
    df_logdata = spark.read.json(log_data)
    print("Successfully loaded logdata to DataFrame")
    
    # filter by actions for song plays
    df = df_logdata.filter(df_logdata.page == 'NextSong')
    print("Successfully filtered logdata by 'NextSong' column")
    
    # created song view to write SQL Queries
    df.createOrReplaceTempView("log_df_table")
    print("Successfully creted view")

    
    
    #===== STEP 5: CREATING AND WRITING USERS_TABLE =====#
    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT ldf.userId AS user_id, 
                            ldf.firstName AS first_name,
                            ldf.lastName AS last_name,
                            ldf.gender AS gender,
                            ldf.level AS level
                            FROM log_df_table ldf
                            WHERE ldf.userId IS NOT NULL
                            ORDER BY last_name
                        """)
    print("users_table schema:")
    users_table.printSchema()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/users_table.parquet')
    print("Successfully written to parquet")
    
    
    
    #===== STEP 6: CREATING AND WRITING TIME_TABLE =====#
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                           .withColumn('start_time', df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates()
    
    print("time_table schema:")
    time_table.printSchema()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+'time_table/time_table.parquet')
    print("Successfully written to parquet")
    
    

    #===== STEP 7: CREATING AND WRITING SONGPLAYS_TABLE =====#
    # create new SQL view with new columns named songplay_id
    df_songdata_increasingid = df_songdata.withColumn("songplay_id", \
                               monotonically_increasing_id())
    
    df_songdata_increasingid.createOrReplaceTempView("staging_songs")
    df_logdata.createOrReplaceTempView("staging_events") 
    print("Successfully creted view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table  = spark.sql(""" 
                            SELECT DISTINCT
                            s.songplay_id AS songplay_id,
                            to_timestamp(e.ts/1000) AS start_time,
                            e.userId, 
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.sessionId,
                            e.location,
                            e.userAgent
                            FROM staging_events e, staging_songs s
                            WHERE e.song = s.title
                            AND e.artist = s.artist_name
                            AND e.length = s.duration
                            AND e.page = 'NextSong'
                            ORDER BY songplay_id
                        """)
    print("songplays_table schema:")
    songplays_table.printSchema()

    # write songplays table to parquet files partitioned by year and month
songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'songplays_table/songplays_table.parquet')
    print("Successfully written to parquet")
    

def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()


