import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function is responsible for extract data from
        song_data and insets it into table songs, artists.

    Input Arguments:
        spark: spark session
        input_data: input data path
        output_data: output path

    Returns:
        None
    """
    # get filepath to song data file
    #     song_data = os.path.join(input_data, 'song_data/*/*/*.json')
    song_data = os.path.join(
        input_data, 'song_data/A/B/C/TRABCEI128F424C983.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title',
                            'artist_id', 'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(
        os.path.join(output_data, 'songs'),
        mode='overwrite',
        partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table = artists_table.write.parquet(
        os.path.join(output_data, 'artists'),
        mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function is responsible for extract data from
        log_data and insets it into table users, time, songplays.

    Input Arguments:
        spark: spark session
        input_data: input data path
        output_data: output path

    Returns:
        None
    """
    # get filepath to log data file
    #     log_data = input_data + "log_data/*/*/*.json"
    log_data = os.path.join(
        input_data, 'log_data/2018/11/2018-11-12-events.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter('page="NextSong"')
    # extract columns for users table
    users_table = df.select('userId',
                            'firstName',
                            'lastName',
                            'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table = users_table.write.parquet(
        os.path.join(output_data, 'users'),
        mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000))
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn('start_time', get_datetime('timestamp'))

    # extract columns to create time table
    time_table = df.select(
        'start_time',
        hour('start_time').alias('hour'),
        dayofmonth('start_time').alias('day'),
        weekofyear('start_time').alias('week'),
        month('start_time').alias('month'),
        year('start_time').alias('year'),
        date_format('start_time', 'E').alias('weekday')
    ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.parquet(
        os.path.join(output_data, 'time'),
        mode='overwrite',
        partitionBy=['year', 'month']
    )

    # read in song data to use for songplays table
#     song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*.json'))
    song_data = os.path.join(
        input_data, 'song_data/A/B/C/TRABCEI128F424C983.json')
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    song_df = spark.read.json(song_data)

    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT
            e.songplay_id, t.year, t.month,
            e.start_time, e.userId, e.level, s.song_id,
            s.artist_id, e.sessionId, e.location,
            e.userAgent
        FROM events e
        JOIN songs s
         ON (e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration)
        JOIN time_table t USING(start_time)
        WHERE e.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.parquet(
        os.path.join(output_data, 'songplays'),
        mode='overwrite',
        partitionBy=['year', 'month']
    )


def main():
    """
    - Create spark session.

    - Process input files and load data into tables.

    - Closes the connection.

    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lakeac/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
