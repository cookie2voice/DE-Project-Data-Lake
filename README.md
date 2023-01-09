 ## Overview
 - Building an ETL pipeline that extracts Sparkify data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow Sparkify analytics team to continue finding insights in what songs their users are listening to.

## How to run the Python scripts
```
    cd /path/to/create_tables.py
    python3 create_tables.py   # create and drop tables
    python3 etl.py  #reads and processes files and loads them into tables

```

## Files in the repository
- etl.py: reads and processes files from song_data and log_data with spark and loads them into tables.
- dl.cfg: contains AWS credentials.
- README.md: details and discussions.

## State and justify the database schema design and ETL pipeline.
- Star schema optimized for queries on song play analysis. This includes the following tables:
    - Fact Table
        - songplays - records in log data associated with song plays i.e. records with page NextSong:
            songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    - Dimension Tables
        - users - users in the app: user_id, first_name, last_name, gender, level
        - songs - songs in music database: song_id, title, artist_id, year, duration
        - artists - artists in music database: artist_id, name, location, latitude, longitude
        - time - timestamps of records in songplays broken down into specific units: start_time, hour,
             day, week, month, year, weekday
