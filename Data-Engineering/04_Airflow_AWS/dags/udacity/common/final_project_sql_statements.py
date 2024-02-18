class SqlQueries:

    create_schema = ("""
    create schema if not exists {}
    """)
    
    drop_table = ("""
    drop table if exists {}.{}
    """)

    staging_events_copy = ("""
        copy {}.{} from '{}' 
            format as json 'auto ignorecase'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}';
        """)

    staging_songs_copy = ("""
         copy {}.{} from '{}'  
         format as json 'auto'
         ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}';
        """)
    
    staging_events_table_create= ("""
    create table if not exists staging.staging_events (
        artist text,
        auth text,
        firstname text,
        gender text,
        iteminsession int,
        lastname text,
        length double precision,
        level text,
        location text,
        method text,
        page text,
        registration bigint,
        sessionid int,
        song text,
        status int,
        ts bigint,
        useragent varchar(1000),
        userid text
    )
    """)

    staging_songs_table_create = ("""
    create table if not exists staging.staging_songs (
        artist_id varchar(200) not null,
        artist_latitude double precision,
        artist_location varchar(max),
        artist_longitude double precision,
        artist_name text,
        duration double precision,
        num_songs int,
        song_id varchar(200) not null distkey,        
        title text,  
        year int
    )
    """)

    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        into {}.{}
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging.staging_events
            WHERE page='NextSong'
        ) events
        LEFT JOIN staging.staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        into {}.{}
        FROM staging.staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        into {}.{}
        FROM staging.staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        into {}.{}
        FROM staging.staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, 
            extract(hour from start_time) as hour, 
            extract(day from start_time) as day, 
            extract(week from start_time) as week, 
            extract(month from start_time) as month, 
            extract(year from start_time) as year, 
            extract(dayofweek from start_time) as weekday
        into {}.{}
        FROM fact.fact_song_plays
    """)