import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists raw_events"
staging_songs_table_drop = "drop table if exists raw_song"
songplay_table_drop = "drop table if exists fact_song_plays"
user_table_drop = "drop table if exists dim_users"
song_table_drop = "drop table if exists dim_songs"
artist_table_drop = "drop table if exists dim_artists"
time_table_drop = "drop table if exists dim_time"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists raw_events (
        artist text,
        auth text not null,
        firstname text not null,
        gender text not null,
        iteminsession int,
        lastname text,
        length double precision,
        level text,
        location text,
        method text,
        page text,
        registration double precision,
        sessionid int,
        song text,
        status int,
        ts bigint,
        useragent text,
        userid int
    )
    """)

staging_songs_table_create = ("""
    create table if not exists raw_song (
        song_id int not null distkey,
        artist_id int not null,
        title text not null,
        artist_name text not null,
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location text,
        duration double precision not null,
        year int,
        num_songs int
    )
    """)

songplay_table_create = ("""
    create table if not exists fact_song_plays (
            songplay_id bigint primary key distkey,
            start_time timestamp,
            user_id bigint sortkey,
            level text,
            song_id bigint,
            artist_id bigint,
            session_id bigint,
            location text,
            useragent text
        )
""")

user_table_create = ("""
    create table if not exists dim_users (
            user_id bigint not null primary key distkey,
            first_name text not null,
            last_name text,
            gender text,
            level text
        )
""")

song_table_create = ("""
    create table if not exists dim_songs (
            song_id bigint not null primary key distkey,
            title text not null,
            artist_id bigint,
            year int,
            duration double precision
        )
""")

artist_table_create = ("""
    create table if not exists dim_artists (
            artist_id bigint not null primary key distkey,
            name text not null,
            location bigint,
            latitude double precision,
            longitude double precision
        )
""")

time_table_create = ("""
    create table if not exists dim_time (
        start_time timestamp,
        hour int not null,
        day int,
        week int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy sporting_event_ticket from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""").format(config['ARN'])

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
