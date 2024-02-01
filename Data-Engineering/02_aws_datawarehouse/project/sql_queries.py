import configparser
import cluster


# CONFIG
# config = configparser.ConfigParser()
# config.read('dwh.cfg')
arn = cluster.get_IAM_role()
# DROP TABLES

staging_schema_drop = "drop schema if exists staging"
dim_schema_drop = "drop schema if exists dim"
fact_schema_drop = "drop schema if exists fact"

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists fact_song_plays"
user_table_drop = "drop table if exists dim_users"
song_table_drop = "drop table if exists dim_songs"
artist_table_drop = "drop table if exists dim_artists"
time_table_drop = "drop table if exists dim_time"

# CREATE SCHEMAS

staging_schema_create = "create schema if not exists staging"
dim_schema_create = "create schema if not exists dim"
fact_schema_create = "create schema if not exists fact"


# CREATE TABLES

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
        artist_location text,
        artist_longitude double precision,
        artist_name text,
        duration double precision,
        num_songs int,
        song_id varchar(200) not null distkey,        
        title text,  
        year int
    )
    """)

songplay_table_create = ("""
    create table if not exists fact.fact_song_plays (
            songplay_id text primary key distkey,
            start_time timestamp,
            user_id bigint sortkey,
            level text,
            song_id varchar(200),
            artist_id varchar(200),
            session_id bigint,
            location text,
            useragent text
        )
""")

user_table_create = ("""
    create table if not exists dim.dim_users (
            user_id bigint not null primary key distkey,
            first_name text,
            last_name text,
            gender text,
            level text
        )
""")

song_table_create = ("""
    create table if not exists dim.dim_songs (
            song_id varchar(200) not null primary key distkey,
            title text,
            artist_id varchar(200),
            year int,
            duration double precision
        )
""")

artist_table_create = ("""
    create table if not exists dim.dim_artists (
            artist_id varchar(200) not null primary key distkey,
            name text,
            location text,
            latitude double precision,
            longitude double precision
        )
""")

time_table_create = ("""
    create table if not exists dim.dim_time (
        start_time timestamp,
        hour int,
        day int,
        week int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging.staging_events from 's3://udacity-dend/log_data' 
    format as json 'auto ignorecase'
    credentials 'aws_iam_role={}'
    ';' compupdate off region 'us-west-2';
""").format(arn)

staging_songs_copy = ("""
copy staging.staging_songs from 's3://udacity-dend/song_data' format as json 'auto'
    credentials 'aws_iam_role={}'
    ';' compupdate off region 'us-west-2';
""").format(arn)

# FINAL TABLES

songplay_table_insert = ("""
insert into fact.fact_song_plays (
    songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    useragent
)
select a.userid::text + '-' + a.ts::text as songplay_id,
        timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time,
        a.userid::bigint as user_id,
        a.level,
        b.song_id,
        b.artist_id,
        a.sessionid as session_id,
        a.location,
        a.useragent
from staging.staging_events a 
left join 
( 
    select songs.*,
        artists.name
    from dim.dim_songs songs
    left join dim.dim_artists artists 
    on songs.artist_id = artists.artist_id
) b
on a.song = b.title 
and a.artist = b.name
where a.page = 'NextSong'
""")

user_table_insert = ("""
insert into dim.dim_users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
select distinct 
    userid::bigint as user_id,
    firstname as first_name,
    lastname as last_name,
    gender,
    level
from staging.staging_events
where page = 'NextSong'
""")

song_table_insert = ("""
insert into dim.dim_songs (
    song_id,
    title,
    artist_id,
    year,
    duration
    )
select distinct 
    song_id,
    title,
    artist_id,
    year,
    duration
from staging.staging_songs
where song_id is not null
""")

artist_table_insert = ("""
insert into dim.dim_artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
select distinct
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
from staging.staging_songs
where artist_id is not null
""")

time_table_insert = ("""
insert into dim.dim_time (
    start_time,
    hour,
    day,
    week,
    year,
    weekday
)
select distinct 
    timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time, 
    DATE_PART('hour', start_time) as hour,
    DATE_PART('day', start_time) as day,
    DATE_PART('week', start_time) as week,
    DATE_PART_YEAR(start_time::date) as year,
    DATE_PART('dayofweek', start_time) as weekday
from staging.staging_events
where page = 'NextSong'
""")

staging_events_success = """
select count(*) from staging.staging_events
"""
staging_songs_success = """
select count(*) from staging.staging_songs
"""

user_table_success = """
select count(*) from dim.dim_users
"""

song_table_success = """
select count(*) from dim.dim_songs
"""

artists_table_success = """
select count(*) from dim.dim_artists
"""

time_table_success = """
select count(*) from dim.dim_time
"""

songplay_table_success = """
select count(*) from fact.fact_song_plays
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
load_staging_success_queries = [staging_events_success, staging_songs_success]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
table_success_queries = [user_table_success, song_table_success, artists_table_success, time_table_success, songplay_table_success]
schemas_drop_queries = [staging_schema_drop, dim_schema_drop, fact_schema_drop]
schemas_create_queries = [staging_schema_create, dim_schema_create, fact_schema_create]