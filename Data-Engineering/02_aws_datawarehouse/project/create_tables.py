import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """The function is to drop table if exist

    Args:
        cur (str): cursor from connection
        conn (str): connection to database
    """    
    drop_table_SONG_query = """
    drop table if exists raw_song
    """
    drop_table_LOG_query = """
    drop table if exists raw_log
    """
    drop_table_dim_users_query = """
    drop table if exists dim_users
    """
    drop_table_dim_songs_query = """
    drop table if exists dim_songs
    """
    drop_table_dim_artists_query = """
    drop table if exists dim_artists
    """
    drop_table_dim_time_query = """
    drop table if exists dim_time
    """
    drop_table_fact_songplays_query = """
    drop table if exists fact_song_plays
    """
    drop_table_queries = [
        drop_table_SONG_query,
        drop_table_LOG_query,
        drop_table_dim_users_query,
        drop_table_dim_songs_query,
        drop_table_dim_artists_query,
        drop_table_dim_time_query,
        drop_table_fact_songplays_query
    ]

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create table if not exist

    Args:
        cur (str): cursor from connection
        conn (str): connection to database
    """  
    create_table_SONG_query = """
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
    """
    create_table_LOG_query = """
    create table if not exists raw_log (
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
    """
    create_table_dim_users_query = """
    create table if not exists dim_users (
        user_id bigint not null primary key distkey,
        first_name text not null,
        last_name text,
        gender text,
        level text
    )
    """
    create_table_dim_songs_query = """
    create table if not exists dim_songs (
        song_id bigint not null primary key distkey,
        title text not null,
        artist_id bigint,
        year int,
        duration double precision
    )
    """
    create_table_dim_artists_query = """
    create table if not exists dim_artists (
        artist_id bigint not null primary key distkey,
        name text not null,
        location bigint,
        latitude double precision,
        longitude double precision
    )
    """
    create_table_dim_time_query = """
    create table if not exists dim_time (
        start_time timestamp,
        hour int not null,
        day int,
        week int,
        year int,
        weekday int
    )
    """
    create_table_fact_songplays_query = """
    create table if not exists fact_song_plays (
        songplay_id bigint primary key sortkey,
        start_time timestamp,
        user_id bigint sortkey,
        level text,
        song_id bigint,
        artist_id bigint,
        session_id bigint,
        location text,
        useragent text
    )
    """
    create_table_queries = [
        create_table_SONG_query,
        create_table_LOG_query,
        create_table_dim_users_query,
        create_table_dim_songs_query,
        create_table_dim_artists_query,
        create_table_dim_time_query,
        create_table_fact_songplays_query
    ]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    try:
        drop_tables(cur, conn)
    except Exception as e:
        print(e)
    try:
        create_tables(cur, conn)
    except Exception as e:
        print(e)

    conn.close()


if __name__ == "__main__":
    main()