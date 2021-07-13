# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(128),
        last_name VARCHAR(128),
        gender VARCHAR(1),
        level VARCHAR(4)
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(18) PRIMARY KEY,
        title VARCHAR(128),
        artist_id VARCHAR(18),
        year INT,
        duration DECIMAL(12, 5)
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(18) PRIMARY KEY,
        name VARCHAR(128),
        location VARCHAR(128),
        latitude DECIMAL(10, 5),
        longitude DECIMAL(10, 5)
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP WITHOUT TIME ZONE UNIQUE,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id SERIAL PRIMARY KEY NOT NULL,
        start_time TIMESTAMP WITHOUT TIME ZONE
                   NOT NULL
                   REFERENCES time(start_time),
        user_id INT NOT NULL REFERENCES users(user_id),
        level VARCHAR(4),
        song_id VARCHAR(18) REFERENCES songs(song_id),
        artist_id VARCHAR(18) REFERENCES artists(artist_id),
        session_id INT,
        location VARCHAR(256),
        user_agent TEXT
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT into songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT into users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT into artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    select sng.song_id, art.artist_id from
        songs sng
        INNER JOIN artists art on (sng.artist_id = art.artist_id)
        WHERE sng.title = %s
        and art.name = %s
        and sng.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
