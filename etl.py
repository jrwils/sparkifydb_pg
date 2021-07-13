import os
import glob
import psycopg2
import pandas as pd
from sql_queries import (
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    user_table_insert,
    song_select,
    songplay_table_insert,
)


def process_song_file(cur, filepath):
    """
    Extracts song and artist information from a song .json file,
    filters the necessary attributes, and then inserts the data
    into both the songs and artists tables.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_df = df.filter(
        items=[
            'song_id',
            'title',
            'artist_id',
            'year',
            'duration'
        ]
    )
    song_data = list(song_df.values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_df = artist_data = df.filter(
        items=[
            'artist_id',
            'artist_name',
            'artist_location',
            'artist_latitude',
            'artist_longitude'
        ]
    )
    artist_data = list(artist_df.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts log information from .json log files representing
    song plays.
    Filters out all records where page != 'NextSong'.
    Takes timestamp information and populates the time table.
    Takes songplay information and populates the songplay table.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [
        df['ts'],
        df['ts'].dt.hour,
        df['ts'].dt.day,
        df['ts'].dt.week,
        df['ts'].dt.month,
        df['ts'].dt.year,
        df['ts'].dt.weekday,
    ]
    column_labels = (
        'timestamp',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday',
    )
    df_dict = {}
    for idx, t_label in enumerate(column_labels):
        df_dict[t_label] = time_data[idx]
    time_df = pd.DataFrame(df_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.filter(['userId', 'firstName', 'lastName', 'gender', 'level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Recursively walks file directories starting at 'filepath'
    and applies the passed 'func' argument on each file
    with cursor and datafile parameters.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Runs the entire ETL process.
    """
    conn = psycopg2.connect(
        """
        host=127.0.0.1
        dbname=sparkifydb
        user=student
        password=student
        """
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
