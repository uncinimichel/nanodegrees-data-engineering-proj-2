import glob
import os
import pandas as pd
import psycopg2

from sql_queries import *


def process_time_table(df, cur):
    '''

    This method process the log_data Panda DataFrame.
    It filters by "NextSong" and add Time record to the Time Table

    :param df: DataFrame representing the log_data file
    :param cur: Postgres cursor
    :return: None
    '''
    df = df[df.page == "NextSong"]

    t = pd.to_datetime(df['ts'], unit='ms')

    time_data = {'start_time': t,
                 'hour': t.dt.hour,
                 'day': t.dt.day,
                 'week': t.dt.week,
                 'month': t.dt.month,
                 'year': t.dt.year,
                 'weekday': t.dt.weekday}

    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


def process_user_table(df, cur):
    '''

    This method expect the log_data to be Panda DataFrame (df).
    It extract Users information and add them to the User Table

    :param df: DataFrame representing the log_data file
    :param cur: Postgres cursor
    :return: None
    '''
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

# Log file you will create: song,artist
def process_song_file(cur, filepath):
    '''

    This method process the song_data
    It extract Songs and Artists information and add them to the songs and artists table

    :param cur: Postgres cursor
    :param filepath: the path of the song_data
    :return: None
    '''
    df = pd.read_json(filepath, lines=True)

    df_song_table = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = df_song_table.values[0].tolist()
    cur.execute(song_table_insert, song_data)

    df["artist_latitude"] = df["artist_latitude"].astype(str)
    df["artist_longitude"] = df["artist_longitude"].astype(str)

    df_artist_table = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artist_data = df_artist_table.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

# Log file you will create: time, user and songplay
def process_log_file(cur, filepath):
    '''

    This method process the log_data and it inserts times, users and songplays records in the appropriate tables

    :param cur: Postgres cursor
    :param filepath: the path of the log_data
    :return: None
    '''
    df = pd.read_json(filepath, lines=True)

    process_time_table(df, cur)
    process_user_table(df, cur)

    for index, row in df.iterrows():
        # Find song by song title in the local
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    It walks the filepath and it enumerate all the files that it finds. 
    then each file is passed to the 'func' callback  

    :param cur: Postgres cursor
    :param conn: Postgres connections
    :param filepath: the base path for all the file
    :param func: callback to invoke for each file read
    :return:
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
