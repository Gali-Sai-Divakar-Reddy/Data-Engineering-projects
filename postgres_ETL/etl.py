import os
import glob
import configparser
import psycopg2
import pandas as pd
from sql_queries import *

def read_config():
    # Create a ConfigParser object
    config = configparser.ConfigParser()
 
    # Read the configuration file
    config.read('config.ini')
 
    # Access values from the configuration file
    db_host = config.get('Database', 'hostname')
    db_default = config.get('Database', 'default_database')
    db_name = config.get('Database', 'database')
    db_username = config.get('Database', 'username')
    db_pwd = config.get('Database', 'pwd')
    db_port = config.get('Database', 'port_id')
 
    # Return a dictionary with the retrieved values
    config_values = {
        'db_name': db_name,
        'db_default': db_default,
        'db_host': db_host,
        'db_username': db_username,
        'db_pwd': db_pwd,
        'db_port': db_port
    }
 
    return config_values

class Database:
    def __init__(self, config_data):
        self.conn = psycopg2.connect(
            host= config_data['db_host'],
            dbname = config_data['db_name'],
            user = config_data['db_username'],
            password = config_data['db_pwd'],
            port = config_data['db_port']
        )
        self.cur = self.conn.cursor()

    def close(self):
        self.conn.close()

def extract_data_from_file(db, filepath, func):
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
    for i, datafile in enumerate(all_files,1):
        func(db.cur, datafile)
        db.conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def transform_song_data(cur, datafile):
    df = pd.read_json(datafile, lines=True)
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location',
                       'artist_latitude', 'artist_longitude']].values[0])
    load_data(cur, song_data, artist_data, None, None, None)

def transform_log_data(cur, datafile):

    # open log file
    log_df = pd.read_json(datafile, lines=True)

    # filter by NextSong section
    log_df = log_df[log_df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(log_df['ts'])

    # time data records
    time_data = [(tt.value, tt.hour, tt.day, tt.week, tt.month, tt.year, tt.weekday()) for tt in t]
    time_df = pd.DataFrame(time_data, columns=('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'))

    # user table records
    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Load time data, user data, and songplay data
    load_data(cur, None, None, time_df, user_df, log_df)

def load_data(cur, song_data, artist_data, time_df, user_df, log_df):
    if song_data and artist_data:
        cur.execute(song_table_insert, song_data)
        cur.execute(artist_table_insert, artist_data)
    if not time_df.empty:
        for _, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    else:
        print("No data in time_df")
    if not user_df.empty:
        for _, row in user_df.iterrows():
            cur.execute(user_table_insert, list(row))
    else:
        print("No data in user_df")
    if not log_df.empty:
        for index, row in log_df.iterrows():
            cur.execute(song_select, (row.song, row.artist, row.length))
            result = cur.fetchone()
            if result:
                songid, artistid = result
            else:
                songid, artistid = None, None
            songplay_data = (index, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'],row['location'], row['userAgent'])
            cur.execute(songplay_table_insert, songplay_data)

def main():
    db = None
    try:
        config_data = read_config()
        db = Database(config_data)
        extract_data_from_file(db, filepath='data/song_data', func=transform_song_data)
        extract_data_from_file(db, filepath='data/log_data',func=transform_log_data)
        print("ETL process completed successfully.")
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
    finally:
        if db:
            db.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()