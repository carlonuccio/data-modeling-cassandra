import pandas as pd
import os

from create_tables import insert_from_dataframe
import create_tables

def process_files(folder):
    """
    open all csv files within a root folder and append to a pandas dataframe
    :param folder: path root folder
    :return: a pandas dataframe
    """
    df = pd.DataFrame()
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".csv"):
                df_new = pd.read_csv(os.path.join(root, file))
                df = df.append(df_new, ignore_index=True)
    return df

def process_data(IPCluster, keyspace, folder):
    """
    - Process data
    - remove na based on each primary key
    - insert into database
    :param IPCluster: Host Cassandra Address
    :param keyspace: keyspace Name
    :param folder: path root folder for song data
    """

    df = process_files(folder)

    df['gender'] = df['gender'].astype(str)
    df['song'] = df['song'].astype(str)
    df['artist'] = df['artist'].astype(str)
    df['location'], df['state'] = df['location'].str.split(', ', 1).str

    # 1. Give me every song in my music library that was listened by men
    #   - SELECT song from SONG_GENDER where gender = 'M'

    # SESSIONID INT, ITEMINSESSION INT, ARTIST TEXT, SONG TEXT, LENGTH TEXT
    SONG_GENDER = df[['gender', 'song', 'artist']].copy()
    SONG_GENDER = SONG_GENDER.dropna(subset=['gender'])
    insert_from_dataframe(IPCluster, keyspace, "SONG_GENDER", SONG_GENDER, ['GENDER', 'SONG', 'ARTIST_NAME'])

    # 2. Give me song of "Fall Out Boy" that is in my music library that was listened in San Jose-Sunnyvale-Santa Clara
    #   - SELECT song from table_2 where artist = 'Fall Out Boy' and location = 'San Jose-Sunnyvale-Santa Clara'
    SONG_ARTIST_LOCATION = df[['song', 'artist', 'location', 'state']].copy()
    SONG_ARTIST_LOCATION = SONG_ARTIST_LOCATION.dropna(subset=['location', 'song'])
    insert_from_dataframe(IPCluster, keyspace, "SONG_ARTIST_LOCATION", SONG_ARTIST_LOCATION,
                          ['SONG', 'ARTIST_NAME', 'LOCATION', 'STATE'])

    # 3. Give me all the artists that was listened in California
    #   - SELECT artist from table_3 where state = 'CA'
    ARTIST_STATE = df[['artist', 'state', 'song']].copy()
    ARTIST_STATE = ARTIST_STATE.dropna(subset=['artist', 'state'])
    insert_from_dataframe(IPCluster, keyspace, "ARTIST_STATE", ARTIST_STATE, ['ARTIST_NAME', 'STATE', 'SONG'])


def main():
    """
    - Creates the sparkify keyspace and relative tables
    - process data
    """
    IPCluster, keyspace = "127.0.0.1","sparkifydb"
    create_tables.main( IPCluster, keyspace)

    process_data(IPCluster, keyspace, "./event_data/")

if __name__ == "__main__":
    main()