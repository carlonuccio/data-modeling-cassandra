import pandas as pd
import os

from create_tables import insert_from_dataframe
import create_tables

def process_files(folder):
    df = pd.DataFrame()
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".csv"):
                df_new = pd.read_csv(os.path.join(root, file))
                df = df.append(df_new, ignore_index=True)
    return df

def process_data(IPCluster, keyspace, folder):

    df = process_files(folder)

    df['gender'] = df['gender'].astype(str)
    df['song'] = df['song'].astype(str)
    df['artist'] = df['artist'].astype(str)

    # 1. Give me every song in my music library that was listened by men
    #   - `SELECT song from SONG_GENDER where gender = 'M'

    # SESSIONID INT, ITEMINSESSION INT, ARTIST TEXT, SONG TEXT, LENGTH TEXT
    SONG_GENDER = df[['gender', 'song', 'artist']].copy()
    SONG_GENDER = SONG_GENDER.dropna(subset=['gender'])
    insert_from_dataframe(IPCluster, keyspace, "SONG_GENDER", SONG_GENDER, ['GENDER', 'SONG', 'ARTIST_NAME'])



def main():
    IPCluster, keyspace = "127.0.0.1","sparkifydb"
    create_tables.main( IPCluster, keyspace)

    process_data(IPCluster, keyspace, "./event_data/")

if __name__ == "__main__":
    main()