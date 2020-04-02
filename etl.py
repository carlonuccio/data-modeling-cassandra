import pandas as pd
import os

from create_tables import insert_from_dataframe, query_keyspace
import create_tables

def process_files(folder):
    """
    - open all csv files within a root folder and append to a pandas dataframe
    - write all data in csv file
    :param folder: path root folder
    :return: a pandas dataframe
    """
    df = pd.DataFrame()
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".csv"):
                df_new = pd.read_csv(os.path.join(root, file))
                df = df.append(df_new, ignore_index=True)
    df.to_csv('./event_data_new.csv', encoding='utf-8', index=False)
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

    df['artist'] = df['artist'].astype(str)
    df['song'] = df['song'].astype(str)
    df['length'] = df['length'].astype(float)
    df['firstName'] = df['firstName'].astype(str)
    df['lastName'] = df['lastName'].astype(str)

    # Part 1: Give me the artist, song title and song’s length in the music app history that was
    # heard during sessionId = 338, and itemInSession = 4

    # SESSIONID INT, ITEMINSESSION INT, ARTIST TEXT, SONG TEXT, LENGTH TEXT
    session_items = df[['sessionId', 'itemInSession', 'artist', 'song', 'length']].copy()
    session_items = session_items.dropna(subset=['sessionId', 'itemInSession'])
    insert_from_dataframe(IPCluster, keyspace, "SESSION_ITEMS", session_items, ['SESSIONID', 'ITEMINSESSION',
                                                                                'ARTIST', 'SONG', 'LENGTH'])

    # Part 2: Give me only the following: name of artist, song (sorted by itemInSession) and user(first and last
    # name) for userid = 10, sessionid = 182

    session_users = df[['userId', 'sessionId', 'itemInSession', 'artist', 'song', 'firstName', 'lastName']].copy()
    session_users = session_users.dropna(subset=['sessionId', 'userId'])
    session_users['userId'] = session_users['userId'].astype(int)
    insert_from_dataframe(IPCluster, keyspace, "session_users", session_users,['USERID', 'SESSIONID', 'ITEMINSESSION',
                                                                             'ARTIST', 'SONG', 'FIRSTNAME', 'LASTNAME'])

    # Part 3: Give me every user name(first and last) in my music
    # app history who listened to the song ‘All Hands Against His Own’

    song_users = df[['song', 'userId', 'firstName', 'lastName']].copy()
    song_users = song_users.dropna(subset=['song', 'userId'])
    song_users['userId'] = song_users['userId'].astype(int)
    song_users.to_csv('./song_users.csv', encoding='utf-8', index=False)
    insert_from_dataframe(IPCluster, keyspace, "song_users", song_users, ['SONG', 'USERID', 'FIRSTNAME', 'LASTNAME'])


def query_result(IPCluster, keyspace):
    """
    return query result
    :param IPCluster: Host Cassandra Address
    :param keyspace: keyspace Name
    """
    query_keyspace(IPCluster, keyspace,"SELECT artist, song, length from session_items "\
                                       "WHERE sessionId=338 AND itemInSession=4")

    query_keyspace(IPCluster, keyspace,"SELECT artist, song, firstName, lastName "
                                       "FROM session_users WHERE userId=10 and sessionId=182")

    query_keyspace(IPCluster, keyspace,"SELECT firstName, lastName FROM song_users "
                                       "WHERE song=\'All Hands Against His Own\'")


def main():
    """
    - Creates the sparkify keyspace and relative tables
    - process data
    - query result
    """
    IPCluster, keyspace = "127.0.0.1","sparkifydb"
    create_tables.main( IPCluster, keyspace)

    process_data(IPCluster, keyspace, "./event_data/")
    query_result(IPCluster, keyspace)

if __name__ == "__main__":
    main()