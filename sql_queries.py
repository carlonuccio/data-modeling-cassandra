# SESSION_SONGS
drop_session_items = "DROP TABLE IF EXISTS SESSION_ITEMS"
create_session_items = "CREATE TABLE IF NOT EXISTS SESSION_ITEMS(SESSIONID INT, ITEMINSESSION INT, ARTIST TEXT, " \
                     "SONG TEXT, LENGTH FLOAT, PRIMARY KEY(SESSIONID, ITEMINSESSION));"

# SESSION_USERS
drop_session_users = "DROP TABLE IF EXISTS SESSION_USERS"
create_session_users = "CREATE TABLE IF NOT EXISTS SESSION_USERS(USERID INT, SESSIONID INT, ITEMINSESSION INT, " \
                       "ARTIST TEXT, SONG TEXT, FIRSTNAME TEXT, LASTNAME TEXT, " \
                       "PRIMARY KEY((USERID, SESSIONID), ITEMINSESSION));"

# SONG_USERS
drop_song_users = "DROP TABLE IF EXISTS SONG_USERS"
create_song_users = "CREATE TABLE IF NOT EXISTS SONG_USERS(SONG TEXT, USERID INT, FIRSTNAME TEXT, LASTNAME TEXT, " \
                    "PRIMARY KEY(SONG, USERID));"

list_drop_tables = [drop_session_items, drop_session_users, drop_song_users]
list_create_tables = [create_session_items, create_session_users, create_song_users]