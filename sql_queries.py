# SESSION_SONGS
drop_song_gender = "DROP TABLE IF EXISTS SONG_GENDER"
create_song_gender = "CREATE TABLE IF NOT EXISTS SONG_GENDER(gender text, song text, artist_name text \
                        , PRIMARY KEY((gender), song))"

# SESSION_USERS
drop_song_by_artist_and_location = "DROP TABLE IF EXISTS SONG_ARTIST_LOCATION"
create_song_by_artist_and_location = "CREATE TABLE IF NOT EXISTS SONG_ARTIST_LOCATION(song text, artist_name text, \
                                      location TEXT, state text, PRIMARY KEY((location, song), artist_name))"

# SONG_USERS
drop_artist_state = "DROP TABLE IF EXISTS ARTIST_STATE"
create_artist_state = "CREATE TABLE IF NOT EXISTS ARTIST_STATE(artist_name TEXT, state text, song text, " \
                      "PRIMARY KEY((state, artist_name), song))"

list_drop_tables = [drop_song_gender, drop_song_by_artist_and_location, drop_artist_state]
list_create_tables = [create_song_gender, create_song_by_artist_and_location, create_artist_state]