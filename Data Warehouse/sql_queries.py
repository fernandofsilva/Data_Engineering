import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_IAM_ROLE_NAME   = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH        = config.get("S3", "LOG_JSONPATH")
LOG_DATA            = config.get("S3", "LOG_DATA")
SONG_DATA           = config.get("S3", "SONG_DATA")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
                                 CREATE TABLE IF NOT EXISTS staging_events
                                   (
                                      artist_name  VARCHAR,
                                      auth         VARCHAR,
                                      first_name   VARCHAR,
                                      gender       CHAR,
                                      item_session INT,
                                      last_name    VARCHAR,
                                      length       DECIMAL,
                                      level        VARCHAR,
                                      location     VARCHAR,
                                      method       VARCHAR,
                                      page         VARCHAR,
                                      registration BIGINT,
                                      session_id   INT,
                                      song_title   VARCHAR,
                                      status       INT,
                                      start_time   BIGINT,
                                      user_agent   VARCHAR,
                                      user_id      INT
                                   )
                                 SORTKEY(start_time)
                              """)

staging_songs_table_create = ("""
                                 CREATE TABLE IF NOT EXISTS staging_songs
                                   (
                                      num_songs   INT,
                                      artist_id   VARCHAR,
                                      latitude    DOUBLE PRECISION,
                                      longitude   DOUBLE PRECISION,
                                      location    VARCHAR,
                                      artist_name VARCHAR,
                                      song_id     VARCHAR,
                                      title       VARCHAR,
                                      duration    DECIMAL,
                                      year        INT
                                   )
                                 DISTKEY(song_id)
                                 SORTKEY(song_id) 
                              """)

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays
                              (
                                 songplay_id INT IDENTITY(0,1),
                                 start_time  TIMESTAMP NOT NULL,
                                 user_id     INT NOT NULL,
                                 level       VARCHAR,
                                 song_id     VARCHAR NOT NULL,
                                 artist_id   VARCHAR,
                                 session_id  INT,
                                 location    VARCHAR,
                                 user_agent  VARCHAR,
                                 PRIMARY KEY(songplay_id)
                              )
                            DISTKEY(song_id)
                            SORTKEY(song_id, start_time)
                         """)

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users
                          (
                             user_id    INT,
                             first_name VARCHAR,
                             last_name  VARCHAR,
                             gender     CHAR,
                             level      VARCHAR,
                             PRIMARY KEY(user_id)
                          )
                        DISTSTYLE ALL
                        SORTKEY(user_id)
                     """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songs
                          (
                             song_id   VARCHAR,
                             title     VARCHAR,
                             artist_id VARCHAR NOT NULL,
                             year      INT,
                             duration  DECIMAL,
                             PRIMARY KEY(song_id)
                          )
                        DISTKEY(song_id)
                        SORTKEY(song_id)
                     """)

artist_table_create = ("""
                          CREATE TABLE IF NOT EXISTS artists
                            (
                               artist_id VARCHAR,
                               name      VARCHAR,
                               location  VARCHAR,
                               latitude  DECIMAL,
                               longitude DECIMAL,
                               PRIMARY KEY(artist_id)
                            )
                          DISTSTYLE ALL
                          SORTKEY(artist_id)
                       """)

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time
                          (
                             start_time TIMESTAMP,
                             hour       INT,
                             day        INT,
                             week       INT,
                             month      INT,
                             year       INT,
                             weekday    INT,
                             PRIMARY KEY(start_time)
                          )
                        SORTKEY(start_time)
                     """)

# STAGING TABLES
staging_events_copy = ("""
                          COPY staging_events
                          FROM {}
                          IAM_ROLE {}
                          JSON {}
                          REGION 'us-west-2'
                       """).format(LOG_DATA, DWH_IAM_ROLE_NAME, LOG_JSONPATH)

staging_songs_copy = ("""
                         COPY staging_songs
                         FROM {}
                         IAM_ROLE {}
                         JSON 'auto'
                         REGION 'us-west-2'
                     """).format(SONG_DATA, DWH_IAM_ROLE_NAME)

# FINAL TABLES
songplay_table_insert = ("""
                            INSERT INTO songplays
                                        (
                                                    start_time,
                                                    user_id,
                                                    level,
                                                    song_id,
                                                    artist_id,
                                                    session_id,
                                                    location,
                                                    user_agent
                                        )
                            SELECT    timestamp 'epoch' + staging_events.start_time/1000 * interval '1 second',
                                      staging_events.user_id,
                                      staging_events.level,
                                      staging_songs.song_id,
                                      staging_songs.artist_id,
                                      staging_events.session_id,
                                      staging_events.location,
                                      staging_events.user_agent
                            FROM      staging_events
                            LEFT JOIN staging_songs
                            ON        (
                                                staging_events.song_title = staging_songs.title
                                      AND       staging_events.artist_name = staging_songs.artist_name)
                            WHERE     (
                                                staging_events.page='NextSong'
                                      AND       staging_songs.song_id IS NOT NULL)
                         """)

user_table_insert = ("""
                        INSERT INTO users
                        SELECT DISTINCT user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level
                        FROM   staging_events
                        WHERE  user_id IS NOT NULL
                     """)

song_table_insert = ("""
                        INSERT INTO songs
                        SELECT DISTINCT song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                        FROM   staging_songs
                     """)

artist_table_insert = ("""
                          INSERT INTO artists
                          SELECT DISTINCT artist_id,
                                          artist_name,
                                          location,
                                          latitude,
                                          longitude
                          FROM   staging_songs
                       """)

time_table_insert = ("""
                        INSERT INTO TIME 
                        (SELECT songplays.start_time,
                                Datepart(h, songplays.start_time),
                                Datepart(d, songplays.start_time),
                                Datepart(w, songplays.start_time),
                                Datepart(mon, songplays.start_time),
                                Datepart(y, songplays.start_time),
                                Datepart(dow, songplays.start_time)
                         FROM   songplays)
                     """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, song_table_create, artist_table_create,
                        time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_songs_copy, staging_events_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
