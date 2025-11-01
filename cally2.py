import json
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',       
    'password': 'admin',   
    'database': 'Spotify_Project_Final'
}

# Path to your JSON file
JSON_FILE_PATH = 'dd.json'


def connect_to_database():
    """Create a connection to the MySQL database"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("✓ Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"✗ Error connecting to MySQL: {e}")
        return None


def insert_playlist(cursor, playlist_info):
    """Insert playlist information"""
    try:
        query = """
        INSERT INTO Playlists (PlaylistID, Title, UserID, Description)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Title=VALUES(Title), Description=VALUES(Description)
        """
        cursor.execute(query, (
            playlist_info['playlistId'],
            playlist_info['title'],
            1,  # LocalUser ID
            playlist_info.get('description')
        ))
        print(f"✓ Inserted playlist: {playlist_info['title']}")
    except Error as e:
        print(f"✗ Error inserting playlist: {e}")


def insert_artist(cursor, artist):
    """Insert artist and return the ArtistID"""
    try:
        query = """
        INSERT INTO Artists (Name, SourceArtistID)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE ArtistID=LAST_INSERT_ID(ArtistID)
        """
        cursor.execute(query, (artist['name'], artist.get('id')))
        return cursor.lastrowid
    except Error as e:
        print(f"✗ Error inserting artist {artist['name']}: {e}")
        return None


def insert_album(cursor, album):
    """Insert album and return the AlbumID"""
    if not album:
        return None
    
    try:
        query = """
        INSERT INTO Albums (Title, SourceAlbumID)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE AlbumID=LAST_INSERT_ID(AlbumID)
        """
        cursor.execute(query, (album['name'], album.get('id')))
        return cursor.lastrowid
    except Error as e:
        print(f"✗ Error inserting album {album['name']}: {e}")
        return None


def insert_song(cursor, song, album_id):
    """Insert song and return the SongID"""
    try:
        query = """
        INSERT INTO Songs (Title, Duration_Seconds, VideoID, AlbumID)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE SongID=LAST_INSERT_ID(SongID)
        """
        cursor.execute(query, (
            song['title'],
            song.get('duration'),
            song['videoId'],
            album_id
        ))
        return cursor.lastrowid
    except Error as e:
        print(f"✗ Error inserting song {song['title']}: {e}")
        return None


def insert_song_artists(cursor, song_id, artists):
    """Link song with its artists"""
    for idx, artist in enumerate(artists):
        artist_id = insert_artist(cursor, artist)
        if artist_id:
            try:
                query = """
                INSERT INTO Song_Artists (SongID, ArtistID, IsPrimary)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE IsPrimary=VALUES(IsPrimary)
                """
                is_primary = (idx == 0)  # First artist is primary
                cursor.execute(query, (song_id, artist_id, is_primary))
            except Error as e:
                print(f"✗ Error linking song to artist: {e}")


def insert_thumbnails(cursor, song_id, thumbnails):
    """Insert song thumbnails"""
    for thumbnail in thumbnails:
        try:
            query = """
            INSERT INTO Thumbnails (SongID, URL)
            VALUES (%s, %s)
            """
            cursor.execute(query, (song_id, thumbnail['url']))
        except Error as e:
            print(f"✗ Error inserting thumbnail: {e}")


def insert_filepath(cursor, song_id, file_path):
    """Insert song file path"""
    try:
        query = """
        INSERT INTO FilePaths (SongID, FilePathURL)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE FilePathURL=VALUES(FilePathURL)
        """
        cursor.execute(query, (song_id, file_path))
    except Error as e:
        print(f"✗ Error inserting file path: {e}")


def insert_playlist_song(cursor, playlist_id, song_id, track_order):
    """Link song to playlist"""
    try:
        query = """
        INSERT INTO Playlist_Songs (PlaylistID, SongID, TrackOrder)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE TrackOrder=VALUES(TrackOrder)
        """
        cursor.execute(query, (playlist_id, song_id, track_order))
    except Error as e:
        print(f"✗ Error linking song to playlist: {e}")


def insert_library_info(cursor, export_date, app_version):
    """Insert library metadata"""
    try:
        query = """
        INSERT INTO Library_Info (KeyName, KeyValue)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE KeyValue=VALUES(KeyValue)
        """
        cursor.execute(query, ('ExportDate', export_date))
        cursor.execute(query, ('AppVersion', app_version))
        print("✓ Inserted library information")
    except Error as e:
        print(f"✗ Error inserting library info: {e}")


def import_data():
    """Main function to import all data from JSON to MySQL"""
    
    # Load JSON file
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✓ Successfully loaded JSON file: {JSON_FILE_PATH}")
    except FileNotFoundError:
        print(f"✗ Error: File '{JSON_FILE_PATH}' not found!")
        return
    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON: {e}")
        return
    
    # Connect to database
    connection = connect_to_database()
    if not connection:
        return
    
    cursor = connection.cursor()
    
    try:
        # Insert playlist information
        playlist_info = data['playlistInfo']
        insert_playlist(cursor, playlist_info)
        
        # Insert library metadata
        insert_library_info(cursor, data['exportDate'], data['appVersion'])
        
        # Process each song
        songs = data['songs']
        print(f"\n📀 Processing {len(songs)} songs...")
        
        for idx, song in enumerate(songs, 1):
            print(f"\n[{idx}/{len(songs)}] Processing: {song['title']}")
            
            # Insert album if exists
            album_id = None
            if song.get('album'):
                album_id = insert_album(cursor, song['album'])
            
            # Insert song
            song_id = insert_song(cursor, song, album_id)
            
            if song_id:
                # Insert artists and link to song
                if song.get('artists'):
                    insert_song_artists(cursor, song_id, song['artists'])
                
                # Insert thumbnails
                if song.get('thumbnails'):
                    insert_thumbnails(cursor, song_id, song['thumbnails'])
                
                # Insert file path
                if song.get('url'):
                    insert_filepath(cursor, song_id, song['url'])
                
                # Link song to playlist
                insert_playlist_song(cursor, playlist_info['playlistId'], song_id, idx)
                
                print(f"  ✓ Successfully processed song: {song['title']}")
        
        # Commit all changes
        connection.commit()
        print("\n" + "="*50)
        print("✓ ALL DATA IMPORTED SUCCESSFULLY!")
        print("="*50)
        
    except Error as e:
        print(f"\n✗ An error occurred: {e}")
        connection.rollback()
        print("✗ Changes rolled back")
    
    finally:
        cursor.close()
        connection.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    print("="*50)
    print("    JSON TO MYSQL DATA IMPORTER")
    print("="*50)
    import_data()
