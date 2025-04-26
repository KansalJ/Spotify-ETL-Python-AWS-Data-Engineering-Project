import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3 # used for s3 storage CRUD operations - will explore more in future
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist = sp.user_playlists('spotify')#not used

    playlist_link = "https://open.spotify.com/playlist/3cEYpjA9oz9GiPac4AsH4n?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split("/")[-1].split('?')[0]

    spotify_data = sp.playlist_tracks(playlist_URI)
    filename = "spotify_raw" + str(datetime.now()) + ".json"
    
    s3_client = boto3.client('s3') 
    s3_client.put_object(
        Bucket = 'spotify-etl-project-kartik-kansal',
        Key = 'raw_data/to_be_processed/'+filename,
        Body = json.dumps(spotify_data)
        )

