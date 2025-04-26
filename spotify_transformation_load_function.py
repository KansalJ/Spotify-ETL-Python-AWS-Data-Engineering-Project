import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        #print(album_name)
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                            'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    return album_list
    
def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list
    
    
def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    Bucket = "spotify-etl-project-kartik-kansal"
    Key = "raw_data/to_be_processed"
    
    spotify_data = []
    spotify_keys = []
    for file in s3_client.list_objects(Bucket = Bucket,Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            #print('file is ok : ' + file_key)
            response = s3_client.get_object(Bucket= Bucket,Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_keys.append(file_key)
            spotify_data.append(jsonObject)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        #print (album_list)  
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)
        song_df['song_added'] =  pd.to_datetime(song_df['song_added'])
        
        song_key = "transformed_data/songs_data/song_transformed_" + str(datetime.now()) + ".csv"
        song_buffer= StringIO()
        song_df.to_csv(song_buffer,index = False)
        song_content = song_buffer.getvalue()
        s3_client.put_object( Bucket = Bucket, Key = song_key, Body = song_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer= StringIO()
        artist_df.to_csv(artist_buffer,index = False)
        artist_content = artist_buffer.getvalue()
        s3_client.put_object( Bucket = Bucket, Key = artist_key, Body = artist_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer= StringIO()
        album_df.to_csv(album_buffer,index = False)
        album_content = album_buffer.getvalue()
        s3_client.put_object( Bucket = Bucket, Key = album_key, Body = album_content)
        
    #code for once data is transformed and inserted to transformed_data, then moving the data from to_be_processed/ to the _processed/ folder
    #for this we can not directly move, instead we will copy data in _processed folder and delete it from to_be_processed folder
    s3_resource = boto3.resource('s3')   #we can use boto3 resource like we do for client if we want to perform simple CRUD operations ...
    for key in spotify_keys:
        copy_source_dict = {'Bucket':Bucket,'Key':key}#copying data from all files of to_be_processed/ folder
        s3_resource.meta.client.copy(copy_source_dict,Bucket,'raw_data/processed/'+ key.split('/')[-1])#copying the data from copy_source_dict to the _processed/ folder and in same Bucket
        s3_resource.Object(Bucket,key).delete()#deleting data from original source ( from to_be_processed/ folder)
    
    
    
   

    
    
    
    
    
