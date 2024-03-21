import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import mysql.connector
import pymongo
import googleapiclient.discovery
import json
import re
from datetime import datetime
from pymongo import MongoClient

#google api

api_service_name = "youtube"
api_version = "v3"

api_key='AIzaSyCqIq8ZyhhoWmuGZyU53KwMvbA8TNcaO1c'

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#Mongodb
connection = MongoClient("mongodb+srv://thivakaranmnm:ac6vpoz38lUsgCqx@cluster0.u1ol918.mongodb.net/")
db=connection['Project']
col=db['Youtube_data']

#Mysql connection
mycon=mysql.connector.connect(host="localhost",user="root",password="Thiva@1999",database="youtubedb",auth_plugin='mysql_native_password',charset="utf8mb4")
mycursor=mycon.cursor()


def get_channel_details(channel_id):
    response = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails'
    )

    channel_data = response.execute()

    playlist_id=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    playlistitems_response = youtube.playlistItems().list(playlistId=playlist_id,part='contentDetails',maxResults=10).execute()
    playlist_videos =[item['contentDetails'] for item in playlistitems_response.get('items', [])]
    video_ids = playlist_videos

    channel_videos = get_video_details(video_ids, playlist_videos)
    
    for video_id in video_ids:
        
        
         channel_informations = {
             'channel_name' : channel_data['items'][0]['snippet']['title'],
             'channel_description' : channel_data['items'][0]['snippet']['description'],
             'playlists' : channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
             'Subscriber_Count' : channel_data['items'][0]['statistics']['subscriberCount'],
             'Channel_Views' : channel_data['items'][0]['statistics']['viewCount'],
             'Playlist_Id' : channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
             'Published_At': channel_data['items'][0]['snippet']['publishedAt'],
             'Videos': {f"Video_Id{i+1}": video for i, video in enumerate(channel_videos)}
           }
    return channel_informations

def get_video_details(video_ids, playlist_videos):
    videos = []
    video_ids = [item['videoId'] for item in playlist_videos]
    for video_id in video_ids:
    #video_id = ['videoId']
        video_response = youtube.videos().list(part='snippet,statistics,contentDetails', id=video_id, maxResults=10).execute()
        
        video_comments = get_video_comments(video_id)
    
        if video_response['items']:
            video_information = {
                "Video_Id": video_id,
                "Video_Name": video_response['items'][0]['snippet']['title'],
                "Video_Description": video_response['items'][0]['snippet']['description'], 
                "Like_Count": video_response['items'][0]['statistics']['likeCount'] if 'title' in video_response['items'][0]['statistics'] else "Not Available",
                "Duration": video_response['items'][0]['contentDetails']['duration'],
                "View_Count": video_response['items'][0]['statistics']['viewCount'],
                "Comment_Count":video_response['items'][0]['statistics']['commentCount'],
                "Comments" : {f"Comment_Id{i+1}": comment for i, comment in enumerate(video_comments)}
             }
        if 'likeCount' in video_response['items'][0]['statistics']:
                video_information["Like_Count"] = video_response['items'][0]['statistics']['likeCount']
        else:
            video_information["Like_Count"] = 0

        videos.append(video_information)
    return videos

def get_video_comments(video_id):
    comments = []
    comments_response = youtube.commentThreads().list(part='snippet',  videoId = video_id, maxResults=10).execute()
    for comment in comments_response['items']:
        comment_information = {
                "Comment_Id": comment['snippet']['topLevelComment']['id'],
                "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt'] }
        comments.append(comment_information)
    return comments

def update_db(channel_detail_n):
    def duration_to_time(duration_str):
        pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
        match = pattern.match(duration_str)

        hours = int(match.group(1)[:-1]) if match.group(1) else 0
        minutes = int(match.group(2)[:-1]) if match.group(2) else 0
        seconds = int(match.group(3)[:-1]) if match.group(3) else 0

        return '{:02}:{:02}:{:02}'.format(hours, minutes, seconds)
     
    channel_data = channel_detail_n

    sql_insert = """
             INSERT INTO Channelstable (channel_name, channel_description, playlists, subscriber_count, channel_views, playlist_id, published_at)
             VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

    # Extract channel data
    channel_name = channel_data['channel_name']
    channel_description = channel_data['channel_description']
    playlists = channel_data['playlists']
    subscriber_count = channel_data['Subscriber_Count']
    channel_views = channel_data['Channel_Views']
    playlist_id = channel_data['Playlist_Id']
    published_at = channel_data['Published_At']

    # Insert into ChannelsTable
    mycursor.execute(sql_insert, (channel_name, channel_description, playlists, subscriber_count, channel_views, playlist_id, published_at))
    mycon.commit()

    # Extract video details
    videos_data = channel_data['Videos']
    for video_id, video_data in videos_data.items():
        sql_insert_video = """
                          INSERT INTO Videostable (video_id, channel_name, video_name, video_description, like_count, duration, view_count, comment_count)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                          """
        duration_time = duration_to_time(video_data['Duration'])
        # Extract video data
        video_name = video_data['Video_Name']
        video_description = video_data['Video_Description']
        like_count = video_data['Like_Count']
        duration = duration_time
        view_count = video_data['View_Count']
        comment_count = video_data['Comment_Count']
    
   
        mycursor.execute(sql_insert_video, (video_id, channel_name, video_name, video_description, like_count, duration, view_count, comment_count))
        mycon.commit()
    
    
        comments_data = video_data['Comments']
        for comment_id, comment_data in comments_data.items():
            sql_insert_comment = """
                    INSERT INTO Commentstable (channel_name, video_name, comment_id, comment_text, comment_author, comment_published_at) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
       
            comment_text = comment_data['Comment_Text']
            comment_author = comment_data['Comment_Author']
            comment_published_at = datetime.strptime(comment_data['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        
        
            mycursor.execute(sql_insert_comment, (channel_name, video_name, comment_id, comment_text, comment_author, comment_published_at))
            mycon.commit()
    
    res = "Channel Details Updated..."
            
    return res

#Dataframes
all_channel_details = []

for doc in col.find():
    try:
        channel_info = {
            'channel_name': doc['data']['channel_name'],
            'channel_description': doc['data']['channel_description'],
            'playlists': doc['data']['playlists'],
            'subscriber_count': doc['data']['Subscriber_Count'],
            'channel_views': doc['data']['Channel_Views'],
            'playlist_id': doc['data']['Playlist_Id'],
            'published_at': doc['data']['Published_At']
        }
        all_channel_details.append(channel_info)
    except Exception as e:
        print(f"Error processing document {doc['_id']}: {e}")

channel_df = pd.DataFrame(all_channel_details)

all_video_details = []

for doc in col.find():
    try:
        videos_data = doc['data'].get('Videos', {})
        
        for video_id, video_details in videos_data.items():
            video_info = {
                'video_id': video_details.get('Video_Id', ''),
                'channel_name': doc['data']['channel_name'],
                'video_name': video_details.get('Video_Name', ''),
                'video_description': video_details.get('Video_Description', ''),
                'like_count': int(video_details.get('Like_Count', 0)),
                'duration': video_details.get('Duration', ''),
                'view_count': int(video_details.get('View_Count', 0)),
                'comment_count': int(video_details.get('Comment_Count', 0))
            }
            all_video_details.append(video_info)
    except Exception as e:
        print(f"Error processing document {doc['_id']}: {e}")

video_df = pd.DataFrame(all_video_details)

all_comments = []

for doc in col.find():
    # Check if 'data' exists in the document
    if 'data' in doc:
        # Check if 'Videos' exists in the 'data' field
        videos = doc['data'].get('Videos', {})
        
        for video_id, video_details in videos.items():
            # Check if 'Comments' exists in the 'video_details'
            if 'Comments' in video_details:
                comments = video_details['Comments']
            
                for comment_id, comment_details in comments.items():
                    comment = {
                        'channel_name': doc['data'].get('channel_name', ''),
                        'video_name': video_details.get('Video_Name', ''),
                        'comment_id': comment_details.get('Comment_Id', ''),
                        'comment_text': comment_details.get('Comment_Text', ''),
                        'comment_author': comment_details.get('Comment_Author', ''),
                        'comment_published_at': comment_details.get('Comment_PublishedAt', '')
                    }
                    all_comments.append(comment)
    else:
        print(f"No 'data' field found in document {doc['_id']}")

comments_df = pd.DataFrame(all_comments)

#SQL queries
queries = {
    "Names of all videos and their corresponding channels": "SELECT video_name, channel_name FROM Videostable",
    "Channels with the most number of videos and their count": "SELECT channel_name, COUNT(*) AS video_count FROM Videostable GROUP BY channel_name ORDER BY video_count DESC",
    "Top 10 most viewed videos and their respective channels": "SELECT video_name, channel_name, view_count FROM Videostable ORDER BY view_count DESC LIMIT 10",
    "Number of comments on each video and their corresponding names": "SELECT video_name, COUNT(*) AS comment_count FROM Commentstable GROUP BY video_name",
    "Videos with the highest number of likes and their corresponding channel names": "SELECT video_name, channel_name, MAX(like_count) AS max_likes FROM Videostable GROUP BY video_name, channel_name ORDER BY max_likes DESC LIMIT 1",
    "Total number of likes and dislikes for each video and their corresponding names": "SELECT video_name, SUM(like_count) AS total_likes FROM Videostable GROUP BY video_name",
    "Total number of views for each channel and their corresponding names": "SELECT channel_name, SUM(view_count) AS total_views FROM Videostable GROUP BY channel_name",
    "Channels that have published videos in 2022": "SELECT DISTINCT channel_name, published_at FROM Channelstable WHERE YEAR(published_at) = 2022",
    "Average duration of all videos in each channel": "SELECT channel_name, AVG(duration) AS average_duration_in_seconds FROM Videostable GROUP BY channel_name",
    "Videos with the highest number of comments and their corresponding channel names": "SELECT video_name, channel_name, comment_count AS max_comments FROM Videostable ORDER BY max_comments DESC"
}

# Streamlit 

#st.markdown("<h1 style='color: red;'> YouTube </h1><h2 style='color: black;'>Data Harvesting and Warehousing </h2>", unsafe_allow_html=True)
st.sidebar.markdown('<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/YouTube_icon_%282013-2017%29.png/240px-YouTube_icon_%282013-2017%29.png" alt="YouTube Icon" style="width: 100px;"><br><br>', unsafe_allow_html=True)


st.title("YouTube Data Harvesting and Warehousing")

with st.sidebar:
    
    select = option_menu("Main Menu",["HOME", "TABLES", "QUERY"])
    
if select == "HOME":
    vid_id = st.text_input("Enter YouTube Channel Id")
    if vid_id:
        channel_detail_n=get_channel_details(vid_id)
        st.write(channel_detail_n)
        col.insert_one({"data":channel_detail_n})
        update=update_db(channel_detail_n)
        st.warning(update)
        
elif select == "TABLES":
    
    tab1, tab2, tab3 = st.tabs(["Channels","Videos","Comments"])
    
    with tab1:
        st.write(channel_df)
    with tab2:
        st.write(video_df)
    with tab3:
        st.write(comments_df)
    
elif select =="QUERY":
    selected_query = st.selectbox("Select a query:", list(queries.keys()))

    if selected_query:
        query_text = queries[selected_query]
        mycursor.execute(query_text)
        results = mycursor.fetchall()
        if results:
            df = pd.DataFrame(results, columns=[desc[0] for desc in mycursor.description])
            st.dataframe(df)
        else:
            st.write("No results found for this query.")

      


