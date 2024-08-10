import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

# Function to connect to the YouTube API
def api_connect():
    api_key = "AIzaSyC-JTDlKubvoWCHkzPy2l0wvO36X54teeA"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

# Function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()
    data = {}
    for item in response['items']:
        data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_Id': item['id'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Views': item['statistics']['viewCount'],
            'Total_Videos': item['statistics']['videoCount'],
            'Channel_Description': item['snippet']['description']
        }
    return data

# Function to get video IDs from a channel
def get_video_ids(youtube, channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response_1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response_1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response_1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# Function to get video information
def get_video_info(youtube, video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet'].get('description'),
                'Published_Date': item['snippet']['publishedAt'],
                'Views': item['statistics'].get('viewCount'),
                'Likes': item['statistics'].get('likeCount'),
                'Comments': item['statistics'].get('commentCount')
            }
            video_data.append(data)
    return video_data

# Function to get comment information
def get_comment_info(youtube, video_ids):
    comment_data = []
    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        )
        response = request.execute()
        for item in response['items']:
            data = {
                'Comment_Id': item['snippet']['topLevelComment']['id'],
                'Video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment_Published': item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comment_data.append(data)
    return comment_data

# Function to insert channel details into MongoDB
def insert_channel_details(channel_id):
    youtube = api_connect()
    ch_details = get_channel_info(youtube, channel_id)
    vi_ids = get_video_ids(youtube, channel_id)
    vi_details = get_video_info(youtube, vi_ids)
    com_details = get_comment_info(youtube, vi_ids)
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["youtube_project"]
    coll = db["channel_details"]
    coll.insert_one({
        "channel_information": ch_details,
        "video_information": vi_details,
        "comment_information": com_details
    })
    return "Upload completed"

# Function to create and populate the channels table in PostgreSQL
def channels_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    cursor = mydb.cursor()
    drop_query = 'DROP TABLE IF EXISTS channels'
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels(
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100) PRIMARY KEY,
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print("Error:", e)
    ch_list = []
    db = client["youtube_project"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df_1 = pd.DataFrame(ch_list)
    for index, row in df_1.iterrows():
        insert_query = '''INSERT INTO channels(
            Channel_Name,
            Channel_Id,
            Subscribers,
            Views,
            Total_Videos,
            Channel_Description
        ) VALUES (%s, %s, %s, %s, %s, %s)'''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscribers'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("Error:", e)
    cursor.close()
    mydb.close()

# Function to create and populate the videos table in PostgreSQL
def videos_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    cursor = mydb.cursor()
    drop_query = 'DROP TABLE IF EXISTS videos'
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos(
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(100) PRIMARY KEY,
            Title VARCHAR(255),
            Thumbnail TEXT,
            Description TEXT,
            Published_Date TIMESTAMP,
            Views BIGINT,
            Likes BIGINT,
            Comments BIGINT
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print("Error:", e)
    vi_list = []
    db = client["youtube_project"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({}, {"_id": 0, "video_information": 1}):
        vi_list.extend(vi_data["video_information"])
    df_2 = pd.DataFrame(vi_list)
    for index, row in df_2.iterrows():
        insert_query = '''INSERT INTO videos(
            Channel_Name,
            Channel_Id,
            Video_Id,
            Title,
            Thumbnail,
            Description,
            Published_Date,
            Views,
            Likes,
            Comments
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Views'],
            row['Likes'],
            row['Comments']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("Error:", e)
    cursor.close()
    mydb.close()

# Function to create and populate the comments table in PostgreSQL
def comments_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    cursor = mydb.cursor()
    drop_query = 'DROP TABLE IF EXISTS comments'
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments(
            Comment_Id VARCHAR(100) PRIMARY KEY,
            Video_Id VARCHAR(100),
            Comment_Text TEXT,
            Comment_Author VARCHAR(255),
            Comment_Published TIMESTAMP
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print("Error:", e)
    com_list = []
    db = client["youtube_project"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({}, {"_id": 0, "comment_information": 1}):
        com_list.extend(com_data["comment_information"])
    df_3 = pd.DataFrame(com_list)
    for index, row in df_3.iterrows():
        insert_query = '''INSERT INTO comments(
            Comment_Id,
            Video_Id,
            Comment_Text,
            Comment_Author,
            Comment_Published
        ) VALUES (%s, %s, %s, %s, %s)'''
        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("Error:", e)
    cursor.close()
    mydb.close()
    
    
    
def playlists_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    cursor = mydb.cursor()
    drop_query = 'DROP TABLE IF EXISTS playlists'
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS playlists(
            Playlist_Id VARCHAR(100) PRIMARY KEY,
            Title VARCHAR(255),
            Channel_Id VARCHAR(100),
            Channel_Name VARCHAR(255),
            PublishedAt TIMESTAMP,
            Video_Count INT
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print("Error:", e)
    pl_list = []
    db = client["youtube_project"]
    coll2 = db["channel_details"]
    for pl_data in coll2.find({}, {"_id": 0, "playlist_information": 1}):
        pl_list.extend(pl_data["playlist_information"])
    df_2 = pd.DataFrame(pl_list)
    for index, row in df_2.iterrows():
        insert_query = '''INSERT INTO playlists(
            Playlist_Id,
            Title,
            Channel_Id,
            Channel_Name,
            PublishedAt,
            Video_Count
        ) VALUES (%s, %s, %s, %s, %s, %s)'''
        values = (
            row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['PublishedAt'],
            row['Video_Count']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("Error:", e)
    cursor.close()
    mydb.close()
    

# Function to create tables
def tables():
    channels_table()
    videos_table()
    comments_table()
    playlists_table()
    return "Tables are created"

# Function to display channels table
def show_channels_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    query = "SELECT * FROM channels"
    df = pd.read_sql(query, mydb)
    st.write(df)
    mydb.close()

# Function to display videos table
def show_videos_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    query = "SELECT * FROM videos"
    df = pd.read_sql(query, mydb)
    st.write(df)
    mydb.close()

# Function to display comments table
def show_comments_table():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    query = "SELECT * FROM comments"
    df = pd.read_sql(query, mydb)
    st.write(df)
    mydb.close()
    
def show_playlists_tables():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    query = "SELECT * FROM playlists"
    df = pd.read_sql(query, mydb)
    st.write(df)
    mydb.close()    


# Streamlit application
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id = st.text_input("Enter the channel ID")

if st.button("Collect and store data"):
    ch_ids = []
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["youtube_project"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel details of the given channel ID already exist")
    else:
        insert = insert_channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    tables_created = tables()
    st.success(tables_created)

show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "VIDEOS", "COMMENTS","PLAYLISTS"))

if show_table == "CHANNELS":
    show_channels_table()
elif show_table == "VIDEOS":
    show_videos_table()
elif show_table == "COMMENTS":
    show_comments_table()
elif show_table == "PLAYLISTS":
    show_playlists_tables()

# SQL Query
question = st.selectbox("Select your question", [
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Channels that have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments on each video, and what are their corresponding video names?",
    "5. Videos with the highest number of likes, and their corresponding channel names?",
    "6. Total number of likes and dislikes for each video and their corresponding video names?",
    "7. Views for each channel, and what are their corresponding channel names?",
    "8. Channels that have published videos in the year 2022?",
    "9. Average duration of all videos in each channel?",
    "10. Videos with the highest number of comments?"
])



# Function to perform SQL queries based on user's selection
def perform_sql_query(question):
    mydb = psycopg2.connect(host="localhost", user="postgres", password="Mounish007@", database="youtube_project", port="5432")
    cursor = mydb.cursor()
    if question == "1. What are the names of all the videos and their corresponding channels?":
        query = "SELECT title AS video_title, channel_name FROM videos"
    elif question == "2. Channels that have the most number of videos, and how many videos do they have?":
        query = "SELECT channel_name, total_videos FROM channels ORDER BY total_videos DESC"
    elif question == "3. What are the top 10 most viewed videos and their respective channels?":
        query = "SELECT views, channel_name, title AS video_title FROM videos WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10"
    elif question == "4. How many comments on each video, and what are their corresponding video names?":
        query = "SELECT comments AS no_comments, title AS video_title FROM videos WHERE comments IS NOT NULL"
    elif question == "5. Videos with the highest number of likes, and their corresponding channel names?":
        query = "SELECT title AS video_title, channel_name, likes AS like_count FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC"
    elif question == "6. Total number of likes and dislikes for each video and their corresponding video names?":
        query = "SELECT likes, title AS video_title FROM videos"
    elif question == "7. Views for each channel, and what are their corresponding channel names?":
        query = "SELECT views, channel_name FROM channels"
    elif question == "8. Channels that have published videos in the year 2022?":
        query = "SELECT title AS video_title, published_date AS video_release, channel_name FROM videos WHERE EXTRACT(year FROM published_date) = 2022"
    elif question == "9. Average duration of all videos in each channel?":
        query = "SELECT channel_name, AVG(duration) AS average_duration FROM videos GROUP BY channel_name"
    elif question == "10. Videos with the highest number of comments?":
        query = "SELECT title AS video_title, channel_name, comments FROM videos WHERE comments IS NOT NULL ORDER BY comments DESC"
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    mydb.close()
    return result




if st.button("Execute SQL Query"):
    result = perform_sql_query(question)
    st.write(pd.DataFrame(result))

