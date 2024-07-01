# Import packages
from googleapiclient.discovery import build
import googleapiclient.discovery
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

channel_id=st.text_input("Please Enter the Channel ID on below")

# Api key connection
def Api_connect():
    Api_Id = "AIzaSyCZ6epKniZIHu1oiDFQHb7xqZzAFCbf7jg"
    
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=Api_Id)
    
    return youtube

youtube = Api_connect()


# Function to get Channel Information

def get_channel_info(channel_id):
    channel_data =[]
    request = youtube.channels().list(
        part="contentDetails,snippet,statistics",
        id=channel_id
        )
    response = request.execute()

    for i in response["items"]:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    Subscribers = i["statistics"]["subscriberCount"],
                    Views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_id = i['contentDetails']['relatedPlaylists']['uploads'],
                    )
        channel_data.append(data)
    return channel_data


# Function to get Video ID's

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, 
                                    part='contentDetails').execute()
    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(part='snippet',
                                            playlistId=Playlist_Id,  
                                            maxResults=50,
                                            pageToken = next_page_token
                                            ).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")
        
        if next_page_token is None:
            break
    return video_ids


# Function to get Video Information

def get_video_info(video_ids):  
    video_data =[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        
        for item in response["items"]:
            data = dict(Channel_Name=item["snippet"]["channelTitle"],
                        Channel_Id = item["snippet"]["channelTitle"],
                        Video_Id = item["id"],
                        Title = item["snippet"]["title"],
                        Tags = item["snippet"].get('tags'),
                        Thumbnail = item["snippet"]["thumbnails"]["default"]["url"],
                        Description = item["snippet"].get("description"),
                        Published_Date=item["snippet"]["publishedAt"],
                        Duration=item["contentDetails"]["duration"],
                        Views = item["statistics"].get("viewCount"),
                        Likes = item["statistics"].get('likeCount'),
                        Comments = item["statistics"].get('commentCount'),
                        Favorite_Count = item["statistics"]["favoriteCount"],
                        Definition = item["contentDetails"]["definition"],
                        Caption_Status = item["contentDetails"]["caption"]
                        )
            video_data.append(data)
    return video_data

         
# Function to get Comment Information

def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet",
                                                    videoId=video_id,
                                                    maxResults=50,
                                                    )
            response = request.execute()

            for item in response["items"]:
                data = dict(Comment_Id = item["snippet"]["topLevelComment"]["id"],
                            Video_Id = item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'],
                            )
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data


# Function to get Playlist Details

def get_playlist_details(channel_id):
    
    next_page_token = None
    All_data = []

    while True:
        request = youtube.playlists().list(
                part = "snippet,contentDetails",
                channelId = channel_id,
                maxResults = 50,
                pageToken = next_page_token
        )
        response = request.execute()

        for item in response["items"]:
                data = dict(Playlist_Id=item["id"],
                            Title = item["snippet"]["title"],
                            Channel_Id = item["snippet"]["channelId"],
                            Channel_Name = item["snippet"]["channelTitle"],
                            PublishedAt = item["snippet"]["publishedAt"],
                            Video_Count = item["contentDetails"]["itemCount"])
                All_data.append(data)
                
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
        
    return All_data
            
            
# MySQL Connection and Table creation for Channel Tables

def channels_table():
    mydb = mysql.connector.connect(host="localhost",
                                user="root",
                                password="",
                                database = "project"
    )
    cursor = mydb.cursor()

    try:
        create_query = '''CREATE TABLE if not exists channels(Channel_Name VARCHAR(100),
                                                            Channel_Id VARCHAR(100) PRIMARY KEY,
                                                            Subscribers BIGINT,
                                                            Views BIGINT,
                                                            Total_Videos INT,
                                                            Channel_Description TEXT,
                                                            Playlist_id VARCHAR(100))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels table already created")


    channel_info= get_channel_info(channel_id)   
    df = pd.DataFrame(channel_info)
    

    for index,row in df.iterrows():
        insert_query = '''INSERT INTO channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_id) 
                                                                                    
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row["Channel_Name"],
                row["Channel_Id"],
                row["Subscribers"],
                row["Views"],
                row["Total_Videos"],
                row["Channel_Description"],
                row["Playlist_id"])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
            
        except:
            print("Channels values are already inserted")  


# MySQL Connection and Table creation for Playlist Tables   

def playlist_table():
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database = "project"
  )
  cursor = mydb.cursor()

  create_query = '''CREATE TABLE if not exists playlist(Playlist_Id VARCHAR(100) primary key,
                                                        Title VARCHAR(100),
                                                        Channel_Id VARCHAR(100),
                                                        Channel_Name VARCHAR(100),
                                                        PublishedAt timestamp,
                                                        Video_Count INT
                                                      )'''
  cursor.execute(create_query)
  mydb.commit()
  
  playlist_info = get_playlist_details(channel_id)
  df1 = pd.DataFrame(playlist_info)
  
  def convert_datetime(datetime_str):
      return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

  for index,row in df1.iterrows():
          insert_query = '''INSERT INTO playlist(Playlist_Id,
                                                  Title,
                                                  Channel_Id,
                                                  Channel_Name,
                                                  PublishedAt,
                                                  Video_Count) 
                                                                                      
                                                  values(%s,%s,%s,%s,%s,%s)'''
          values = (row["Playlist_Id"],
                  row["Title"],
                  row["Channel_Id"],
                  row["Channel_Name"],
                  convert_datetime(row["PublishedAt"]),
                  row["Video_Count"]
                  )
      
          cursor.execute(insert_query,values)
          mydb.commit()
 
 
 # MySQL Connection and Table creation for Video Tables  
        
def videos_table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database = "project"
    )
    cursor = mydb.cursor()

    create_query = '''CREATE TABLE if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration time,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(100),
                                                        Caption_Status varchar(100)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    video_info= get_video_info(get_video_ids(channel_id))
    df2 = pd.DataFrame(video_info)
      
    def convert_duration(duration_str):
            if duration_str.startswith('P'):
                if duration_str == 'P0D':
                    return '00:00:00'  # If duration is 0 days, return 00:00:00
                duration_str = duration_str[1:]  # Remove the leading 'P'
                duration = timedelta()
                current = ''
                for char in duration_str:
                    if char.isdigit():
                        current += char
                    elif char in ['T', 'H', 'M', 'S']:
                        if current:
                            if char == 'T':
                                continue  # Skip the 'T' separator
                            elif char == 'H':
                                duration += timedelta(hours=int(current))
                            elif char == 'M':
                                duration += timedelta(minutes=int(current))
                            elif char == 'S':
                                duration += timedelta(seconds=int(current))
                        current = ''
                return str(duration)
            else:
                return duration_str

    def convert_datetime(datetime_str):
            return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

    for index,row in df2.iterrows():
            
            video_id = row["Video_Id"]
        
            # Check if the record with the same Video_Id exists
            cursor.execute("SELECT COUNT(*) FROM videos WHERE Video_Id = %s", (video_id,))
            result = cursor.fetchone()
            if result[0] > 0:
                    print(f"Record with Video_Id {video_id} already exists. Skipping insertion.")
                    continue
            
            
            tags = ','.join(row["Tags"]) if isinstance(row["Tags"], list) else row["Tags"]
            duration = str(row["Duration"]) if isinstance(row["Duration"], list) else row["Duration"]
            
            duration = convert_duration(duration)
            
            insert_query = '''INSERT INTO videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status
                                                        ) 
                                                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values = (row["Channel_Name"],
                    row["Channel_Id"],
                    video_id,
                    row["Title"],
                    tags,
                    row["Thumbnail"],
                    row["Description"],
                    convert_datetime(row["Published_Date"]),
                    duration,
                    row["Views"],
                    row["Likes"],
                    row["Comments"],
                    row["Favorite_Count"],
                    row["Definition"],
                    row["Caption_Status"],
                    )
        
            cursor.execute(insert_query,values)
            mydb.commit()


# MySQL Connection and Table creation for Comments Tables
        
def comments_table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database = "project"
    )
    cursor = mydb.cursor()

    create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_Id varchar(100),
                                                    Comment_Text text,
                                                    Comment_Author varchar(200),
                                                    Comment_Published timestamp
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    comments_info = get_comment_info(get_video_ids(channel_id))
    df3 = pd.DataFrame(comments_info)

    def convert_datetime(datetime_str):
            return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

    for index,row in df3.iterrows():
            insert_query = '''INSERT INTO comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                                    ) 
                                                                                    
                                                    values(%s,%s,%s,%s,%s)'''
            values = (row["Comment_Id"],
                    row["Video_Id"],
                    row["Comment_Text"],
                    row["Comment_Author"],
                    convert_datetime(row["Comment_Published"])
                    )

            cursor.execute(insert_query,values)
            mydb.commit()



# Calling the tables function for creating the tables

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Tables created Successfully"



# Using Streamlit part to view on web page

with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.caption(":green[TOOLS USED]")
    st.caption("1. Youtube API v3")
    st.caption("2. Python Scripting")
    st.caption("3. MySql")
    st.caption("4. streamlit")
    st.caption("5. Data Collections")
    st.caption("6. Data management by MySql")

#channel_id=st.text_input("Enter the Channel ID")

mydb = mysql.connector.connect(host="localhost",
                            user="root",
                            password="",
                            database = "project"
                            )
cursor = mydb.cursor()

    
if st.button(":green[RUN]"):   
    Tables = tables()
    st.success(Tables)

if st.button(":green[YOUTUBE LINK]"):
    link=("https://www.youtube.com/hashtag/youtubelink")
    link

if st.button(":green[AVAILABLE CHANNELS]"):
    cursor.execute("SELECT Channel_Name,Channel_Id FROM channels")
    channels = cursor.fetchall()
    st.dataframe(channels)


if st.button(":green[CHANNELS]"):
    cursor.execute("SELECT * FROM channels")
    ch_det=cursor.fetchall()
    st.dataframe(ch_det)

if st.button(":green[PLAYLIST]"):
    cursor.execute("SELECT * FROM playlist")
    pl_det=cursor.fetchall()
    st.dataframe(pl_det)

if st.button(":green[VIDEOS]"):
    cursor.execute("SELECT * FROM videos")
    vd_det=cursor.fetchall()
    st.dataframe(vd_det)

if st.button(":green[COMMENTS]"):
    cursor.execute("SELECT * FROM comments")
    cm_det=cursor.fetchall()
    st.dataframe(cm_det)




        
    
# Creating MySQL connection for the 10 question with viewing on the streamlit part 





question = st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their  corresponding channel names?",
                                                "6. What is the total number of likes for each video, and what are  their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                                                ))

if question == "1. What are the names of all the videos and their corresponding channels?":
    query1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns=["videos title","channel name"])
    st.write(df)
    
elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)
    st.bar_chart(df2.set_index("channel name"))
    
elif question == "3. What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=["views","channel name","video title"])
    st.write(df3)
    st.bar_chart(df3.set_index("video title"))

elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)
    st.bar_chart(df4.set_index("videotitle"))
    
elif question == "5. Which videos have the highest number of likes, and what are their  corresponding channel names?":
    query5 = '''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

 
elif question == "6. What is the total number of likes for each video, and what are  their corresponding video names?":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6) 
    st.line_chart(df6.set_index("videotitle"))
    
elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7,columns=["channel_name","total_views"])
    st.write(df7)
    st.bar_chart(df7.set_index("channel_name"))
    
elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8,columns=["video_title","published_date","channelname"])
    st.write(df8)

elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9,columns=["channel_name","average_duration"])
    #st.write(df9)
    
    T9 = []
    for index,row in df9.iterrows():
        channel_title = row["channel_name"]
        average_duration = row["average_duration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle = channel_title,avgduration = average_duration_str))
    df1=pd.DataFrame(T9)  
    st.write(df1)
   
elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is 
                not null order by comments desc'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10,columns=["video_title","channel_name","comments"])
    st.write(df10)
