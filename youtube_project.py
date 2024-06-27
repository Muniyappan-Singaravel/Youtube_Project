# required libraries and modules 
import streamlit as st
import pandas as pd
import mysql.connector
from googleapiclient.discovery import build
from datetime import datetime 
import re

# Building required Connections with MySQL Database & Youtube API 
mydb = mysql.connector.connect( host = "localhost" , username = "root" , password = "muni@mysql")
mycursor = mydb.cursor()
mycursor.execute("use youtubedb")

api_key = "AIzaSyDXPI2Bn2yzDzZNuaXbGpTYQ48knms-TUo"
youtube = build('youtube' , 'v3' , developerKey = api_key )


# Streamlit Page Setup 
st.set_page_config(page_title = "Muni's Project")

st.header("Youtube Data Harvesting & Warehousing")


# User Interface setup for Channel Details
st.subheader("Channel Details")

# Function to Display the channel details as per the user requirement
mycursor.execute("select channel_name from channel_details")
ch_name = mycursor.fetchall()
channel_names = [name[0] for name in ch_name]

selected_channel = st.selectbox('Select Channel', channel_names)

if selected_channel:
    mycursor.execute('''SELECT thumb_nail,channel_name, playlist_id, channel_video,
                        channel_view,subscribers FROM channel_details WHERE channel_name = %s''',(selected_channel,))
    channel_detls = mycursor.fetchone()

    if channel_detls:
        thumb_nail,chnl_name,plylst_id,chnl_vido,chnl_view,subscribers = channel_detls
        st.image(f'{thumb_nail}')
        st.markdown(f'_Channel Name : {chnl_name}_')
        st.markdown(f'_Playlist ID : {plylst_id}_')
        st.markdown(f'_Channel Video : {chnl_vido}_')
        st.markdown(f'_Channel View : {chnl_view}_')
        st.markdown(f'_Subscribers : {subscribers}_')


# Function for Extracting Data from Youtube API and Cleaning then Inserting it to the SQL database which is created already
st.subheader("Add Channel's")
st.markdown("_Data Harvesting & Warehousing_")


def channel_details(chnl_id):
    channel_data = []
    
    # id = ','.join(chnl_ids) [while inserting multiple channel ids]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id = chnl_id )
    response = request.execute()
    
    
    for i in range(len(response['items'])):
        channel_details = dict( channel_id = response['items'][i]['id'] ,
                                chennal_name = response['items'][i]['snippet']['title'],
                                playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                                channel_video = response['items'][i]['statistics']['videoCount'],
                                channel_view = response['items'][i]['statistics']['viewCount'],
                                thumb_nail = response['items'][i]['snippet']["thumbnails"]["medium"]["url"],
                                subscribers = response['items'][i]["statistics"]["subscriberCount"])
        channel_data.append(channel_details)

    # getting playlist id in a seperate list
    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']

    # Inserting channel_details to the sql table using DDL & TCL commands
    for i in range(len(channel_data)):
        mycursor.execute('''INSERT INTO channel_details(channel_id,channel_name,playlist_id,
                                        channel_video, channel_view,thumb_nail,subscribers)
                                        VALUES (%s,%s,%s,%s,%s,%s,%s)''',tuple(channel_data[i].values()))
        mydb.commit()

    # Function to get all video ids using playlist id
#def video_ids(palaylist_id):
    request = youtube. playlistItems().list(
                part = 'contentDetails',
                playlistId = Playlist_id , 
                maxResults = 50 )
    response1 = request.execute()
    
    video_id = []
    vido_id = video_id
    for i in range(len(response1['items'])):
        vdo_id = response1['items'][i]['contentDetails']['videoId']
        video_id.append(vdo_id)

    next_page_token = response1.get('nextPageToken')
    more_pages = True

    while more_pages:
        if  next_page_token is None: # condition to break the while loop
            more_pages = False
        else:
            request = youtube. playlistItems().list(
                part = 'contentDetails',
                playlistId = Playlist_id , 
                maxResults = 50 ,
                pageToken = next_page_token )
            response1 = request.execute()

            for i in range(len(response1['items'])):
                vdo_id = response1['items'][i]['contentDetails']['videoId']
                video_id.append(vdo_id)
            
            next_page_token = response1.get('nextPageToken')

    vido_id = video_id
    
    
    
    # function to get video details using video_id
# def video_details(vido_id):
    all_video_details = []
    for i in range(0, len(vido_id) , 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(vido_id[i:i+50]) )
        response2 = request.execute()

        for video in response2['items']:
            # converting string type date to a datetime datatype
            date_time = video['snippet']['publishedAt']
            date_str = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
            f_d = date_str.strftime('%Y-%m-%d %H:%M:%S')
            
            # converting string type time into normal time type using RE
            duration_str = video['contentDetails']['duration']
            pattern = r'PT(\d+)S'
            pattern1 = r'PT(\d+)M(\d+)S'
            pattern2 = r'PT(\d+)H(\d+)M(\d+)S'

            match = re.match(pattern, duration_str)
            match1 = re.match(pattern1, duration_str)
            match2 = re.match(pattern2, duration_str)
            
            if match:  # There are three patterns so, using if statement to find a active one
                seconds = int(match.group(1))
                dura_tion = '{:02}:{:02}:{:02}'.format(0,0,seconds)

            elif match1:
                minutes = int(match1.group(1))
                seconds = int(match1.group(2))
                dura_tion = '{:02}:{:02}:{:02}'.format(0, minutes, seconds)
                
            elif match2:
                hours = int(match2.group(1))
                minutes = int(match2.group(2))
                seconds = int(match2.group(3))
                dura_tion = '{:02}:{:02}:{:02}'.format(hours,minutes,seconds)
            
            vido_dtl = dict(video_id = video['id'],
                           publish_date = f_d,
                           channel_id = video['snippet']['channelId'],
                           video_name = video['snippet']['title'],
                           duration = dura_tion,
                           view_count = video['statistics']['viewCount'],
                           like_count = video['statistics'].get('likeCount'),
                           comment_count = video['statistics'].get('commentCount'))
            
            all_video_details.append(vido_dtl)

    # Inserting video_details to the sql table using DDL & TCL commands
    for i in range(len(all_video_details)):
        mycursor.execute('''INSERT INTO video_details(video_id,publish_date,channel_id,
                                        video_name,duration,view_count,
                                        like_count,comment_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                                        tuple(all_video_details[i].values()))
        mydb.commit()
    
    return channel_details,Playlist_id,video_id,all_video_details


# Getting user input from streamlit and triggering our pre defined channel_details() function to collect, clean and store data
chenl_id = st.text_input("Enter any Channel ID")
chnl_id = chenl_id

if chenl_id:
    channel_details(chnl_id)
    st.markdown("_Data Harvesting & Warehousing Done Successfully_")



# Defining multiple functions to answer our queries with the help of sql DML commands and pandas
def channels():
    mycursor.execute('''select channel_name,channel_id from channel_details''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question1():
    mycursor.execute('''select channel_name , video_name from channel_details join video_details
                                              on video_details.channel_id=channel_details.channel_id ''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question2():
    mycursor.execute('''select channel_name , channel_video from channel_details
                                              order by channel_video desc limit 5''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question3():
    mycursor.execute('''select channel_name, video_name, view_count from video_details join
                                             channel_details on channel_details.channel_id = video_details.channel_id
                                             order by view_count desc limit 10''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question4():
    mycursor.execute('''select video_name, comment_count from video_details''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question5():
    mycursor.execute('''select channel_name, video_name , like_count from video_details join
                                             channel_details on channel_details.channel_id = video_details.channel_id
                                             order by like_count desc limit 10''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question6():
    mycursor.execute(''' select video_name , like_count from video_details ''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question7():
    mycursor.execute('''select channel_name, channel_view from channel_details''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question8():
    mycursor.execute('''SELECT channel_name, publish_date FROM channel_details JOIN video_details ON
                                             video_details.channel_id = channel_details.channel_id
                                             WHERE publish_date LIKE '2022%' ''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    return df

def question9():
    mycursor.execute('''select channel_name, sec_to_time(avg(time_to_sec(duration))) as
                                             avg_duration from video_details join channel_details on
                                             channel_details.channel_id = video_details.channel_id
                                             group by video_details.channel_id ''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df

def question10():
    mycursor.execute(''' select channel_name ,video_name, comment_count from
                                              video_details join channel_details on
                                              channel_details.channel_id = video_details.channel_id
                                              order by comment_count desc limit 10''')
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    return df


# Showcasing queries to the users , so that they can select the query which they wish to get answers
st.subheader("Question Compilation")
st.markdown("_Select any Query_")

question = st.selectbox("Queries" , ["Display the list of Channels",
                                     "01.What are the names of all the videos and their corresponding channels?",
                                     "02.Which channels have the most number of videos ,  and how many videos do they have?",
                                     "03.What are the top 10 most viewed videos and their respective channels?",
                                     "04.How many comments were made on each video , and what are their corresponding video name?",
                                     "05.Which videos have the highest number of likes , and what are their corresponding channel names?",
                                     "06.What is the total number of likes and dislikes for each video , and what are their corresponding video names?",
                                     "07.What is the total number of views for each channel , and what are their corresponding channel names?",
                                     "08.What are the names of all channels that have published videos in the year of 2022?",
                                     "09.What is the average duration of all videos in each channel , and what are their corresponding channel name?",
                                     "10.Which video have highest number of comments , and what are their corresponding channel names?"])


# Function to answer the users as per their question , Also we are using pre defined functions here to display the ans
if question == "Display the list of Channels":
    st.dataframe(channels())
    
elif question == "01.What are the names of all the videos and their corresponding channels?":
    st.dataframe(question1())

elif question == "02.Which channels have the most number of videos ,  and how many videos do they have?":
    st.dataframe(question2())

elif question == "03.What are the top 10 most viewed videos and their respective channels?":
    st.dataframe(question3())

elif question == "04.How many comments were made on each video , and what are their corresponding video name?":
    st.dataframe(question4())

elif question == "05.Which videos have the highest number of likes , and what are their corresponding channel names?":
    st.dataframe(question5())

elif question == "06.What is the total number of likes and dislikes for each video , and what are their corresponding video names?":
    st.dataframe(question6())

elif question == "07.What is the total number of views for each channel , and what are their corresponding channel names?":
    st.dataframe(question7())

elif question == "08.What are the names of all channels that have published videos in the year of 2022?":
    st.dataframe(question8())

elif question == "09.What is the average duration of all videos in each channel , and what are their corresponding channel name?":
    st.dataframe(question9())

elif question == "10.Which video have highest number of comments , and what are their corresponding channel names?":
    st.dataframe(question10())


       
# MySQL commands used for creating database and tables
# while keeping this commands active we can't rerun the program So...

# mycursor.execute('''create database youtubedb''')

# mycursor.execute('''create table channel_details (channel_id varchar(100),
#                                  primary key,chennal_name varchar(250),
#                                  playlist_id varchar(250),channel_video int,
#                                  channel_view int,thumb_nail varvhar(250),subscribers int)''')

# mycursor.execute('''create table video_details (video_id varchar(250),publish_date DATETIME,
#                                  channel_id varchar(100),FOREIGN KEY (channel_id) references channel_details(channel_id),
#                                  video_name varchar(100),duration TIME,view_count int,like_count int,comment_count int)''')

 
