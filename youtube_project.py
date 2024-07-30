# required libraries and modules 
import streamlit as st
import pandas as pd
import mysql.connector
from googleapiclient.discovery import build
from datetime import datetime 
import re
import time

# Building required Connections with MySQL Database & Youtube API 
mydb = mysql.connector.connect( host = "localhost" , username = "root" , password = "muni@mysql")
mycursor = mydb.cursor()
mycursor.execute("use youtubedb")

api_key = "AIzaSyDXPI2Bn2yzDzZNuaXbGpTYQ48knms-TUo"
youtube = build('youtube' , 'v3' , developerKey = api_key )


def channel_details(channel_id):
    channel_data = []
    video_id = []
    all_video_details = []
    # Fetch channel details
    request = youtube.channels().list(
        part ="snippet,contentDetails,statistics",
        id = channel_id )
    response = request.execute()
    
    channel_details = dict( channel_id = response['items'][0]['id'] ,
                                chennal_name = response['items'][0]['snippet']['title'],
                                playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                                channel_video = response['items'][0]['statistics'].get('videoCount'),
                                channel_view = response['items'][0]['statistics'].get('viewCount'),
                                thumb_nail = response['items'][0]['snippet']["thumbnails"]["medium"]["url"],
                                subscribers = response['items'][0]["statistics"].get("subscriberCount"))
    channel_data.append(channel_details)

    mycursor.execute('''INSERT INTO channel_details (channel_id, channel_name, 
						playlist_id, channel_video, channel_view, thumb_nail, subscribers)
						VALUES (%s, %s, %s, %s, %s, %s, %s)''', tuple(channel_data[0].values()))
	
    mydb.commit()
    
    
    # Fetch video IDs using playlist ID
    request = youtube.playlistItems().list(
        part ='contentDetails',
        playlistId = channel_data[0]['playlist_id'],
        maxResults = 50)
    response1 = request.execute()
    
    video_id.extend(item['contentDetails']['videoId'] for item in response1['items'])
    
    # Handle pagination
    while 'nextPageToken' in response1:
        request = youtube.playlistItems().list(
            part ='contentDetails',
            playlistId = channel_data[0]['playlist_id'],
            maxResults = 50,
            pageToken = response1['nextPageToken'])
        response1 = request.execute()
        
        video_id.extend(item['contentDetails']['videoId'] for item in response1['items'])


    # Fetch video details using video IDs
    for i in range(0, len(video_id) , 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_id[i:i+50]) )
        response2 = request.execute()

        for video in response2['items']:
            # converting string to a datetime datatype
            date_time = video['snippet']['publishedAt']
            date_str = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
            f_d = date_str.strftime('%Y-%m-%d %H:%M:%S')
            
            # converting string into time type 
            duration_str = video['contentDetails']['duration']
            pattern = r'PT(\d+)S'
            pattern1 = r'PT(\d+)M(\d+)S'
            pattern2 = r'PT(\d+)H(\d+)M(\d+)S'

            match = re.match(pattern, duration_str)
            match1 = re.match(pattern1, duration_str)
            match2 = re.match(pattern2, duration_str)
            
            if match:  # Finding a active one
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

    for i in range(len(all_video_details)):
        mycursor.execute('''INSERT INTO video_details(video_id,publish_date,channel_id,
                                        video_name,duration,view_count,
                                        like_count,comment_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                                        tuple(all_video_details[i].values()))
        mydb.commit()

    return channel_data, video_id, all_video_details

# Function to get a basic details about a channel
def channel_basic():
	
    mycursor.execute("SELECT channel_name FROM channel_details")
    ch_name = mycursor.fetchall()
    channel_names = [name[0] for name in ch_name]

    selected_channel = st.selectbox('Select Channel', channel_names)

    if selected_channel:
        mycursor.execute('''SELECT thumb_nail, channel_name, playlist_id, channel_video,
                            channel_view, subscribers FROM channel_details WHERE channel_name = %s''', (selected_channel,))
        channel_detls = mycursor.fetchone()

        if channel_detls:
            thumb_nail, chnl_name, plylst_id, chnl_vido, chnl_view, subscribers = channel_detls
            st.image(f'{thumb_nail}')
            st.markdown(f'***Channel Name :*** {chnl_name}')
            st.markdown(f'***Playlist ID :*** {plylst_id}')
            st.markdown(f'***Number of Videos :*** {chnl_vido}')
            st.markdown(f'***Total Views :*** {chnl_view}')
            st.markdown(f'***Subscribers :*** {subscribers}')
        else:
            st.error("No details found for the selected channel.")


# Getting user input from streamlit and triggering our pre defined channel_details() function to collect, clean and store data
def feed_datas():
    # Input for Channel ID
    chenl_id = st.text_input("Enter Channel ID")

    if chenl_id:
        try:
            # Check if the Channel ID exists in the database
            mycursor.execute("SELECT channel_id FROM channel_details WHERE channel_id = %s", (chenl_id,))
            result = mycursor.fetchone()

            if result:
                st.success("This Channel Already Exists, Please Enter Another Channel ID.")
            else:
                # Execute predefined function if the Channel ID does not exist
                with st.spinner('Processing...'):
                    channel_details(chenl_id)
                st.success("Done!")
				
        except Exception as e:
            st.error(f"An error occurred: {e}, Please enter a valid channel ID")


# Defining multiple functions to answer Questions
def channels():
    mycursor.execute('''SELECT channel_name, channel_id FROM channel_details''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
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


# Showcasing question's to the users 
def question_compile():
	question = st.selectbox("Questions" , ["01.What are the names of all the videos and their corresponding channels?",
	                                     "02.Which channels have the most number of videos ,  and how many videos do they have?",
	                                     "03.What are the top 10 most viewed videos and their respective channels?",
	                                     "04.How many comments were made on each video , and what are their corresponding video name?",
	                                     "05.Which videos have the highest number of likes , and what are their corresponding channel names?",
	                                     "06.What is the total number of likes and dislikes for each video , and what are their corresponding video names?",
	                                     "07.What is the total number of views for each channel , and what are their corresponding channel names?",
	                                     "08.What are the names of all channels that have published videos in the year of 2022?",
	                                     "09.What is the average duration of all videos in each channel , and what are their corresponding channel name?",
	                                     "10.Which video have highest number of comments , and what are their corresponding channel names?"])
	
	
	    
	if question == "01.What are the names of all the videos and their corresponding channels?":
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


# Streamlit Setup

# Set up the main page layout and title
st.set_page_config(page_title="YouTube Project")

# Sidebar for navigation
st.sidebar.title('Youtube Project')
st.sidebar.subheader(':green-background[Choose a page]')
selected_page = st.sidebar.radio("Pages", ['Home', 'Channel_Detail', 'Add_Channel', "Questions"])

if selected_page == "Home":
    st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]')
    st.subheader(':violet[Process of the Project]')
    st.write('1. Data Collection')
    st.write('2. Data Cleaning')
    st.write('3. Insert Data to SQL Database')
    st.write('4. Converting to SQL Table')
    st.write('5. Converting into DataFrame')

    # Button to display channel list
    if st.button("Stream Channel List"):
        st.write(channels())

elif selected_page == 'Channel_Detail':
	st.subheader(":violet[Channel Details]")
	st.markdown(":blue-background[_Basic Details About a Channel_]")
	channel_basic()
	
elif selected_page == 'Add_Channel':
	st.subheader(":violet[Add Channel]")
	st.markdown(":blue-background[_Data Harvesting & Warehousing_]")
	feed_datas()
	
elif selected_page == "Questions":
	st.subheader(":violet[Question Compilation]")
	st.markdown(":blue-background[_Select any Question_]")
	question_compile()
	

# __over__
