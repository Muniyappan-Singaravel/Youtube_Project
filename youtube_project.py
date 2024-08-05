# required libraries and modules 
import streamlit as st
import pandas as pd
import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime 
import re


# Creating Connections object with MySQL Database 
mydb = mysql.connector.connect( host = "localhost" , username = "root" , password = "muni@mysql")
mycursor = mydb.cursor()
mycursor.execute("use youtubedb")

# creating resourse object for interacting with an API
api_key = "AIzaSyDXPI2Bn2yzDzZNuaXbGpTYQ48knms-TUo"
youtube = build('youtube' , 'v3' , developerKey = api_key )


# function to collect data from youtube using API
def channel_details(channel_id):
    
    global channel_data, video_id, comment_data, video_data
    channel_data = []
    video_id = []
    comment_data = []
    video_data = []

    # getting channel datas from youtube through API request using channel ID
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id)
    response = request.execute()

    channel_details = dict(channel_name = response["items"][0]["snippet"]["title"],
                           channel_id = response["items"][0]["id"],
                           subscription_count = response["items"][0]["statistics"].get("subscriberCount"),
                           channel_views = response["items"][0]["statistics"].get("viewCount"),
                           channel_description = response["items"][0]["snippet"]["description"],
                           playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                           channel_video = response["items"][0]["statistics"].get("videoCount"),
                           thumbnail = response["items"][0]["snippet"]["thumbnails"]["medium"]["url"])
    channel_data.append(channel_details)


    # getting video ids from youtube through API request using playlist ID
    request1 = youtube.playlistItems().list(
           part = "contentDetails",
           playlistId = channel_data[0]["playlist_id"],
           maxResults = 50 )
    response1 = request1.execute()

    for i in response1["items"]:
        video_id.append(i["contentDetails"]["videoId"])

    while "nextPageToken" in response1:
        request1 = youtube.playlistItems().list(
               part = "contentDetails",
               playlistId = channel_data[0]["playlist_id"],
               maxResults = 50,
               pageToken = response1["nextPageToken"] )
        response1 = request1.execute()
    
        for i in response1["items"]:
            video_id.append(i["contentDetails"]["videoId"])


    # collecting comment datas from youtube through API request using video ID
    try:
        for video_ids in video_id:
            request2 = youtube.commentThreads().list(
                       part ="snippet,replies",
                       videoId = video_ids,
                       maxResults = 100 )
            response2 = request2.execute()

            for cmt in response2['items']:
                published_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt']
                date_str = datetime.fromisoformat(published_date)
                format_date = date_str.strftime('%Y-%m-%d %H:%M:%S')

                comment_info = dict(
                    comment_id = cmt['snippet']['topLevelComment']['id'],
                    comment_video_id = cmt['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    publish_date = format_date)
                comment_data.append(comment_info)
    # Log the error for debugging purposes
    except HttpError as e:
        if e.resp.status == 403:
            print(f"Comments are diabled for this video :{video_id}.")
        else:
            print(f"An error occured while fetching comments {video_id}:{e}")

    # collecting video datas from youtube through API request using video ID
    for v_id in video_id:
        request3 = youtube.videos().list( 
                  part = "snippet,contentDetails,statistics",
                  id = v_id )
        response3 = request3.execute()

        for video in response3["items"]:
            # converting string to a datetime datatype
            date_time = video["snippet"]["publishedAt"]
            date_str = datetime.fromisoformat(date_time)
            f_d = date_str.strftime('%Y-%m-%d %H:%M:%S')
            
            # converting string into time type 
            duration_str = video["contentDetails"]["duration"]
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
            
            video_info = dict(video_id = video["id"],
                              channel_id = video["snippet"]["channelId"],
                              video_name = video["snippet"]["title"],
                              video_description = video["snippet"]["description"],
                              publish_data = f_d,
                              view_count = video["statistics"].get("viewCount"),
                              like_count = video["statistics"].get("likeCount"),
                              favorite_count = video["statistics"].get("favoriteCount"),
                              comment_count = video["statistics"].get("commentCount"),
                              duration = dura_tion,
                              thumbnail = video["snippet"]["thumbnails"]["default"]["url"],
                              caption_status = video["contentDetails"].get("caption"))
            video_data.append(video_info)
    
    
    return channel_data, video_id, comment_data, video_data

# function to transfer all collected datas to a database
def inject_datas():
    # injecting channel datas to channel table
    for channel in channel_data:
        mycursor.execute('''INSERT INTO channel_details (channel_name, 
                            channel_id, subscribers, channel_views, channel_description, 
                            playlist_id, channel_video, thumbnail) VALUES 
                            (%s, %s, %s, %s, %s, %s, %s, %s)''', tuple(channel.values()))
    
    mydb.commit()

    # injecting video datas to video table 
    for video in video_data:
        mycursor.execute('''INSERT INTO video_details (video_id, channel_id, video_name, 
                            video_description, publish_date, view_count, like_count, 
                            favorite_count, comment_count, duration, thumbnail, caption_status) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', tuple(video.values()))
    mydb.commit()
    
    # injecting comment datas to comment table
    for comment in comment_data:
        mycursor.execute('''INSERT INTO comment_details (comment_id, 
                            comment_video_id, comment_text, comment_author, publish_date) 
                            VALUES (%s, %s, %s, %s, %s)''', tuple(comment.values()))

    mydb.commit()


# Function to get a basic details about a channel
def channel_basic():
    mycursor.execute("SELECT channel_name FROM channel_details")
    ch_name = mycursor.fetchall()
    channel_names = [name[0] for name in ch_name]

    selected_channel = st.selectbox('Select Channel :', channel_names)

    if selected_channel:
        mycursor.execute('''SELECT thumbnail, channel_name, playlist_id, channel_video,
                            channel_views, subscribers, channel_description FROM channel_details WHERE channel_name = %s''', (selected_channel,))
        channel_detls = mycursor.fetchone()

        if channel_detls:
            thumbnail, chnl_name, plylst_id, chnl_vido, chnl_view, subscribers, channel_description = channel_detls
            st.image(f'{thumbnail}')
            st.markdown(f'***Channel Name :*** {chnl_name}')
            st.markdown(f'***Playlist ID :*** {plylst_id}')
            st.markdown(f'***Number of Videos :*** {chnl_vido}')
            st.markdown(f'***Total Views :*** {chnl_view}')
            st.markdown(f'***Subscribers :*** {subscribers}')
            st.write(f'Description : {channel_description}')
        else:
            st.error("No details found for the selected channel.")



# Getting user input from streamlit and triggering our pre defined channel_details() function to collect, clean and store data
def feed_datas():
    # Input for Channel ID
    chenl_id = st.text_input("Enter Channel ID :")

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
                st.success("Data collected successfully!")
				
        except Exception as e:
            st.error(f"An error occurred: {e} not found, Please enter a valid channel ID")


# Defining multiple functions to answer Questions
def channels():
    mycursor.execute('''SELECT channel_name, channel_id FROM channel_details''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  # Adjusting the index to start from 1
    return df

def question1():
    mycursor.execute('''SELECT channel_name, video_name 
                        FROM channel_details 
                        JOIN video_details ON video_details.channel_id = channel_details.channel_id''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question2():
    mycursor.execute('''SELECT channel_name, channel_video 
                        FROM channel_details
                        ORDER BY channel_video DESC LIMIT 5''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question3():
    mycursor.execute('''SELECT channel_name, video_name, view_count 
                        FROM video_details 
                        JOIN channel_details ON channel_details.channel_id = video_details.channel_id
                        ORDER BY view_count DESC LIMIT 10''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question4():
    mycursor.execute('''SELECT cd.channel_name, vd.video_name, vd.comment_count 
                        FROM video_details vd LEFT JOIN channel_details cd ON vd.channel_id = cd.channel_id''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question5():
    mycursor.execute('''SELECT channel_name, video_name, like_count 
                        FROM video_details 
                        JOIN channel_details ON channel_details.channel_id = video_details.channel_id
                        ORDER BY like_count DESC LIMIT 10''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question6():
    mycursor.execute('''SELECT cd.channel_name, vd.video_name, vd.like_count FROM video_details vd
                        LEFT JOIN channel_details cd ON vd.channel_id = cd.channel_id''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question7():
    mycursor.execute('''SELECT channel_name, channel_views 
                        FROM channel_details ORDER BY channel_views DESC''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question8():
    mycursor.execute('''SELECT cd.channel_name, MIN(vd.publish_date) AS first_published_at 
                        FROM channel_details cd JOIN video_details vd 
                        ON cd.channel_id = vd.channel_id
                        WHERE publish_date LIKE '2022%' GROUP BY cd.channel_name''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question9():
    mycursor.execute('''SELECT channel_name, SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) AS avg_duration 
                        FROM video_details 
                        JOIN channel_details ON channel_details.channel_id = video_details.channel_id
                        GROUP BY video_details.channel_id''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question10():
    mycursor.execute('''SELECT channel_name, video_name, comment_count 
                        FROM video_details 
                        JOIN channel_details ON channel_details.channel_id = video_details.channel_id
                        ORDER BY comment_count DESC LIMIT 10''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df

def question11():
    mycursor.execute('''SELECT cd.channel_name, vd.video_name, COUNT(cd2.comment_text) AS comment_text_count 
                        FROM channel_details cd 
                        JOIN video_details vd ON cd.channel_id = vd.channel_id 
                        RIGHT JOIN comment_details cd2 ON vd.video_id = cd2.comment_video_id 
                        GROUP BY cd.channel_name, vd.video_name 
                        ORDER BY comment_text_count DESC''')
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    df.index = df.index + 1  
    return df


# Showcasing questions to the users 
def question_compile():
	question = st.selectbox("Questions :" , ["01.What are the names of all the videos and their corresponding channels?",
	                                     "02.Which channels have the most number of videos, and how many videos do they have?",
	                                     "03.What are the top 10 most viewed videos and their respective channels?",
	                                     "04.How many comments were made on each video, and what are their corresponding video name?",
	                                     "05.Which videos have the highest number of likes, and what are their corresponding channel names?",
	                                     "06.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
	                                     "07.What is the total number of views for each channel, and what are their corresponding channel names?",
	                                     "08.What are the names of all channels that have published videos in the year of 2022?",
	                                     "09.What is the average duration of all videos in each channel, and what are their corresponding channel name?",
	                                     "10.Which video have highest number of comments, and what are their corresponding channel names?",
										 "11.How many comment text were collected for each video, and what are their corresponding video name?"])
	
	
	    
	if question == "01.What are the names of all the videos and their corresponding channels?":
		st.dataframe(question1())
	
	elif question == "02.Which channels have the most number of videos, and how many videos do they have?":
	    st.dataframe(question2())
	
	elif question == "03.What are the top 10 most viewed videos and their respective channels?":
		st.dataframe(question3())
	
	elif question == "04.How many comments were made on each video, and what are their corresponding video name?":
	    st.dataframe(question4())
	
	elif question == "05.Which videos have the highest number of likes, and what are their corresponding channel names?":
	    st.dataframe(question5())
	
	elif question == "06.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
	    st.dataframe(question6())
	
	elif question == "07.What is the total number of views for each channel, and what are their corresponding channel names?":
	    st.dataframe(question7())
	
	elif question == "08.What are the names of all channels that have published videos in the year of 2022?":
	    st.dataframe(question8())
	
	elif question == "09.What is the average duration of all videos in each channel, and what are their corresponding channel name?":
	    st.dataframe(question9())
	
	elif question == "10.Which video have highest number of comments, and what are their corresponding channel names?":
	    st.dataframe(question10())

	elif question == "11.How many comment text were collected for each video, and what are their corresponding video name?":
		st.dataframe(question11())

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
    st.write('1. Data Collection From Youtube Server')
    st.write('2. The API and the Channel ID is used to retrieve channel details')
    st.write('3. Data Transfer to SQL Data Warehouse')
    st.write('4. Converting to SQL Table & DataFrame')

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

	st.subheader(":blue[Transfer Datas to DataBase] ")
	st.write("click the below button to transfer datas to the database!")
	if st.button("Inject Datas"):
		with st.spinner('Processing...'):
			inject_datas()
			st.success("Data Transferred Successfully!!")
		
elif selected_page == "Questions":
	st.subheader(":violet[Question Compilation]")
	st.markdown(":blue-background[_Select any Question_]")
	question_compile()
	

# __over__

