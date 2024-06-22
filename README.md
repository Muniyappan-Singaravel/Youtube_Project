# reserved_wolf
Youtube data Harvesting and Warehousing using Python, MySQL, Streamlit and google API

# Youtube Data Harvesting and Warehousing

## Problem Statement;

        
The task is to build a Streamlit application using Python, MySQL ans Pandas for the users. The app should gives access to the users to analyze the data from multiple Youtube channels  like their channel details and video details. Also user need an access to add channel by giving channel ID as a input. It should provide access to migrate the selected channel data from SQL database, inclusive of advanced functions like joining tables to get a detailed information.
## Technologies Used


1. Python

2. MySQL

3. Googel API client

4. Pandas
## Approach
 

1. Need to generate our own API key to build a connection with Youtube API v3 with the help of Python google API client library to harvest data from Youtube API. 

2. Collected data must be stord somewhere, then only we can retrive it when user needs it. For storing and retriving purpose we need to build a connection with MySQL database using MySQL.connector library.

3. Using sql DML commands join the tables within tha database to get the specific channel details based on user input. For this, tables which created previously must be created with proper constraints as primary key and foreign key.

4. Setting up streamlit application using Python streamlit library. Using this we can provide a simple and easy user interface to the users. So that they can enter channel ID and view channel details, also they can select the channel to migrate the data from database.

5. There are some pre defined queries to answer that write SQL commands through Python to retrive data from database. Retrived data is displayed in streamlit application.

## API Reference
  httphttps://console.cloud.google.com/apis/library?project=ivory-ego-425011-g4
  
  API key used in this project : AIzaSyDXPI2Bn2yzDzZNuaXbGpTYQ48knms-TUo


## Demo

Linkedin link can find a demo video 



https://www.linkedin.com/posts/muniyappan-singaravel-23b277314_data-science-project-youtube-data-harvesting-activity-7210199560118272001-1gCB?utm_source=share&utm_medium=member_desktop










