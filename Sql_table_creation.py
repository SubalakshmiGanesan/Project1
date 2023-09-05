import sqlite3 
from datetime import datetime
import pandas as pd
connection = sqlite3.connect("Youtube_Data.db")
curs = connection.cursor()

# query to create the channel table 
query_chan = """CREATE TABLE Channel_Info(Channel_ID PRIMARY KEY NOT NULL,
Channel_Name TEXT NOT NULL,
Channel_Description Text,
Published_At  TIMESTAMP,
Playlist_ID TEXT,
Subscribers_Count INT,
Total_Videos INT,
View_Count INT)"""
curs.execute(query_chan)

# query to create the playlist table
query_play = """CREATE TABLE Playlist_Info(Channel_ID PRIMARY KEY NOT NULL,
Playlist_ID TEXT,
Playlist_Name TEXT)"""
curs.execute(query_play)

# query to create the video information table
query_video = """CREATE TABLE Video_Information(Video_Id TEXT NOT NULL,
Playlist_ID TEXT,
Video_Name TEXT,
Video_Description TEXT,
View_Count INT,
Like_Count INT,
Dislike_Count INT,
Comment_Count INT,
Duration INT,
Published_At TIMESTAMP)"""
curs.execute(query_video)

# query to create the comment information table
query_comment = """CREATE TABLE Comment_Info(Video_Id Text NOT NULL,
Playlist_Id TEXT,
Comment_Id TEXT,
Comment_Text TEXT,
Comment_Author TEXT,
Comment_Published_At TIMESTAMP)"""
curs.execute(query_comment)
