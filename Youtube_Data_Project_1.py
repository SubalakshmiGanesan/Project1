import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import sqlite3 
from datetime import datetime
import pandas as pd

#api key
api_key = "AIzaSyDyaNP_oj5p69OmnpciubY6eAICcEf0eaw"
youtube = build('youtube','v3',developerKey=api_key)

#MongoDB connection
loc_client = MongoClient("mongodb://localhost:27017/")
db = loc_client['Pro_Sample']
tot = db['Total_info']


# creating a sql connection
connection = sqlite3.connect("Youtube_Data.db")
curs = connection.cursor()

#Streamlit page setup
st.set_page_config(page_title = "Project 1")
st.header("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
st.subheader("To get the details from Youtube API and stored in MongoDB")


#main function for getting data from youtube api
def main(ch):
    channel_info,playlist_info = get_channel_info(ch)
    videos = get_channel_videos(ch)
    #print(videos)
    video_information = video_info(videos)
    video_id_list = lis(video_information)
    tot_comments = get_comment_threads(ch,video_id_list)
   
    
    data = {'Channel_details':channel_info,
            "Playlist_details":playlist_info,
           "Video_details":video_information,
           "Comment_details":tot_comments}
    
    return data
  

# function of gettin channel information and playlist information
def get_channel_info(ch):
    response = youtube.channels().list(id = ch,
                                   part ='snippet,statistics,contentDetails' )
    channel_data =response.execute()
    channel_information = {
        "Channel_ID":channel_data['items'][0]['id'],
        'channel_name' :channel_data['items'][0]['snippet']['title'],
        'channel_description':channel_data['items'][0]['snippet']['description'],
        "Published_At":channel_data['items'][0]['snippet']['publishedAt'],
        'playlists' :channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'subscribers' :channel_data['items'][0]['statistics']['subscriberCount'],
        'Total_videos' :channel_data['items'][0]['statistics']['videoCount'],
        "View_count":channel_data['items'][0]['statistics']['viewCount']}
    
    playlist_information = {
        "Channel_ID":channel_data['items'][0]['id'],
        'Playlists_Id' :channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'Playlist_name' :channel_data['items'][0]['snippet']['title'] }
    
    return channel_information,playlist_information


# function of getting all the videos in the playlist
def get_channel_videos(channel_id):
    response = youtube.channels().list(id = ch,
                                   part ='snippet,statistics,contentDetails' )
    channel_data =response.execute()
    playlists =channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    playlist_videos = []
    next_page_token = None
    while 1:
        res = youtube.playlistItems().list(playlistId = playlists,
                                          part = 'snippet',maxResults = 100,
                                          pageToken = next_page_token).execute()
        
        playlist_videos += res['items']
        next_page_token = res.get('nextpageToken')
        
        if next_page_token is None:
            break
    return playlist_videos


# function of getting particular details from the video
def video_info(videos):
    total_videos = []
    for item in videos:
        video_id = item['snippet']['resourceId']['videoId']
        video_res = youtube.videos().list(id = video_id,
                                     part = 'snippet,statistics,contentDetails',
                                         maxResults = 100).execute()
    
        if video_res['items']:
            video_info = {
                "Video_Id" : video_id,
                "Video_Number":item['snippet']['position'],
                'Playlist_Id':item['snippet']['playlistId'],
                "Video_Name":video_res['items'][0]['snippet']['title']if 'title' in video_res['items'][0]['snippet']else "Not Available",
                "Video_desc":video_res['items'][0]['snippet']['description']if 'description' in video_res['items'][0]['snippet']else "Not Available",
                "View_Count":video_res['items'][0]['statistics']['viewCount']if 'viewCount' in video_res['items'][0]['statistics']else 0,
                "Like_Count":video_res['items'][0]['statistics']['likeCount']if 'likeCount' in video_res['items'][0]['statistics']else 0,
                "Dislike_Count":video_res['items'][0]['statistics']['dislikeCount']if 'dislikeCount' in video_res['items'][0]['statistics']else 0,
                "Comment_count" : video_res['items'][0]['statistics']['commentCount']if 'commentCount' in video_res['items'][0]['statistics']else 0,
                "Duration":video_res['items'][0]['contentDetails']['duration']if 'duration' in video_res['items'][0]['contentDetails']else 0,
                "Published_At":video_res['items'][0]['snippet']['publishedAt']}
            total_videos.append(video_info)
            
    return total_videos



# function of getting only video_id from the video information
def lis(video_information):
    id_list = []
    for i in video_information:
        video_id = i['Video_Id']
        comment_count = i['Comment_count']
        if int(comment_count)>0:
            id_list.append(video_id)

    return id_list


# function of getting comments for each video from the video id
def get_comment_threads(ch,video_id_list):
    response = youtube.channels().list(id = ch,
                                   part ='snippet,statistics,contentDetails' )
    channel_data =response.execute()
    list_of_com = []
    for i in video_id_list:
        req1 = youtube.commentThreads().list(part = "snippet,replies",
                                        videoId =i ,
                                        maxResults = 100)
        response1 = req1.execute()
        for item in response1['items']:
            comment_info ={
                    "Video_id":i,
                    'Playlists_Id' :channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    "Comment_Id" : item["snippet"]["topLevelComment"]['id']if 'id' in item['snippet']['topLevelComment']else "Not Available",
                    "Comment_Text":item['snippet']['topLevelComment']['snippet']['textDisplay']if 'textDisplay' in item['snippet']['topLevelComment']['snippet']else "Not Available",
                    "Comment_Author":item['snippet']['topLevelComment']['snippet']['authorDisplayName']if 'authorDisplayName' in item['snippet']['topLevelComment']['snippet']else "Not Available",
                    "Comment_publishedAt":item['snippet']['topLevelComment']['snippet']['publishedAt']if 'publishedAt' in item['snippet']['topLevelComment']['snippet']else "Not Available"
                }
            list_of_com.append(comment_info)
    
    return (list_of_com)



# creating a text box to input a channel id
ch = st.text_input('Enter Your Channel ID',key = 'text')
col1,col2 = st.columns(2)
def clear_txt():
    st.session_state['text'] = ""
with col1:
    clear = st.button('Clear',on_click = clear_txt)# to clear the text box
with col2:
    submit = st.button(label = 'Submit') #submit button used to put the channel details in mongodb


# creating a list of channel id's from mongodb, to check whether the channel detail is already updated in the mongodb
lis_id = []
for i in db.Total_info.find({},{"Channel_details.Channel_ID":1}):
        lis_id.append(i['Channel_details']['Channel_ID'])

# to insert the channel detail to the mongodb
if submit:
    try:
        if ch in lis_id:
            st.write("This Channel is already Inserted")
        else:
            Full_data = main(ch)
            st.write(Full_data)
            db.Total_info.insert_one(Full_data)
            st.write("This Channel details inserted successfully!")
    except:
        st.write("Invalid Channel ID")

# creating a list of channel name which is already inserted in the mongodb for selectbox in streamlit
new_lis1=[]
def new_lis():
    for i in db.Total_info.find({},{"Channel_details.channel_name":1}):
        new_lis1.append(i['Channel_details']['channel_name'])
    return new_lis1               



st.subheader("Migrate the Channel details from MongoDB to SQL table")
chan = st.selectbox('Select the Channel name',new_lis()) #selectbox is used to select the channel name
migrate = st.button(label = "Migrate") # migrate button is used to migrate the channel details to the sql table


#main function of sql table updation
def sql_update(chan):
    chan_input(chan)
    Play_Id = playlist_input(chan)
    #print(Play_Id)
    video_input(Play_Id)
    comment_input(Play_Id)

    
# function of inserting the channel details to the table
def chan_input(chan):
    query1 = {"$match":{"Channel_details.channel_name":chan}}
    query2 = {"$project":{"_id":0}}
    b = []
    for i in tot.aggregate([query1,query2]):
        b.append(i['Channel_details'])
    
    channel = pd.DataFrame(b)
    channel['Published_At']=pd.to_datetime(channel['Published_At'])
    con_ch = {'subscribers':int,
         'Total_videos':int,
         'View_count':int}
    channel =channel.astype(con_ch)
    channel.to_sql("Channel_Info",connection,if_exists='append')

    
# function of inserting playlist details to the playlist table
def playlist_input(chan):
    query1 = {"$match":{"Playlist_details.Playlist_name":chan}}
    query2 = {"$project":{"_id":0}}
    p = []
    for i in tot.aggregate([query1,query2]):
        p.append(i['Playlist_details'])
        
    play_id = p[0]['Playlists_Id']
    play = pd.DataFrame(p)
    play.to_sql("Playlist_Info",connection,if_exists='append')
    #play_id = "Playlist_details.Playlist_Id"
    return play_id


# function of inserting video details to the video_information table
def video_input(Play_Id):
    query1 = {"$match":{"Video_details.Playlist_Id":Play_Id}}
    query2 = {"$project":{"_id":0}}
    for i in tot.aggregate([query1,query2]):
        v = (i['Video_details'])
        
    video = pd.DataFrame(v)
    video['Published_At'] = pd.to_datetime(video['Published_At'])
    video['Duration'] = video['Duration'].apply(lambda x: pd.Timedelta(x).total_seconds())
    con_video ={'View_Count':int,
           'Like_Count':int,
           "Comment_count":int,
           "Duration":int}
    video = video.astype(con_video)
    video.to_sql("Video_Information",connection,if_exists='append')

    
# function of inserting comment details to the comment table
def comment_input(Play_Id):
    query1 = {"$match":{"Comment_details.Playlists_Id":Play_Id}}
    query2 = {"$project":{"_id":0}}
    for i in tot.aggregate([query1,query2]):
        c = (i['Comment_details'])
    comment = pd.DataFrame(c)
    comment['Comment_publishedAt']=pd.to_datetime(comment['Comment_publishedAt'])
    comment.to_sql("Comment_Info",connection,if_exists='append')

    
# to migrate the selected channel details from mongodb to sql
if migrate:
        que = """SELECT Channel_Name from Channel_Info"""
        data1 = curs.execute(que)
        df = pd.DataFrame(data1)
        obj = df.eq(chan).any(axis=1).sum()
        if obj == 1:
            st.write("Already Migrated")
        else:
            sql_update(chan)
            st.write("Data Migrated Successfully")


st.subheader("To solve the queries")
# 10 questions to describe as a table format from the sql
queries = ['1. What are the names of all the videos and their corresponding channels?','2. Which channels have the most number of videos, and how many videos do they have?','3. What are the top 10 most viewed videos and their respective channels?','4. How many comments were made on each video, and what are their corresponding video names?','5. Which videos have the highest number of likes, and what are their corresponding channel names?','6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?','7. What is the total number of views for each channel, and what are their corresponding channel names?','8. What are the names of all the channels that have published videos in the year 2022?','9. What is the average duration of all videos in each channel, and what are their corresponding channel names?','10. Which videos have the highest number of comments, and what are their corresponding channel names?']

select_query = st.selectbox("Please select the query",queries) #select box for selecting the query
table = st.button(label = "Show Table") # show table button is used to get the answer for the selected query


# function of getting answer for question 1
def query1():
    query1 = """SELECT Video_Information.Video_Name,Channel_Info.channel_name FROM Channel_Info
    INNER JOIN Video_Information ON Channel_Info.playlists=Video_Information.Playlist_Id"""

    curs.execute(query1)
    data=curs.fetchall()
    df = pd.DataFrame(data)
    df=df.rename(columns = {0:"Video_Name",1:"Channel_Name"})
    df

# function of getting answer for question 2
def query2():
    query2 = """SELECT channel_name,Total_videos from Channel_Info ORDER by Total_videos DESC LIMIT 1"""

    curs.execute(query2)
    data=curs.fetchall()
    df2 = pd.DataFrame(data)
    df2=df2.rename(columns = {0:"Channel_Name",1:"Number_of_Videos"})
    df2

    
# function of getting answer for question 3
def query3():
    query3 = """SELECT Channel_Info.channel_name,Video_Information.Video_Name,Video_Information.View_count FROM Channel_Info
    INNER JOIN Video_Information ON Channel_Info.playlists=Video_Information.Playlist_Id
    ORDER BY Video_Information.View_count DESC LIMIT 10"""

    curs.execute(query3)
    data=curs.fetchall()
    df3 = pd.DataFrame(data)
    df3=df3.rename(columns = {0:"Channel_Name",1:"Video_Name",2:"Total_Views"})
    df3

    
# function of getting answer for question 4
def query4():
    query4 = """SELECT Video_Name,Comment_count from Video_Information"""

    curs.execute(query4)
    data=curs.fetchall()
    df4 = pd.DataFrame(data)
    df4=df4.rename(columns = {0:"Video_Name",1:"Comment_Count"})
    df4

    
# function of getting answer for question 5
def query5():
    query5 = """SELECT Channel_Info.channel_name,Video_Information.Video_Name,max(video_Information.Like_Count) from Channel_Info
    INNER JOIN Video_Information on Channel_Info.playlists = Video_Information.Playlist_Id
    GROUP by Video_Information.Playlist_Id"""

    curs.execute(query5)
    data=curs.fetchall()
    df5 = pd.DataFrame(data)
    df5=df5.rename(columns = {0:"Channel_Name",1:"Video_Name",2:"Highest_Likes"})
    df5

# function of getting answer for question 6    
def query6():
    query6 = """SELECT Video_Name,Like_Count,Dislike_Count from Video_Information"""

    curs.execute(query6)
    data=curs.fetchall()
    df6 = pd.DataFrame(data)
    df6=df6.rename(columns = {0:"Video_Name",1:"Like_Count",2:"Dislike_Count"})
    df6

    
    
# function of getting answer for question 7
def query7():
    query7 = """SELECT channel_name,View_count from Channel_Info"""

    curs.execute(query7)
    data=curs.fetchall()
    df7 = pd.DataFrame(data)
    df7=df7.rename(columns = {0:"Channel_Name",1:"Total_Views"})
    df7

    
# function of getting answer for question 8
def query8():
    query8 = """SELECT Channel_Info.channel_name,Video_Information.Video_Name,Video_Information.Published_At from Channel_Info 
    INNER JOIN Video_Information on Channel_Info.playlists = Video_Information.Playlist_Id
    WHERE strftime('%Y',Video_Information.Published_At) = '2022'"""

    curs.execute(query8)
    data=curs.fetchall()
    df8 = pd.DataFrame(data)
    df8=df8.rename(columns = {0:"Channel_Name",1:"Video_Name",2:"Year"})
    df8

    
# function of getting answer for question 9
def query9():
    query9 ="""SELECT Channel_Info.channel_name,avg(Video_Information.Duration) from Channel_Info
    INNER join Video_Information on Channel_Info.playlists = Video_Information.Playlist_Id
    group by Video_Information.Playlist_Id"""

    curs.execute(query9)
    data = curs.fetchall()
    df9 = pd.DataFrame(data)
    df9 = df9.rename(columns = {0:'Channel_Name',1:"Average_Duration"})
    df9

# function of getting answer for question 10
def query10():
    query10 ="""SELECT Channel_Info.channel_name,Video_Information.Video_Name,max(video_Information.Comment_Count) from Channel_Info
    INNER JOIN Video_Information on Channel_Info.playlists = Video_Information.Playlist_Id
    GROUP by Video_Information.Playlist_Id"""

    curs.execute(query10)
    data = curs.fetchall()
    df10 = pd.DataFrame(data)
    df10 = df10.rename(columns = {0:'Channel_Name',1:"Video_Name",2:"Maximum_Comment_Count"})
    df10
    
    
# to get the answer for the selected query when we click the table button
if table:
    ind = queries.index(select_query)
    if ind == 0:
        query1()
    elif ind == 1:
        query2()
    elif ind == 2:
        query3()
    elif ind == 3:
        query4()
    elif ind == 4:
        query5()
    elif ind == 5:
        query6()
    elif ind == 6:
        query7()
    elif ind == 7:
        query8()
    elif ind == 8:
        query9()
    else:
        query10()
        
