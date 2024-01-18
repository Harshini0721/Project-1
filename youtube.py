from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connection

def Api_connect():
    Api_Id="AIzaSyCvjyns7Gz5sgOlxYuSqCepNjkqGYSWlCk"
    
    api_service_name="youtube"
    api_version="v3" 

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


#Get channel details

def get_channel_info(channel_id):
        request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=channel_id
        )
        response = request.execute()

        for i in response['items']:
         data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'], 
                Views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
        return data


#Get video ids

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:

        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId= Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range (len(response1['items'])):
         video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
         break
    return video_ids


#Get video details
def get_video_info(video_Ids):
    video_data=[]
    for video_id in video_Ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                      Video_Id=item['id'],
                      Video_title=item['snippet']['title'],
                      Description=item['snippet'].get('description'),
                      publish_date=item['snippet']['publishedAt'],
                      Tags=item['snippet'].get('tags'),
                      Thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Duration=item['contentDetails']['duration'],
                      Caption_status=item['contentDetails']['caption'],
                      Views=item['statistics'].get('viewCount'),
                      Likes=item['statistics'].get('likeCount'),
                      Comments=item['statistics'].get('commentCount')
                      )
            video_data.append(data)
    return video_data


#get comment details

def get_comment_details(video_Ids):
    Comment_details=[]
    try:
       for video_id in video_Ids:
        request=youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50)
        response=request.execute()

        for item in response['items']:
           data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                     Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                     Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                     Comment_PublishesAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
    
           Comment_details.append(data)
        
    except:
        pass
    return Comment_details 


#Get playlist details

def get_playlist_details(channelId):

        next_page_token=None
        Playlist_data=[]
        while True:
                request=youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channelId,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Playlist_Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                Video_Count=item['contentDetails']['itemCount'] )
                        Playlist_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                  break
        return Playlist_data


#Inset Data in Mongo db

connection=pymongo.MongoClient("mongodb://localhost:27017/")
database=connection["Youtube_data_harvesting"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids) 
    com_details=get_comment_details(vi_ids)

    collection=database["channel_details"]
    collection.insert_one({"channel_information":ch_details,"playlist_details": pl_details,
                           "video_details": vi_details,"comment_details":com_details})
    
    
    
    return "upload completed successfully"


#TABLE CREATION FOR CHANNELS,PLAYLISTS,VIDEOS,COMMENTS
def channel_table():
    mydab=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="harsh",
                        database="youtube_data_harvesting",
                        port="5432")
    cursor=mydab.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydab.commit()

    
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydab.commit()


    channel_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for cha_data in collection.find({},{"_id":0,"channel_information":1}):
        channel_list.append(cha_data["channel_information"])
    df=pd.DataFrame(channel_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id ,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        cursor.execute(insert_query,values)
        mydab.commit()


    
def playlist_table():
    mydab=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="harsh",
                        database="youtube_data_harvesting",
                        port="5432")
    cursor=mydab.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydab.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Playlist_Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        Video_Count int
                                                        )'''


    cursor.execute(create_query)
    mydab.commit()

    playlist_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for pla_data in collection.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pla_data["playlist_details"])):
          playlist_list.append(pla_data["playlist_details"][i])
    df1=pd.DataFrame(playlist_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                              Playlist_Title,
                                              Channel_Id,
                                              Channel_Name,
                                              Video_Count
                                              )

                                         
                                            values(%s,%s,%s,%s,%s)'''
        
        values=(row['Playlist_Id'],
                row['Playlist_Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Video_Count']
                )
        

        cursor.execute(insert_query,values)
        mydab.commit()


def videos_table():
    mydab=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="harsh",
                        database="youtube_data_harvesting",
                        port="5432")
    cursor=mydab.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydab.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Video_title varchar(150),
                                                    Description text,
                                                    publish_date timestamp,
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Duration interval,
                                                    Caption_status varchar(50),
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int
                                                    )'''

    cursor.execute(create_query)
    mydab.commit()

    video_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for vid_data in collection.find({},{"_id":0,"video_details":1}):
        for i in range(len(vid_data["video_details"])):
            video_list.append(vid_data["video_details"][i])
    df2=pd.DataFrame(video_list)

    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Video_title,
                                                    Description,
                                                    publish_date,
                                                    Tags,
                                                    Thumbnail,
                                                    Duration,
                                                    Caption_status,
                                                    Views,
                                                    Likes,
                                                    Comments
                                                    )

                                            
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Video_title'],
                    row['Description'],
                    row['publish_date'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Duration'],
                    row['Caption_status'],
                    row['Views'],
                    row['Likes'],
                    row['Comments']
                    )
        

            cursor.execute(insert_query,values)
            mydab.commit()



def comments_table():
    mydab=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="harsh",
                            database="youtube_data_harvesting",
                            port="5432")
    cursor=mydab.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydab.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),                 
                                                        Comment_PublishesAt timestamp
                                                        )'''

    cursor.execute(create_query)
    mydab.commit()

    comment_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for comm_data in collection.find({},{"_id":0,"comment_details":1}):
        for i in range(len(comm_data["comment_details"])):
            comment_list.append(comm_data["comment_details"][i])
    df3=pd.DataFrame(comment_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_PublishesAt
                                                )

                                            
                                                values(%s,%s,%s,%s)'''
            
            values=(row['Comment_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishesAt']
                    )
            

            cursor.execute(insert_query,values)
            mydab.commit()

def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return"Tables created successfully"

def view_channels_table():
    channel_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for cha_data in collection.find({},{"_id":0,"channel_information":1}):
        channel_list.append(cha_data["channel_information"])
    df=st.dataframe(channel_list)

    return df

def view_playlists_details():
    playlist_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for pla_data in collection.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pla_data["playlist_details"])):
            playlist_list.append(pla_data["playlist_details"][i])
    df1=st.dataframe(playlist_list)

    return df1

def view_videos_details():
    video_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for vid_data in collection.find({},{"_id":0,"video_details":1}):
        for i in range(len(vid_data["video_details"])):
            video_list.append(vid_data["video_details"][i])
    df2=st.dataframe(video_list)

    return df2

def view_comments_details():
    comment_list=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for comm_data in collection.find({},{"_id":0,"comment_details":1}):
        for i in range(len(comm_data["comment_details"])):
            comment_list.append(comm_data["comment_details"][i])
    df3=st.dataframe(comment_list)

    return df3


#STREAMLIT

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill takeaways")
    st.caption("Python scripting")
    st.caption("API Integration")
    st.caption("Data collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL") 

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    database=connection["Youtube_data_harvesting"]
    collection=database["channel_details"]
    for cha_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(cha_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success(" channel information of the given channel id already exists")
    
    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

view_table=st.radio("SELECT THE TABLE FOR VIEW",("Channels","Playlists","Videos","Comments"))

if view_table=="Channels":
    view_channels_table()
elif view_table=="Playlists":
    view_playlists_details()
elif view_table=="Videos":
    view_videos_details()
elif view_table=="Comments":
    view_comments_details()

#SQL CONNECTION

mydab=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="harsh",
                        database="youtube_data_harvesting",
                        port="5432")
cursor=mydab.cursor()

questions=st.selectbox("Select your Question",("1.What are the names of all the videos and their corresponding channels?",
                                               "2.Which channels have the most number of videos, and how many videos do they have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.How many comments were made on each video, and what are their corresponding video names?",
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year 2022?",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if questions=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select Video_title as videos,Channel_Name as channelname from videos''' 
    cursor.execute(query1)
    mydab.commit()
    tab1=cursor.fetchall()
    df1=pd.DataFrame(tab1,columns=["videos","channelname"])
    st.write(df1)

elif questions=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select Channel_Name as channelname,Total_Videos as no_videos from channels
                order by Total_Videos desc''' 
    cursor.execute(query2)
    mydab.commit()
    tab2=cursor.fetchall()
    df2=pd.DataFrame(tab2,columns=["channelname","no_videos"])
    st.write(df2)

elif questions=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select Views as views,Channel_Name as channelname,Video_title as videos from videos
                where views is not null order by views desc limit 10''' 
    cursor.execute(query3)
    mydab.commit()
    tab3=cursor.fetchall()
    df3=pd.DataFrame(tab3,columns=["views","channelname","videos"])
    st.write(df3)

elif questions== "4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select Comments as No_of_comments,Video_title as videos from videos where comments is not null''' 
    cursor.execute(query4)
    mydab.commit()
    tab4=cursor.fetchall()
    df4=pd.DataFrame(tab4,columns=["No_of_comments","videos"])
    st.write(df4)

elif questions== "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select Channel_Name as channelname,Video_title as videos,Likes as likes from videos 
                where likes is not null order by Likes desc''' 
    cursor.execute(query5)
    mydab.commit()
    tab5=cursor.fetchall()
    df5=pd.DataFrame(tab5,columns=["channelname","videos","likes "])
    st.write(df5)

elif questions=="6.What is the total number of likes for each video, and what are their corresponding video names?":
    query6='''select Likes as likes,Video_title as videos from videos''' 
    cursor.execute(query6)
    mydab.commit()
    tab6=cursor.fetchall()
    df6=pd.DataFrame(tab6,columns=["likes","videos"])
    st.write(df6)

elif questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select Channel_Name as channelname,Views as views from channels''' 
    cursor.execute(query7)
    mydab.commit()
    tab7=cursor.fetchall()
    df7=pd.DataFrame(tab7,columns=["channelname","views"])
    st.write(df7)

elif questions=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select Video_title as videos,publish_date as videorelease,Channel_Name as channelname from videos
                where extract(year from publish_date)=2022''' 
    cursor.execute(query8)
    mydab.commit()
    tab8=cursor.fetchall()
    df8=pd.DataFrame(tab8,columns=["videos","videorelease","channelname"])
    st.write(df8)

elif questions=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select Channel_Name as channelname ,AVG(duration) as averageduration from videos group by Channel_name''' 
    cursor.execute(query9)
    mydab.commit()
    tab9=cursor.fetchall()
    df9=pd.DataFrame(tab9,columns=["channelname","averageduration"])

    Tab9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        Tab9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df=pd.DataFrame(Tab9)
    st.write(df)

elif questions== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select Video_title as videos,Channel_Name as channelname,Comments as comments from videos
                    where Comments is not null order by Comments desc''' 
    cursor.execute(query10)
    mydab.commit()
    tab10=cursor.fetchall()
    df10=pd.DataFrame(tab10,columns=["videos","channelname","comments"])
    st.write(df10)