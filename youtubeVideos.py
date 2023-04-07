import os
import psycopg2 as pg
from googleapiclient.discovery import build

# CONSUME YOUTUBE API
def get_videoDetails():
    api_key = os.environ.get("YT_API_KEY")
    service = build("youtube", "v3", developerKey=api_key)
    search_params = {
        "part":"snippet",
        "channelId":"UC8butISFwT-Wl7EV0hUK0BQ",
        "type":"video",
        "order":"date"
    }
    # get all video ids
    video_ids = []
    while True:
        search_req = service.search().list(**search_params)
        search_resp = search_req.execute()
        video_ids.extend([id["id"]["videoId"] for id in search_resp["items"]])
        try:
             next_pg_token = search_resp["nextPageToken"]
             search_params["pageToken"] = next_pg_token
        except KeyError:
             break
    #get video stats in batches
    for i in range(0,len(video_ids),50):
        video_req = service.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids[i:i+50]),
            fields="items(id,snippet(publishedAt,title),contentDetails(duration),statistics(viewCount,likeCount,favoriteCount,commentCount))"
        )
        video_resp = video_req.execute()
        return video_resp

# SAVE DATA TO DATALAKE
def load_datalake():
    conn = pg.connect(
        host="localhost",
        database="db",
        user="user",
        password="password"
    )
    cur=conn.cursor()
    # create videos table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Videos (
            VideoID VARCHAR(255),
            Title VARCHAR(255),
            PublishDate DATE,
            Length VARCHAR(255),
            Views int,
            Likes int,
            Favorites int,
            Comments int);
    """)
    video_details=get_videoDetails()
    for video in video_details["items"]:
            video_id = video["id"]
            title = video["snippet"]["title"]
            length = video["contentDetails"]["duration"]
            published_at = video["snippet"]["publishedAt"]
            views = video["statistics"]["viewCount"]
            likes = video["statistics"]["likeCount"]
            favorites = video["statistics"]["favoriteCount"]
            comments = video["statistics"]["commentCount"]
            # populate videos table
            query="INSERT into Videos (VideoID,Title,Length,PublishDate,Views,Likes,Favorites,Comments) values (%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(query, (video_id, title, length, published_at, views, likes, favorites, comments))
    conn.commit()
    cur.close()
    conn.close()

