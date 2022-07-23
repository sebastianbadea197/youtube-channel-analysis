import pandas as pd

def get_channel_stats (youtube, channel_ids):
    """
    Get Channel Stats

    Args:
        youtube (object): build object of Youtube API
        channel_ids (list): list of channel IDs
        
    Returns:
        pd.DataFrame: all the stats for each channel
    """
    all_data = []
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response = request.execute()
    
    #  loop through all the analysed videos' JSON files
    for item in response['items']:
        data = {
            'channelName': item['snippet']['title'],
            'subscribers': item['statistics']['subscriberCount'],
            'views': item['statistics']['viewCount'],
            'totalVideos': item['statistics']['videoCount'],
            'playlistId': item['contentDetails']['relatedPlaylists']['uploads']
        }
        
        all_data.append(data)
        
    return(pd.DataFrame(all_data))

def get_video_ids(youtube, playlist_id):
    
    """
    Get video IDs from a channel playlist

    Args:
        youtube (object): build object of Youtube API
        playlist_id (list): list of video IDs
        
    Returns:
        pd.DataFrame: all the stats for each video
    """
    
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet, contentDetails",
        playlistId=playlist_id,
        maxResults = 50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')

    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        
    return video_ids

def get_video_details(youtube, video_ids):
    
    """
    Get the stat details from a channel's youtube videos

    Args:
        youtube (object): build object of Youtube API
        video_ids (list): list of video IDs
        
    Returns:
        pd.DataFrame: all the stats for each video
    """
    
    all_video_info = []

    for i in range (0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet, contentDetails, statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {
                'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                'contentDetails' : ['duration', 'definition', 'caption']
            }

            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None
            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)

def get_comments_in_videos(youtube, video_ids):
    
    """
    Get the comments from a channel's videos

    Args:
        youtube (object): build object of Youtube API
        video_ids (list): list of video IDs

    Returns:
        pd.Dataframe: the comments from each video
    """
    all_comments = []
    
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part = 'snippet,replies',
                videoId = video_id
            )
            response = request.execute()

            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments': comments_in_video}

            all_comments.append(comments_in_video_info)
        except:
            print('Error getting comments on video ' + video_id)
        
    return pd.DataFrame(all_comments)
