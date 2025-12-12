# extract_youtube.py

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.discovery import build

# ----------------------------
# Setup
# ----------------------------

# Load environment variables
load_dotenv()
API_KEY = os.environ.get("API_KEY")

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "input_data"  # Data folder at same level as script
DATA_DIR.mkdir(exist_ok=True)

# ----------------------------
# Extract functions
# ----------------------------

def get_channel_ids(file_name="chanels_ids.json"):
    """
    Read channel IDs from a JSON file in the same folder as the script
    """
    file_path = BASE_DIR / file_name
    with open(file_path, "r", encoding="utf8") as f:
        data = json.load(f)
    return data.get("channel_ids", [])

def get_youtube_client():
    """Returns a YouTube API client"""
    return build("youtube", "v3", developerKey=API_KEY)

def get_channel_stats(youtube, channel_ids, save_file="channel_stats.json"):
    all_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response = request.execute()

    for item in response.get('items', []):
        data = {
            'channelName': item['snippet']['title'],
            'subscribers': item['statistics'].get('subscriberCount'),
            'views': item['statistics'].get('viewCount'),
            'totalVideos': item['statistics'].get('videoCount'),
            'playlistId': item['contentDetails']['relatedPlaylists']['uploads']
        }
        all_data.append(data)

    # Save JSON
    save_path = DATA_DIR / save_file
    with open(save_path, "w", encoding="utf8") as f:
        json.dump(all_data, f, indent=2)

    return all_data

def get_video_ids(youtube, playlist_id, save_file=None):
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get('items', []):
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    if save_file:
        save_path = DATA_DIR / save_file
        with open(save_path, "w", encoding="utf8") as f:
            json.dump(video_ids, f, indent=2)

    return video_ids

def get_video_details(youtube, video_ids, save_file="video_details.json"):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response.get('items', []):
            video_info = {
                'video_id': video.get('id'),
                'channelTitle': video.get('snippet', {}).get('channelTitle'),
                'title': video.get('snippet', {}).get('title'),
                'description': video.get('snippet', {}).get('description'),
                'tags': video.get('snippet', {}).get('tags'),
                'publishedAt': video.get('snippet', {}).get('publishedAt'),
                'viewCount': video.get('statistics', {}).get('viewCount'),
                'likeCount': video.get('statistics', {}).get('likeCount'),
                'favouriteCount': video.get('statistics', {}).get('favouriteCount'),
                'commentCount': video.get('statistics', {}).get('commentCount'),
                'duration': video.get('contentDetails', {}).get('duration'),
                'definition': video.get('contentDetails', {}).get('definition'),
                'caption': video.get('contentDetails', {}).get('caption')
            }
            all_video_info.append(video_info)

    # Save JSON
    if save_file:
        save_path = DATA_DIR / save_file
        with open(save_path, "w", encoding="utf8") as f:
            json.dump(all_video_info, f, indent=2)

    return all_video_info

def get_comments_in_videos(youtube, video_ids, save_file="comments.json", max_comments=10):
    all_comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_comments
            )
            response = request.execute()
            comments = [
                item['snippet']['topLevelComment']['snippet']['textOriginal']
                for item in response.get('items', [])
            ]
            all_comments.append({'video_id': video_id, 'comments': comments})
        except Exception:
            print(f'Could not get comments for video {video_id}')

    # Save JSON
    if save_file:
        save_path = DATA_DIR / save_file
        with open(save_path, "w", encoding="utf8") as f:
            json.dump(all_comments, f, indent=2)

    return all_comments

# ----------------------------
# Example execution (can be called from ETL)
# ----------------------------

if __name__ == "__main__":
    youtube_client = get_youtube_client()
    channels = get_channel_ids()

    # Save channel stats
    channel_stats = get_channel_stats(youtube_client, channels, save_file="channel_stats.json")

    # Get video IDs for all channels and save per channel
    all_video_ids = []
    for ch in channel_stats:
        playlist_id = ch['playlistId']
        vids = get_video_ids(youtube_client, playlist_id, save_file=f"video_ids_{ch['channelName']}.json")
        all_video_ids.extend(vids)

    # Save video details
    video_details = get_video_details(youtube_client, all_video_ids, save_file="video_details.json")

    # Save comments
    comments_data = get_comments_in_videos(youtube_client, all_video_ids, save_file="comments.json")

    print(f"Extracted {len(channel_stats)} channels, {len(video_details)} videos, "
          f"{len(comments_data)} comment sets")
