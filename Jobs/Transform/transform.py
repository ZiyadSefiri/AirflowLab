# transform_videos.py

import pandas as pd
from dateutil import parser
import isodate
from pathlib import Path

def transform_video_data(video_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a raw video DataFrame by:
    - Converting count columns to numeric
    - Parsing publishedAt to datetime and extracting day of week
    - Converting ISO8601 duration to seconds
    - Counting tags
    
    Args:
        video_df (pd.DataFrame): Raw video data
        
    Returns:
        pd.DataFrame: Transformed video data
    """

    # 1. Check for nulls (optional, can log or handle downstream)
    if video_df.isnull().any().any():
        print("Warning: Null values detected in the following columns:")
        print(video_df.isnull().sum())

    # 2. Convert count columns to numeric
    numeric_cols = ['viewCount', 'likeCount', 'favouriteCount', 'commentCount']
    video_df[numeric_cols] = video_df[numeric_cols].apply(pd.to_numeric, errors='coerce', axis=1)

    # 3. Parse 'publishedAt' to datetime and extract day of week
    video_df['publishedAt'] = video_df['publishedAt'].apply(lambda x: parser.parse(x))
    video_df['publishDayName'] = video_df['publishedAt'].apply(lambda x: x.strftime("%A"))

    # 4. Convert ISO8601 duration to seconds
    video_df['durationSecs'] = video_df['duration'].apply(lambda x: isodate.parse_duration(x))
    video_df['durationSecs'] = video_df['durationSecs'].astype('timedelta64[s]')

    # 5. Add tag count
    video_df['tagCount'] = video_df['tags'].apply(lambda x: 0 if x is None else len(x))

    return video_df

# --------------------------
# Example usage
# --------------------------

# Get path to data folder relative to this script
base_path = Path(__file__).parent.parent
data_path = base_path/ "Extract" / "Data" / "video_details.json"  # Adjust folder/file names as needed

# Load the raw data
df = pd.read_json(data_path)

# Transform
transformed_df = transform_video_data(df)

# Preview
print(transformed_df.head())
