import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from validators_and_processors.pandas_processors import df_from_file
from validators_and_processors.validators import is_element_of_list_in_text

LOCAL_FOLDER = "/home/universe.dart.spb/amkhoyan/Documents/DataEngeener/TASKS/data_processing_and_analytics/data/pandas"
EVENTS = "events.jsonl"
USERS = "users.csv"
VIDEOS = 'videos.csv'


def pipeline_to_visualize_videos_like_rate(events: pd.DataFrame):

    video_by_like = events[['video_id', 'event', 'timestamp']][events['event'] == 'like']\
        .groupby(['video_id', 'timestamp'], as_index=False).agg({'event': 'count'})

    plt.figure(figsize=(10, 6))
    sns.barplot(x="timestamp", hue="video_id", y="event", data=video_by_like)
    plt.show()


def video_likes_rate_by_tags(events: pd.DataFrame, users: pd.DataFrame, videos: pd.DataFrame):

    all_df = (
        events.merge(users, how='left', left_on='user_id', right_on='id')
        .merge(videos, how='left', left_on='video_id', right_on='id')
    )

    with_common_teg = (
        all_df[all_df.apply(lambda x: is_element_of_list_in_text(x['categories'].split(';'), x['name']), axis=1)]
        .groupby(['user_id', 'fname', 'lname', 'video_id', 'timestamp'], as_index=False)
        .agg({'event': 'count'})
        .rename(columns={'event': 'like_count'})
    )

    without_teg = (
        all_df[all_df.apply(lambda x: not is_element_of_list_in_text(x['categories'].split(';'), x['name']), axis=1)]
        .sort_values('user_id')
        .groupby(['user_id', 'fname', 'lname', 'video_id', 'timestamp'], as_index=False)
        .agg({'event': 'count'})
        .rename(columns={'event': 'like_count'})
    )

    print(with_common_teg)
    print(without_teg)


def main():
    print(os.path.isfile(os.path.join(LOCAL_FOLDER, EVENTS)))
    events = df_from_file(os.path.join(LOCAL_FOLDER, EVENTS))

    users = df_from_file(os.path.join(LOCAL_FOLDER, USERS))
    videos = df_from_file(os.path.join(LOCAL_FOLDER, VIDEOS))

    events['timestamp'] = pd.to_datetime(events['timestamp'], unit='s').dt.date
    users['id'] = users['id'].astype(int)
    videos['id'] = videos['id'].astype(int)

    pipeline_to_visualize_videos_like_rate(events)
    video_likes_rate_by_tags(events, users, videos)


if __name__ == "__main__":
    main()
