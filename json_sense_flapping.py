import pandas as pd
import pymongo
import json
from datetime import timedelta

# MongoDB에서 데이터 불러오기
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20180424"]
coll = db["update_entries_2018042411"]
df = pd.DataFrame(list(coll.find()))
df['timestamp'] = pd.to_datetime(df['timestamp'])

# explode prefix, label announce/withdraw
df_ann = df[['timestamp', 'announce_prefixes']].explode('announce_prefixes')
df_ann = df_ann.rename(columns={'announce_prefixes': 'prefix'})
df_ann['type'] = 'announce'

df_wd = df[['timestamp', 'withdraw_prefixes']].explode('withdraw_prefixes')
df_wd = df_wd.rename(columns={'withdraw_prefixes': 'prefix'})
df_wd['type'] = 'withdraw'

events = pd.concat([df_ann, df_wd])
events = events.dropna(subset=['prefix'])
events = events.sort_values(by='timestamp')

# 슬라이딩 윈도우 방식으로 플래핑 감지 + 발생 횟수 저장
window_sec = 10
threshold = 5 
flapping_stats = {}

for prefix, group in events.groupby('prefix'):
    group = group.sort_values('timestamp')
    types = group['type'].tolist()
    timestamps = group['timestamp'].tolist()

    # 슬라이딩 윈도우에서 '타입 전환 횟수'를 기준으로 감지
    start = 0
    for end in range(len(timestamps)):
        while timestamps[end] - timestamps[start] > timedelta(seconds=window_sec):
            start += 1
        window_types = types[start:end+1]
        flip_count = sum(1 for i in range(1, len(window_types)) if window_types[i] != window_types[i-1])
        if flip_count >= threshold:
            flapping_stats[prefix] = flip_count
            break
flapping_stats_sorted = dict(sorted(flapping_stats.items(), key=lambda item: item[1], reverse=True))

# JSON으로 저장
output_path = 'json/flapping_prefixes_300_500.json'

with open(output_path, 'w') as f:
    json.dump(flapping_stats_sorted, f, indent=4)

print(f"정렬된 플래핑 prefix 정보 저장 완료: {output_path}")