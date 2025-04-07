import json
import os
import pandas as pd
import pymongo
from collections import Counter

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20211004"]
update_entries_collection = db["update_entries_2021100415"]
hour = 15

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

# 데이터 로딩
update_entries_df = get_data(update_entries_collection)
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])

# 특정 시간 필터링 (예: 11시 데이터만)
update_entries_df = update_entries_df[update_entries_df['timestamp'].dt.hour == hour]

# 각 announce_prefix를 하나의 리스트로 풀어냄
all_announced_prefixes = update_entries_df['announce_prefixes'].explode()

# prefix별 업데이트 발생 횟수 세기
prefix_counts = all_announced_prefixes.value_counts().head(20).to_dict()

# JSON으로 저장
json_path = 'json/prefix_update_counts.json'
os.makedirs(os.path.dirname(json_path), exist_ok=True)

with open(json_path, 'w') as f:
    json.dump(prefix_counts, f, indent=4)

print(f"JSON 파일 저장 완료: {json_path}")