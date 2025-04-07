import json
import os
import pandas as pd
import pymongo

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20180424"]
update_entries_collection = db["update_entries_2018042411"]
hour = 11

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

# 데이터 로딩
update_entries_df = get_data(update_entries_collection)
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])

# 특정 시간 필터링 (예: 11시 데이터만)
update_entries_df = update_entries_df[update_entries_df['timestamp'].dt.hour == hour]

# ✅ 특정 prefix 지정
target_prefix = "64.70.30.0/24"

# prefix를 포함한 행만 필터링
filtered_df = update_entries_df[update_entries_df['announce_prefixes'].apply(lambda x: target_prefix in x)]

# as_path를 문자열로 변환하여 빈도 계산
as_path_series = filtered_df['as_path'].apply(lambda path: ' '.join(path))
as_path_counts = as_path_series.value_counts().head(10).to_dict()

# JSON 저장
json_path = f'json/as_path_top10_for_{target_prefix.replace("/", "_")}.json'
os.makedirs(os.path.dirname(json_path), exist_ok=True)

with open(json_path, 'w') as f:
    json.dump(as_path_counts, f, indent=4)

print(f"AS path 상위 10개 JSON 저장 완료: {json_path}")