import json
import os
import pandas as pd
import pymongo

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20211004"]
update_entries_collection = db["update_entries_2021100415"]
hour = 15

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

update_entries_df = get_data(update_entries_collection)
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])
update_entries_df = update_entries_df[(update_entries_df['timestamp'].dt.hour == hour)]
update_entries_df['minute'] = update_entries_df['timestamp'].dt.minute

# Updates 발생 횟수 계산
updates_by_minute = update_entries_df.groupby('minute').size()

json_output = updates_by_minute.to_dict()
json_path = 'json/updates_count.json'

with open(json_path, 'w') as f:
    json.dump(json_output, f, indent=4)

print(f"JSON 파일 저장 완료: {json_path}")