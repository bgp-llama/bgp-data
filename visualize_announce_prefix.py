import pymongo
import pandas as pd
import matplotlib.pyplot as plt

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20180424"]
update_entries_collection = db["update_entries_2018042411"]

threshold = 40

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

# Update entries 데이터를 가져옵니다
update_entries_df = get_data(update_entries_collection)

# timestamp 컬럼을 datetime 형식으로 변환
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])

# 'announce_prefixes' 길이 계산
update_entries_df['announce_prefixes_length'] = update_entries_df['announce_prefixes'].apply(lambda x: len(x) if isinstance(x, list) else 0)

# 길이가 긴 announce_prefixes만 필터링 (예: 길이가 3 이상인 프리픽스)
long_announce_prefixes_df = update_entries_df[update_entries_df['announce_prefixes_length'] >= threshold]

# 길이가 긴 announce_prefixes의 개수
print(f"Number of updates with announce_prefixes length >= {threshold}: {long_announce_prefixes_df.shape[0]}")

# CSV로 저장
long_announce_prefixes_df[['announce_prefixes', 'as_path', 'timestamp']].to_csv('csv/long_announce_prefix.csv', index=False)

# 완료 메시지 출력
print("CSV 저장 완료: long_announce_prefix.csv")