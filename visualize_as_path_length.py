import pymongo
import pandas as pd
import matplotlib.pyplot as plt

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20180424"]
update_entries_collection = db["update_entries_2018042411"]

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

# Update entries 데이터를 가져옵니다
update_entries_df = get_data(update_entries_collection)

# timestamp 컬럼을 datetime 형식으로 변환
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])

# AS 경로 길이 계산
update_entries_df['as_path_length'] = update_entries_df['as_path'].apply(lambda x: len(x) if isinstance(x, list) else 0)

# 길이가 긴 AS 경로만 필터링 (예: 길이가 5 이상인 경로)
long_as_paths_df = update_entries_df[update_entries_df['as_path_length'] >= 5]

# 긴 경로의 개수
print(f"Number of updates with AS path length >= 5: {long_as_paths_df.shape[0]}")

# 경로 길이 분포 (히스토그램)
plt.figure(figsize=(10, 6))
plt.hist(update_entries_df['as_path_length'], bins=range(1, 21), edgecolor='black', alpha=0.7)
plt.title('Distribution of AS Path Lengths', fontsize=14)
plt.xlabel('AS Path Length', fontsize=12)
plt.ylabel('Number of Updates', fontsize=12)
plt.grid(True)
plt.tight_layout()

# 그래프 출력
plt.savefig('images/as_path_length_distribution.png')

# 긴 AS 경로 데이터 확인 (상위 10개 출력)
print(long_as_paths_df[['announce_prefixes', 'as_path', 'timestamp']].head(10))