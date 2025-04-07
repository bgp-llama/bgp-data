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

# 'announce_prefixes' 길이 계산
update_entries_df['announce_prefixes_length'] = update_entries_df['announce_prefixes'].apply(lambda x: len(x) if isinstance(x, list) else 0)

# announce_prefixes 길이 분포 (히스토그램)
plt.figure(figsize=(10, 6))
plt.hist(update_entries_df['announce_prefixes_length'], bins=range(1, 21), edgecolor='black', alpha=0.7)
plt.title('Distribution of Announce Prefixes Lengths', fontsize=14)
plt.xlabel('Announce Prefixes Length', fontsize=12)
plt.ylabel('Number of Updates', fontsize=12)
plt.grid(True)
plt.tight_layout()

# 그래프 이미지 파일로 저장
plt.savefig('images/announce_prefixes_length_distribution.png')

# 완료 메시지 출력
print("그래프 이미지 저장 완료: images/announce_prefixes_length_distribution.png")