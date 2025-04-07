import pymongo
import pandas as pd
import matplotlib.pyplot as plt

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20211004"]
update_entries_collection = db["update_entries_2021100415"]
hour = 15
target_as = "32934"

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

# Update entries 데이터를 가져옵니다
update_entries_df = get_data(update_entries_collection)

# timestamp 컬럼을 datetime 형식으로 변환
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])

update_entries_df = update_entries_df[(update_entries_df['timestamp'].dt.hour == hour)]
update_entries_df = update_entries_df[update_entries_df['as_path'].apply(lambda x: target_as in x)]

print (f"데이터 개수: {len(update_entries_df)}")

# 각 분(minute)별로 업데이트 및 withdraw 이벤트 개수 계산
update_entries_df['minute'] = update_entries_df['timestamp'].dt.minute

# Updates 발생 횟수 계산
updates_by_minute = update_entries_df.groupby('minute').size()

# Withdraws 발생 횟수 계산 (withdraw_prefixes가 비어있는 경우를 제외)
withdraws_by_minute = update_entries_df[update_entries_df['withdraw_prefixes'].apply(len) > 0].groupby('minute').size()

# 그래프 그리기
plt.figure(figsize=(12, 6))
plt.plot(updates_by_minute.index, updates_by_minute.values, marker='o', label='Updates', color='b')
plt.plot(withdraws_by_minute.index, withdraws_by_minute.values, marker='x', label='Withdraws', color='r')

plt.title('Updates and Withdraws for 11:00 - 12:00', fontsize=14)
plt.xlabel('Minute of the Hour (11:00 - 12:00)', fontsize=12)
plt.ylabel('Number of Events', fontsize=12)
plt.xticks(range(0, 60, 1))  # X축: 0부터 59까지 분으로 표시
plt.legend()
plt.grid(True)
plt.tight_layout()

# 그래프 출력
plt.savefig('images/updates_withdraws_count.png')