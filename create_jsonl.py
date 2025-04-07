import json
import os
import pandas as pd
import pymongo

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://bgpmongo:27017/")
db = client["20211004"]
update_entries_collection = db["update_entries_2021100414"]
hour = 14
target_asn = "32934"  # 문자열로 비교

# 데이터를 DataFrame으로 변환하는 함수
def get_data(collection):
    return pd.DataFrame(list(collection.find()))

# 데이터 로드 및 전처리
update_entries_df = get_data(update_entries_collection)
update_entries_df['timestamp'] = pd.to_datetime(update_entries_df['timestamp'])
update_entries_df = update_entries_df[update_entries_df['timestamp'].dt.hour == hour]
update_entries_df['minute'] = update_entries_df['timestamp'].dt.minute

# AS path에 target_asn이 포함된 행 필터링
df_32934 = update_entries_df[
    update_entries_df['as_path'].apply(lambda path: target_asn in path)
].copy()

# ✅ 분 단위로 요약
grouped = df_32934.groupby('minute')

summary_blocks = []

for minute, group in grouped:
    ts_str = f"{hour:02d}:{minute:02d}"
    announces = group[group['announce_prefixes'].apply(len) > 0]
    withdraws = group[group['withdraw_prefixes'].apply(len) > 0]
    
    announced_prefixes = sum(announces['announce_prefixes'], [])
    withdrawn_prefixes = sum(withdraws['withdraw_prefixes'], [])
    
    unique_announced = set(announced_prefixes)
    unique_withdrawn = set(withdrawn_prefixes)

    summary_text = (
        f"At {ts_str}, AS{target_asn} had "
        f"{len(announces)} announcement(s) for {len(unique_announced)} prefix(es), "
        f"{len(withdraws)} withdrawal(s) for {len(unique_withdrawn)} prefix(es)."
    )

    summary_blocks.append({
        "time": ts_str,
        "summary": summary_text,
        "announce_count": len(announces),
        "withdraw_count": len(withdraws),
        "announced_prefixes": list(unique_announced),
        "withdrawn_prefixes": list(unique_withdrawn)
    })

# ✅ JSONL 저장
output_dir = 'jsonl'
os.makedirs(output_dir, exist_ok=True)
jsonl_path = os.path.join(output_dir, f'as{target_asn}_summary_{hour}H.jsonl')

with open(jsonl_path, 'w', encoding='utf-8') as f:
    for entry in summary_blocks:
        f.write(json.dumps(entry) + '\n')

print(f"📄 분 단위 요약 JSONL이 저장되었습니다: {jsonl_path}")