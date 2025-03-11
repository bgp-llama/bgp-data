import pymongo
import json

TARGET_AS = "852"
SUFFIX = "202501010000"

# MongoDB 접속 설정
MONGO_URI = "mongodb://bgpmongo:27017/"
DB_NAME = "bgp_data"
UPDATE_ENTRIES_COLLECTION = f"update_entries_{SUFFIX}"
RESULT_JSON_PATH = f"json/update_graph_data_{SUFFIX}.json"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
update_entries = db[UPDATE_ENTRIES_COLLECTION]

# MongoDB에서 데이터 가져오기
bgp_updates = list(
    update_entries.find({"$or": [{"local_as": TARGET_AS}, {"peer_as": TARGET_AS}]})
)

# 애니메이션용 JSON 데이터
animation_data = []

# BGP 업데이트 데이터를 애니메이션 이벤트로 변환
for update in bgp_updates:
    timestamp = update["timestamp"]
    local_as = update["local_as"]
    peer_as = update["peer_as"]

    # Announce 이벤트 추가
    for prefix in update.get("announce_prefixes", []):
        animation_data.append(
            {
                "type": "announce",
                "timestamp": timestamp,
                "local_as": local_as,
                "peer_as": peer_as,
                "prefix": prefix,
            }
        )

    # Withdraw 이벤트 추가
    for prefix in update.get("withdraw_prefixes", []):
        animation_data.append(
            {
                "type": "withdraw",
                "timestamp": timestamp,
                "local_as": local_as,
                "peer_as": peer_as,
                "prefix": prefix,
            }
        )

    # AS Path 변경 이벤트 추가
    as_path = update.get("as_path", [])
    if as_path:
        animation_data.append(
            {
                "type": "path_change",
                "timestamp": timestamp,
                "local_as": local_as,
                "peer_as": peer_as,
                "as_path": as_path,
            }
        )

# JSON 파일로 저장
with open(RESULT_JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(animation_data, f, indent=4, ensure_ascii=False)

print(f"✅ JSON 파일이 생성되었습니다: {RESULT_JSON_PATH}")
