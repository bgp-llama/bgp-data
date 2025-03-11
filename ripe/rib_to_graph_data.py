import pymongo
import json

# SUFFIX = "200404081600"
SUFFIX = "202501010000"

# MongoDB 접속 설정
MONGO_URI = "mongodb://bgpmongo:27017/"
DB_NAME = "bgp_data"
RIB_ENTRIES_COLLECTION = f"rib_entries_{SUFFIX}"
TARGET_AS = "18298"
RESULT_JSON_PATH = f"json/rib_graph_data_{SUFFIX}.json"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
rib_entries = db[RIB_ENTRIES_COLLECTION]

# 노드와 링크를 저장할 자료구조 초기화
nodes = set()
links = []

# MongoDB에서 특정 AS가 포함된 문서만 조회
query = {"as_path": {"$in": [TARGET_AS]}}
for entry in rib_entries.find(query):
    as_path = entry.get("as_path", [])
    if len(as_path) < 2:
        continue
    for i in range(len(as_path) - 1):
        source = as_path[i]
        target = as_path[i + 1]
        nodes.add(source)
        nodes.add(target)
        links.append({"source": source, "target": target})

# 노드 리스트 생성 (각 노드는 "id" 필드를 가짐)
nodes_list = [{"id": node} for node in nodes]

# 최종 그래프 JSON 생성
graph = {"nodes": nodes_list, "links": links}

# JSON 파일로 저장
with open(RESULT_JSON_PATH, "w") as f:
    json.dump(graph, f, indent=2)

# JSON 포맷으로 출력
# print(json.dumps(graph, indent=2))
