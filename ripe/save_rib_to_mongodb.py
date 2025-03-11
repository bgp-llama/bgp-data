import time
import mrtparse
from pymongo import MongoClient

# SUFFIX = "200404081600"
SUFFIX = "202502010000"

# ✅ MongoDB 연결 정보
MONGO_URI = "mongodb://bgpmongo:27017/"
DB_NAME = "bgp_data"
PEERS_COLLECTION = f"peers_{SUFFIX}"
RIB_COLLECTION = f"rib_{SUFFIX}"
RIB_ENTRIES_COLLECTION = f"rib_entries_{SUFFIX}"

BATCH_SIZE = 10000  # 배치 크기 최적화 (50,000 → 10,000)
MRT_FILE_PATH = f"./ripe_data/bview.{SUFFIX[:8]}.{SUFFIX[-4:]}"


# ✅ MongoDB 연결 (Persistent Connection 유지)
def connect_mongo():
    """MongoDB 연결 생성"""
    try:
        client = MongoClient(MONGO_URI, maxPoolSize=20)  # 연결 풀 크기 설정 (최대 20개)
        return client[DB_NAME]
    except Exception as e:
        print("❌ MongoDB 연결 실패:", e)
        return None


# ✅ BGP PEER 데이터 삽입
def insert_bgp_peers(db, peer_entries):
    """BGP PEER_INDEX_TABLE 데이터를 MongoDB에 저장"""
    peers_collection = db[PEERS_COLLECTION]

    if not peer_entries:
        return

    peer_data = [
        {
            "peer_index": idx,
            "peer_as": peer["peer_as"],
            "peer_bgp_id": peer["peer_bgp_id"],
            "peer_ip": peer["peer_ip"],
        }
        for idx, peer in enumerate(peer_entries)
    ]

    peers_collection.insert_many(peer_data)
    print(f"✅ Inserted {len(peer_data)} BGP peers into MongoDB.")


# ✅ BGP RIB 메타데이터 저장
def insert_bgp_rib(db, rib_batch):
    """BGP RIB 데이터를 MongoDB에 배치 저장"""
    if rib_batch:
        db[RIB_COLLECTION].insert_many(rib_batch)
        print(f"✅ Inserted {len(rib_batch)} BGP RIB entries into MongoDB.")
        rib_batch.clear()


# ✅ BGP RIB 개별 경로 데이터 저장
def insert_bgp_rib_entries(db, rib_entries_batch):
    """BGP RIB 개별 엔트리를 MongoDB에 배치 저장"""
    if rib_entries_batch:
        db[RIB_ENTRIES_COLLECTION].insert_many(rib_entries_batch)
        print(f"✅ Inserted {len(rib_entries_batch)} BGP RIB entries into MongoDB.")
        rib_entries_batch.clear()


# ✅ BGP RIB 데이터 MongoDB 저장
def rib_to_mongodb(file_path):
    """RIB 파일을 MongoDB에 저장"""

    db = connect_mongo()
    if db is None:
        return

    parser = mrtparse.Reader(file_path)

    rib_batch = []
    rib_entries_batch = []
    total_processed = 0

    for idx, entry in enumerate(parser):
        try:
            if "type" in entry.data and entry.data["type"].get(13) == "TABLE_DUMP_V2":
                subtype = entry.data["subtype"].get(1)
                if subtype == "PEER_INDEX_TABLE":
                    print(
                        f"🟢 Found PEER_INDEX_TABLE with {entry.data.get('peer_count', 0)} peers"
                    )
                    insert_bgp_peers(db, entry.data["peer_entries"])
                    continue

            entry_data = {
                "entry_id": idx + 1,
                "timestamp": next(iter(entry.data["timestamp"].values()), None),
                "type": next(iter(entry.data["type"].values()), None),
                "subtype": next(iter(entry.data["subtype"].values()), None),
                "sequence_number": entry.data.get("sequence_number", 0),
                "prefix": entry.data.get("prefix"),
                "entry_count": entry.data.get("entry_count", 0),
            }

            rib_batch.append(entry_data)
            total_processed += 1

            if "rib_entries" in entry.data:
                for rib_entry in entry.data["rib_entries"]:
                    try:
                        rib_entries_batch.append(
                            {
                                "rib_entry_id": entry_data["entry_id"],
                                "peer_index": rib_entry["peer_index"],
                                "originated_time": next(
                                    iter(rib_entry["originated_time"].values()), None
                                ),
                                "next_hop": next(
                                    (
                                        item["value"]
                                        for item in rib_entry.get("path_attributes", [])
                                        if item["type"].get(3) == "NEXT_HOP"
                                    ),
                                    None,
                                ),
                                "as_path": next(
                                    (
                                        attr["value"][0]["value"]
                                        for attr in rib_entry.get("path_attributes", [])
                                        if attr["type"].get(2) == "AS_PATH"
                                    ),
                                    None,  # 기본값을 None으로 설정
                                ),
                                "origin": next(
                                    (
                                        item["value"].get(0, "INCOMPLETE")
                                        for item in rib_entry.get("path_attributes", [])
                                        if item["type"].get(1) == "ORIGIN"
                                    ),
                                    "INCOMPLETE",
                                ),
                                "community": [
                                    item["value"]
                                    for item in rib_entry.get("path_attributes", [])
                                    if item["type"].get(8) == "COMMUNITY"
                                ],
                            }
                        )
                    except Exception as e:
                        # print detailed stack trace
                        print
                        # print(f"❌ Error processing BGP RIB entry detail: {e}")

            # 🔹 10,000개 단위로 배치 저장
            if total_processed % BATCH_SIZE == 0:
                insert_bgp_rib(db, rib_batch)
                insert_bgp_rib_entries(db, rib_entries_batch)
                print(f"🚀 Processed {total_processed} total entries...")

        except Exception as e:
            print(f"❌ Error processing entry {idx + 1}: {e}")

    # 마지막 배치 저장
    insert_bgp_rib(db, rib_batch)
    insert_bgp_rib_entries(db, rib_entries_batch)

    print(f"🎯 Successfully inserted {total_processed} BGP RIB entries into MongoDB.")


# ✅ 실행
if __name__ == "__main__":
    start_time = time.time()
    rib_to_mongodb(MRT_FILE_PATH)
    print(f"🎯 Total elapsed time: {time.time() - start_time:.2f} seconds.")
