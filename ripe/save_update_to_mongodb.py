import mrtparse
import os
from datetime import datetime
from pymongo import MongoClient, errors

SUFFIX = "202502010000"
# ✅ MongoDB 연결 정보
MONGO_URI = "mongodb://bgpmongo:27017/"
DB_NAME = "bgp_data"
COLLECTION_NAME = f"update_entries_202502010000"
MRT_FILE_NAME = f"./ripe_data/updates.{SUFFIX[:8]}.{SUFFIX[-4:]}"


def update_to_mongodb(file_path, mongo_uri, db_name, collection_name):
    """
    UPDATE MRT 파일을 MongoDB로 직접 저장.

    Args:
        file_path (str): UPDATE MRT 파일 경로.
        mongo_uri (str): MongoDB 연결 URI.
        db_name (str): 저장할 데이터베이스 이름.
        collection_name (str): 저장할 컬렉션 이름.
    """
    print(f"Parsing and inserting UPDATE MRT file: {file_path} into MongoDB...")

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000, connect=False)
        db = client[db_name]
        collection = db[collection_name]

        parser = mrtparse.Reader(file_path)
        batch_size = 1000
        batch = []

        for idx, entry in enumerate(parser):
            try:
                if "bgp_message" not in entry.data:
                    continue

                bgp_message = entry.data["bgp_message"]

                # timestamp 수정: 값이 존재할 경우만 저장
                timestamp = None
                if "timestamp" in entry.data:
                    timestamp = list(entry.data["timestamp"].values())[0]

                # announce_prefixes & withdraw_prefixes 파싱
                announce_prefixes = []
                withdraw_prefixes = []

                if "nlri" in bgp_message:
                    announce_prefixes = [nlri["prefix"] for nlri in bgp_message["nlri"]]

                if "withdrawn_routes" in bgp_message:
                    withdraw_prefixes = [
                        withdrawn["prefix"]
                        for withdrawn in bgp_message["withdrawn_routes"]
                    ]

                # AS_PATH 추출
                as_path = []
                if "path_attributes" in bgp_message:
                    for attr in bgp_message["path_attributes"]:
                        if next(iter(attr.get("type"))) == 2:
                            if isinstance(attr.get("value"), list):
                                for as_seq in attr.get("value"):
                                    if isinstance(as_seq, dict) and "value" in as_seq:
                                        as_path.extend(as_seq["value"])

                update_entry = {
                    "entry_id": idx + 1,
                    "timestamp": timestamp,
                    "peer_as": entry.data.get("peer_as"),
                    "local_as": entry.data.get("local_as"),
                    "announce_prefixes": announce_prefixes,
                    "withdraw_prefixes": withdraw_prefixes,
                    "as_path": as_path,
                }

                batch.append(update_entry)

                if len(batch) >= batch_size:
                    collection.insert_many(batch)
                    print(f"Inserted {len(batch)} UPDATE entries into MongoDB...")
                    batch.clear()

            except Exception as e:
                print(f"Error processing UPDATE entry {idx + 1}: {e}")
                continue

        if batch:
            collection.insert_many(batch)
            print(f"Inserted remaining {len(batch)} UPDATE entries into MongoDB...")

        print(f"UPDATE data stored in MongoDB: {db_name}.{collection_name}")

    except errors.ServerSelectionTimeoutError as e:
        print(f"MongoDB connection failed: {e}")

    except errors.AutoReconnect as e:
        print(f"MongoDB auto-reconnect issue: {e}")

    finally:
        client.close()


if __name__ == "__main__":
    # 이미 다운로드되어 있는 MRT 파일 경로
    start_time = datetime.now()

    if os.path.exists(MRT_FILE_NAME):
        update_to_mongodb(MRT_FILE_NAME, MONGO_URI, DB_NAME, COLLECTION_NAME)
    else:
        print(f"Failed to locate the MRT file at {MRT_FILE_NAME}.")

    print(f"Total time elapsed: {datetime.now() - start_time}")
