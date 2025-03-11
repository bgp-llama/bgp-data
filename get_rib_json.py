from pymongo import MongoClient

MONGO_URI = "mongodb://bgpmongo:27017/"
DB_NAME = "bgp_data"
RIB_COLLECTION_NAME = f"rib_202502010000"
RIB_ENTRIES_NAME = f"rib_entries_202502010000"
TARGET_AS = "48362"

def get_ribs():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connect=False)
    db = client[DB_NAME]
    rib_collection = db[RIB_COLLECTION_NAME]

    count = 0
    # rib 데이터 순회
    for rib_entry in rib_collection.find():
        timestamp = rib_entry["timestamp"]
        entry_id = rib_entry["entry_id"]
    

        prefix = rib_entry["prefix"]
        as_path = rib_entry["as_path"]

        print(prefix)
        print(as_path)

        # as_path가 존재하는 경우
        if len(as_path) > 0:
            origin_as_paths = []

            for rib_entry in rib_collection.find({"prefix": prefix}):
                rib_entry_id = rib_entry["entry_id"]

                origin_as_paths = [
                    rib_entry["as_path"]
                    for rib_entry in rib_collection.find({"rib_entry_id": rib_entry_id})
                ]

                for origin_as_path in origin_as_paths:
                    if (
                        origin_as_path[0] == as_path[0]
                        and origin_as_path[-1] == as_path[-1]
                    ):
                        print("path change:", origin_as_path, as_path)
                    else:
                        print("announce:", origin_as_path)

                print("announce:", as_path)

        count += 1

        if count == 10:
            break


if __name__ == "__main__":
    get_ribs()
