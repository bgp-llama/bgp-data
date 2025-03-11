from pymongo import MongoClient

MONGO_URI = "mongodb://bgpmongo:27017/"
DB_NAME = "bgp_data"
UPDATE_COLLECTION_NAME = f"update_entries_202502010000"
RIB_COLLECTION_NAME = f"rib_202502010000"
RIB_ENTRIES_NAME = f"rib_entries_202502010000"
TARGET_AS = "48362"

def get_changes():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connect=False)
    db = client[DB_NAME]
    rib_collection = db[RIB_COLLECTION_NAME]
    update_collection = db[UPDATE_COLLECTION_NAME]

    count = 0
    # update 데이터 순회
    for entry in update_collection.find({"as_path": TARGET_AS}):
        announce_prefixes = entry["announce_prefixes"]
        withdraw_prefixes = entry["withdraw_prefixes"]
        as_path = entry["as_path"]

        print(entry["timestamp"])
        print(as_path)

        # announce_prefixes가 존재하는 경우
        if len(announce_prefixes) > 0:

            origin_as_paths = []

            for prefix in announce_prefixes:
                rib_entry = rib_collection.find_one({"prefix": prefix})
                if rib_entry is None:
                    print("new prefix:", prefix)
                else:
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

                print("announce:", announce_prefixes)

        # withdraw_prefixes가 존재하는 경우
        if len(withdraw_prefixes) > 0:
            for prefix in withdraw_prefixes:
                rib_entry_id = rib_collection.find_one({"prefix": prefix})["entry_id"]

                print(rib_entry_id)

                origin_as_paths = [
                    rib_entry["as_path"]
                    for rib_entry in rib_collection.find({"rib_entry_id": rib_entry_id})
                ]

                print("withdraw:", origin_as_paths)

        if count > 10:
            break
        print()
        count += 1


if __name__ == "__main__":
    get_changes()
