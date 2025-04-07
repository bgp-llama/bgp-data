import os
from save_update_to_mongodb import update_to_mongodb

MONGO_URI = "mongodb://bgpmongo:27017/"
date = "20211004"
hour = "14"


for minute in ["00", "15", "30", "45"]:
    os.system(f'curl -o ./routeviews_data/updates.{date}.{hour}{minute}.bz2 https://routeviews.org/bgpdata/{date[:4]}.{date[4:6]}/UPDATES/updates.{date}.{hour}{minute}.bz2')
    os.system(f'bzip2 -d ./routeviews_data/updates.{date}.{hour}{minute}.bz2')

    update_to_mongodb(f"./routeviews_data/updates.{date}.{hour}{minute}", MONGO_URI, date, f"update_entries_{date}{hour}")
    os.system(f'rm ./routeviews_data/updates.{date}.{hour}{minute}')
