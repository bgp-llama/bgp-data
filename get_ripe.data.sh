# wget -P ./ripe_data https://data.ris.ripe.net/rrc00/2025.02/bview.20250201.0000.gz
wget -P ./ripe_data https://data.ris.ripe.net/rrc00/2025.02/updates.20250201.0000.gz

gzip -d ripe_data/updates.20250201.0000.gz

wget -P ./routeviews_data https://archive.routeviews.org/bgpdata/2025.01/RIBS/rib.20250115.0000.bz2
wget -P ./routeviews_data https://archive.routeviews.org/bgpdata/2025.01/UPDATES/updates.20250115.0000.bz2

bzip2 -d routeviews_data/rib.20250115.0000.bz2
bzip2 -d routeviews_data/updates.20250115.0000.bz2