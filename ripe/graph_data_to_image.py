import json
import networkx as nx
import matplotlib.pyplot as plt

SUFFIX = "202501010000"
JSON_FILE = f"json/rib_graph_data_{SUFFIX}.json"
RESULT_IMAGE_FILE = f"image/graph_{SUFFIX}.png"
TARGET_AS = "18298"

# JSON 파일에서 데이터를 읽어옵니다.
with open(JSON_FILE, "r", encoding="utf-8") as file:
    data = json.load(file)

# 방향성을 가진 그래프로 생성 (링크가 방향을 나타내므로 DiGraph 사용)
G = nx.DiGraph()

# 노드 추가
for node in data["nodes"]:
    G.add_node(node["id"])

# 링크(에지) 추가
for link in data["links"]:
    source = link["source"]
    target = link["target"]
    G.add_edge(source, target)

# 그래프 그리기 (레이아웃과 노드, 에지 스타일 설정)
plt.figure(figsize=(12, 12))
pos = nx.spring_layout(G, k=0.15, iterations=20)
node_colors = ["red" if node == TARGET_AS else "skyblue" for node in G.nodes()]
nx.draw_networkx(
    G,
    pos,
    with_labels=True,
    node_color=node_colors,
    node_size=500,
    font_size=8,
    arrows=True,
)

# 축 숨기기 및 이미지 파일로 저장
plt.axis("off")
plt.savefig(RESULT_IMAGE_FILE, format="PNG")
plt.close()
