# -*- coding: utf-8 -*-
# 秘密基地 · 聊天后端
# 一件事：把小愿 / 克老师 / 朵朵 说的每句话，原样存住，再给前端「收话 / 发话」两个口子。
# 关键：原样存（json 字符串照存），不做任何「消化 / 摘要」，从根上不踩记忆库那个引号截断的坑。

import json, os, time, threading
from pathlib import Path
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许网页（不同域名）跨域来取话、发话

# ── 存储 ──────────────────────────────────────────────
# DATA_DIR 必须指向 Render 的【持久盘挂载点】。
# （塌方的教训：盘没挂对，容器一重启数据就没。部署时务必把持久盘挂到这个路径，
#  并在环境变量里把 DATA_DIR 设成那个挂载点。本地跑默认用 ./data。）
DATA_DIR = os.environ.get("DATA_DIR", str(Path(__file__).parent / "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
STORE = Path(DATA_DIR) / "messages.json"

_lock = threading.Lock()
WHO_OK = {"小愿", "克老师", "朵朵"}

def _now_iso():
    # 北京时间，ISO 格式，和前端时间对齐
    return time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())

def load():
    if STORE.exists():
        try:
            return json.loads(STORE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []

def save(msgs):
    # 先写临时文件再替换，避免写一半被读到
    tmp = STORE.with_suffix(".tmp")
    tmp.write_text(json.dumps(msgs, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STORE)

# ── 接口 ──────────────────────────────────────────────
@app.get("/")
def health():
    return jsonify({"ok": True, "name": "秘密基地·聊天后端", "count": len(load())})

@app.get("/api/messages")
def get_messages():
    """取话。可带 ?since=<上一条的 t>，只取它之后的新话；不带就取全部。"""
    since = request.args.get("since")
    msgs = load()
    if since:
        msgs = [m for m in msgs if m.get("t", "") > since]
    return jsonify(msgs)

@app.post("/api/messages")
def post_message():
    """发话。body: {"who": "小愿/克老师/朵朵", "text": "……"}"""
    data = request.get_json(force=True, silent=True) or {}
    who = (data.get("who") or "").strip()
    text = (data.get("text") or "").strip()
    if who not in WHO_OK:
        return jsonify({"error": "who 得是 小愿 / 克老师 / 朵朵 之一"}), 400
    if not text:
        return jsonify({"error": "text 不能为空"}), 400
    msg = {"who": who, "text": text, "t": _now_iso()}
    with _lock:
        msgs = load()
        msgs.append(msg)
        save(msgs)
    return jsonify(msg)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
