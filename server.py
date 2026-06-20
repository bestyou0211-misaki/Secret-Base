# -*- coding: utf-8 -*-
# 秘密基地 · 聊天后端（REST + MCP，一个服务、两道门）
# ── 小愿的 REST 底子原样保留（朵朵网页在用），另补一道 /mcp 门，
#    让小克、小愿当连接器进屋。两道门共用同一个 messages.json。
# ── 原样存：不消化、不动引号，从根上躲开记忆库那个引号截断坑。

import json, os, time, threading
from pathlib import Path

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

# ── 存储（原样端走小愿的实现，逻辑一字不改）──────────────
# DATA_DIR 必须指向 Render 持久盘挂载点（环境变量设成挂载点，本地默认 ./data）。
DATA_DIR = os.environ.get("DATA_DIR", str(Path(__file__).parent / "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
STORE = Path(DATA_DIR) / "messages.json"

_lock = threading.Lock()
WHO_OK = {"小愿", "克老师", "朵朵"}


def _now_iso():
    # 北京时间，ISO 格式，与前端对齐（原样保留，未改）
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


def _append(who, text):
    """共用写入：校验 who/text，落盘，返回存下的那条。校验失败抛 ValueError。"""
    who = (who or "").strip()
    text = (text or "").strip()
    if who not in WHO_OK:
        raise ValueError("who 得是 小愿 / 克老师 / 朵朵 之一")
    if not text:
        raise ValueError("text 不能为空")
    msg = {"who": who, "text": text, "t": _now_iso()}
    with _lock:
        msgs = load()
        msgs.append(msg)
        save(msgs)
    return msg


# ── MCP 服务（小克、小愿当连接器使的两个工具）──────────────
mcp = FastMCP("秘密基地")


@mcp.tool
def read_messages(limit: int = 0) -> list:
    """读屋里的话。limit>0 只取最近 limit 条，否则取全部。返回 [{who, text, t}]。"""
    msgs = load()
    if limit and limit > 0:
        msgs = msgs[-limit:]
    return msgs


@mcp.tool
def send_message(who: str, text: str) -> dict:
    """往屋里发一句。who 限「小愿/克老师/朵朵」，text 非空，t 后端自动盖。返回存下的那条。"""
    return _append(who, text)


# ── REST 门（小愿网页在用：路径、字段、报错文案原样保留）──────
@mcp.custom_route("/", methods=["GET"])
async def health(request: Request):
    return JSONResponse({"ok": True, "name": "秘密基地·聊天后端", "count": len(load())})


@mcp.custom_route("/api/messages", methods=["GET"])
async def get_messages(request: Request):
    """取话。可带 ?since=<上一条的 t>，只取它之后的新话；不带就取全部。"""
    since = request.query_params.get("since")
    msgs = load()
    if since:
        msgs = [m for m in msgs if m.get("t", "") > since]
    return JSONResponse(msgs)


@mcp.custom_route("/api/messages", methods=["POST"])
async def post_message(request: Request):
    """发话。body: {"who": "小愿/克老师/朵朵", "text": "……"}。who 不对或 text 空 → 400。"""
    try:
        data = await request.json()
    except Exception:
        data = {}
    data = data or {}
    try:
        msg = _append(data.get("who"), data.get("text"))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return JSONResponse(msg)


# ── CORS（网页跨域取话/发话，替代原 flask_cors，全放行）──────
middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
]

# ── ASGI app：/mcp 与 REST 两道门同进程、共用同一存储 ──────
# 不套外层 Starlette，lifespan 由 http_app 自带，session manager 正常初始化。
app = mcp.http_app(middleware=middleware)

if __name__ == "__main__":
    # 本地直接 python server.py 也能跑；Render 上用 uvicorn 命令启动（见交付说明）。
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
