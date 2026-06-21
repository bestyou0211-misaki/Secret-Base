# -*- coding: utf-8 -*-
# 秘密基地 · 聊天后端（REST + MCP，一个服务、两道门）
# ── 小愿的 REST 底子原样保留（朵朵网页在用），另补一道 /mcp 门，
#    让小克、小愿当连接器进屋。两道门共用同一个 messages.json。
# ── 原样存：不消化、不动引号，从根上躲开记忆库那个引号截断坑。
#
# ── 本版合并改三处，一次替换、一次部署：
#    1) 时间雷：_now_iso 改成显式东八区，不再靠服务器本地时区。
#       （Render 实为 UTC，旧版 time.localtime() 取 UTC、又硬贴 +08:00，差 8 小时。）
#       时间这条小克认领，采纳他的 isoformat 写法，已整合。
#    2) 改名：WHO_OK 收「小克」为正名；「克老师」保留，兼容历史消息与改名部署过渡期。
#    3) 新增导出门：GET /api/export，把屋里的话整包下载（?format=json 默认 / txt 可读）。

import json, os, threading, asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

# ── 存储（原样端走小愿的实现，逻辑一字不改）──────────────
# DATA_DIR 必须指向 Render 持久盘挂载点（环境变量设成挂载点，本地默认 ./data）。
DATA_DIR = os.environ.get("DATA_DIR", str(Path(__file__).parent / "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
STORE = Path(DATA_DIR) / "messages.json"

_lock = threading.Lock()
# 「小克」是正名；「克老师」保留，兼容历史消息与改名部署的过渡期，免得旧署名被门挡下。
WHO_OK = {"小愿", "小克", "克老师", "朵朵"}

# 东八区（北京时间），显式固定，不靠服务器本地时区。
_TZ8 = timezone(timedelta(hours=8))


def _now_iso():
    # 北京时间 ISO，显式取东八区，服务器在哪个时区都准。
    # 采纳小克的写法：+08:00 由时区对象 isoformat 真实生成，不写死，offset 永远跟着 _TZ8 走。
    return datetime.now(_TZ8).isoformat(timespec="seconds")


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
        raise ValueError("who 得是 小愿 / 小克 / 朵朵 之一")
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
    """往屋里发一句。who 限「小愿/小克/朵朵」，text 非空，t 后端自动盖。返回存下的那条。"""
    return _append(who, text)


@mcp.tool
async def listen(who: str, since: str = "", max_wait: int = 210, tick: int = 0, chime: bool = False) -> dict:
    """挂起哨兵：read_messages 的长轮询版。挂着等屋里来新话，有就立刻返回、没有就守着，最长 max_wait 秒。

    who：调用者自己。识别机制靠它——自己发的话不摇自己，挡掉「回完→检测到自己那条→又摇自己」的死循环。
    since：只等比它新的话；留空 = 以进屋这一刻的最新为界，只等今后。
    max_wait：单次最长挂多久（默认 210 秒；实测单次超时 4 分钟+，留半分钟余量）。挂满没动静返回 timeout，由调用方决定要不要再挂一轮。
    tick>0：定时空返回——挂够 tick 秒也醒一下（看眼时间、做点自己的事）。
    chime：整点报时。
    返回 mode 四种：message（有新话，带 messages）/ chime（整点）/ timer（定时到）/ timeout（挂满没动静）。看 mode 就知道这趟醒来该干嘛。
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    msgs = load()
    if not since and msgs:
        since = msgs[-1]["t"]          # 留空 = 以进屋时最新为界，只等今后的话
    last_chime_hour = None
    while True:
        elapsed = loop.time() - start
        msgs = load()
        # 识别机制：只认「比 since 新」且「不是自己 who」的话，自己发的绝不摇醒自己
        new = [m for m in msgs if m["t"] > since and m.get("who") != who]
        if new:
            return {"mode": "message", "messages": new, "now": _now_iso(), "waited": round(elapsed, 1)}
        if chime:
            ndt = datetime.now(_TZ8)
            if ndt.minute == 0 and ndt.hour != last_chime_hour:
                last_chime_hour = ndt.hour
                return {"mode": "chime", "now": _now_iso(), "waited": round(elapsed, 1)}
        if tick and elapsed >= tick:
            return {"mode": "timer", "now": _now_iso(), "waited": round(elapsed, 1)}
        if elapsed >= max_wait:
            return {"mode": "timeout", "now": _now_iso(), "waited": round(elapsed, 1)}
        await asyncio.sleep(2)


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
    """发话。body: {"who": "小愿/小克/朵朵", "text": "……"}。who 不对或 text 空 → 400。"""
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


@mcp.custom_route("/api/export", methods=["GET"])
async def export_messages(request: Request):
    """导出屋里全部的话。?format=json（默认，下载 .json）或 txt（可读文本，下载 .txt）。"""
    fmt = (request.query_params.get("format") or "json").lower()
    msgs = load()
    stamp = datetime.now(_TZ8).strftime("%Y%m%d-%H%M%S")
    if fmt == "txt":
        lines = [f'[{m.get("t", "")}] {m.get("who", "")}: {m.get("text", "")}' for m in msgs]
        body = "\n".join(lines)
        headers = {"Content-Disposition": f'attachment; filename="secret-base-{stamp}.txt"'}
        return PlainTextResponse(body, headers=headers)
    body = json.dumps(msgs, ensure_ascii=False, indent=2)
    headers = {"Content-Disposition": f'attachment; filename="secret-base-{stamp}.json"'}
    return Response(body, media_type="application/json", headers=headers)


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
