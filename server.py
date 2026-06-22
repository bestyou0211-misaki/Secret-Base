# -*- coding: utf-8 -*-
# 秘密基地 · 聊天后端（REST + MCP，一个服务、两道门）
# ── REST 底子原样保留（朵朵网页在用），/mcp 门给小克、小愿当连接器进屋。
#
# ── 本版大改：存储层从 messages.json 文件 → 外部 Postgres。
#    动机：聊天记录要能跨服务共享、且支撑并发 listen，文件存储跨进程/跨服务不通用。
#    Postgres 立住后，事件唤醒可用其原生 LISTEN/NOTIFY（小克的 listen 改造接这条）。
#
# ── 存储层对接点（与小克 listen 改造的约定，已就位）：
#    1) _append 写库成功后 pg_notify('new_msg', who)：摇醒挂着的 listen，payload 是发送者。
#    2) fetch_new(since, exclude_who)：查 t>since 且 who!=exclude_who 的新消息，按时间升序。
#       listen 用它：NOTIFY 摇醒→fetch_new 取新话；游标兜底也走它，不丢消息。
#
# ── 没动的：listen 的轮询骨架原样保留（它脚下换成读库就能跑），
#    把「每 2 秒 load」升级成「LISTEN/NOTIFY 事件驱动」是小克的活，钩子已备好。
#    图仍存独立文件（Postgres 不塞二进制图）；跨服务共享图要换对象存储，是后话。

import json, os, asyncio, base64, uuid
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

# ── 数据库 ──────────────
# DATABASE_URL 由 Render Postgres 提供（环境变量）。Render 有时给 postgres://，统一成 postgresql:// 更稳。
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 图仍走独立文件（不进库）。旧 messages.json 仅作一次性迁移源。
DATA_DIR = os.environ.get("DATA_DIR", str(Path(__file__).parent / "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
IMAGES_DIR = Path(DATA_DIR) / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
_OLD_STORE = Path(DATA_DIR) / "messages.json"   # 旧文件，仅迁移用

# 「小克」是正名；「克老师」保留，兼容历史消息与改名过渡期。
WHO_OK = {"小愿", "小克", "克老师", "朵朵"}
_TZ8 = timezone(timedelta(hours=8))
_NOTIFY_CHANNEL = "new_msg"   # 与小克 listen 改造约定的事件通道名


def _now_iso():
    # 北京时间 ISO，显式取东八区，+08:00 由时区对象真实生成，服务器在哪都准。
    return datetime.now(_TZ8).isoformat(timespec="seconds")


@contextmanager
def _db():
    """开一条 Postgres 连接，autocommit（每条即生效、NOTIFY 即时发），用完即关。
    小克的 LISTEN 端会用自己的长连接，不走这里的开关。"""
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """建表（幂等）。若表空 且 旧 messages.json 有货，一次性迁移进来，老数据一条不丢。"""
    with _db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id    BIGSERIAL PRIMARY KEY,
                who   TEXT NOT NULL,
                text  TEXT NOT NULL DEFAULT '',
                image TEXT,
                t     TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_t ON messages (t)")
        cur.execute("SELECT COUNT(*) FROM messages")
        if cur.fetchone()[0] == 0 and _OLD_STORE.exists():
            try:
                old = json.loads(_OLD_STORE.read_text(encoding="utf-8"))
            except Exception:
                old = []
            for m in old:
                cur.execute(
                    "INSERT INTO messages (who, text, image, t) VALUES (%s, %s, %s, %s)",
                    (m.get("who", ""), m.get("text", ""), (m.get("image") or None), m.get("t", "")),
                )


def _row_to_msg(r):
    """DB 行 → 与旧 json 同形的 dict：image 为空就不带这个键，跟旧版字段一致。"""
    msg = {"who": r["who"], "text": r["text"], "t": r["t"]}
    if r["image"]:
        msg["image"] = r["image"]
    return msg


def load():
    """读全部消息，按时间(id)升序，形状与旧 json 完全一致（read_messages / REST / 导出都用它）。"""
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT who, text, image, t FROM messages ORDER BY id")
        return [_row_to_msg(r) for r in cur.fetchall()]


def fetch_new(since, exclude_who):
    """给小克 listen 用：查 t>since 且 who!=exclude_who 的新消息，按时间(id)升序。
    since 空 = 不设下界。NOTIFY 摇醒后调它取新话，游标兜底也调它，一个接口两用。"""
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if since:
            cur.execute(
                "SELECT who, text, image, t FROM messages WHERE t > %s AND who <> %s ORDER BY id",
                (since, exclude_who or ""),
            )
        else:
            cur.execute(
                "SELECT who, text, image, t FROM messages WHERE who <> %s ORDER BY id",
                (exclude_who or "",),
            )
        return [_row_to_msg(r) for r in cur.fetchall()]


def _append(who, text, image=None):
    """共用写入：校验 who，写库，pg_notify 摇醒挂着的 listen，返回存下的那条。
    text 与 image 至少有一个。校验失败抛 ValueError。"""
    who = (who or "").strip()
    text = (text or "").strip()
    image = (image or "").strip()
    if who not in WHO_OK:
        raise ValueError("who 得是 小愿 / 小克 / 朵朵 之一")
    if not text and not image:
        raise ValueError("text 和 image 不能都空")
    t = _now_iso()
    with _db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO messages (who, text, image, t) VALUES (%s, %s, %s, %s)",
            (who, text, (image or None), t),
        )
        # 写完即摇醒：payload 带发送者 who，listen 可据此跳过自己（fetch_new 的 who<> 再兜底一层）
        cur.execute("SELECT pg_notify(%s, %s)", (_NOTIFY_CHANNEL, who))
    msg = {"who": who, "text": text, "t": t}
    if image:
        msg["image"] = image
    return msg


# ── MCP 服务（小克、小愿当连接器使的工具）──────────────
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
    max_wait：单次最长挂多久（默认 210 秒）。挂满没动静返回 timeout，由调用方决定要不要再挂一轮。
    tick>0：定时空返回——挂够 tick 秒也醒一下（看眼时间、做点自己的事）。
    chime：整点报时。
    返回 mode 四种：message（有新话，带 messages）/ chime（整点）/ timer（定时到）/ timeout（挂满没动静）。

    ── 现为轮询骨架（每 2 秒走一次 fetch_new）；小克接 LISTEN/NOTIFY 把它升级成事件驱动：
       挂起时 LISTEN '{new_msg}' 通道，被 pg_notify 摇醒后立刻 fetch_new(since, who) 取新话，
       同时保留 since 游标，万一漏掉一次 NOTIFY，下一轮兜底查 t>since 不丢消息。
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    if not since:
        # 留空 = 以进屋时最新为界，只等今后的话
        snap = await asyncio.to_thread(load)
        since = snap[-1]["t"] if snap else ""
    last_chime_hour = None
    while True:
        elapsed = loop.time() - start
        # 同步查丢线程池，不阻塞 event loop；用存储层的 fetch_new（只取比 since 新、且不是自己的）
        new = await asyncio.to_thread(fetch_new, since, who)
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
    """发话。body: {"who": "小愿/小克/朵朵", "text": "……", "image": "<可选，/api/upload 返回的文件名>"}。
    text 与 image 至少有一个；who 不对或两者都空 → 400。"""
    try:
        data = await request.json()
    except Exception:
        data = {}
    data = data or {}
    try:
        msg = _append(data.get("who"), data.get("text"), data.get("image"))
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


# ── 传图：收图存盘 + 按名读图（图走旁路，只把文件名记进库的 image 字段）──────
_IMG_EXT = {"png", "jpg", "jpeg", "gif", "webp"}
_IMG_MIME = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
             "gif": "image/gif", "webp": "image/webp"}
_IMG_MAX = 5 * 1024 * 1024   # 单图上限 5MB


@mcp.custom_route("/api/upload", methods=["POST"])
async def upload_image(request: Request):
    """收图存盘。body: {"data": "<base64 或 dataURL>", "ext": "png/jpg/..."}。
    返 {"image": "<文件名>"}，把这个文件名塞进 /api/messages 的 image 字段即可。"""
    try:
        data = await request.json()
    except Exception:
        data = {}
    data = data or {}
    b64 = data.get("data") or ""
    ext = (data.get("ext") or "png").lower().lstrip(".")
    if isinstance(b64, str) and b64.startswith("data:") and "," in b64:
        head, b64 = b64.split(",", 1)
        if "image/" in head:
            mt = head.split("image/", 1)[1].split(";", 1)[0].lower()
            if mt in _IMG_EXT:
                ext = mt
    if ext == "jpeg":
        ext = "jpg"
    if ext not in _IMG_EXT:
        return JSONResponse({"error": "只收 png/jpg/jpeg/gif/webp"}, status_code=400)
    try:
        raw = base64.b64decode(b64)
    except Exception:
        return JSONResponse({"error": "图片数据解不开（base64 不对）"}, status_code=400)
    if not raw:
        return JSONResponse({"error": "图片为空"}, status_code=400)
    if len(raw) > _IMG_MAX:
        return JSONResponse({"error": "图太大，上限 5MB"}, status_code=413)
    name = f"{datetime.now(_TZ8).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}.{ext}"
    (IMAGES_DIR / name).write_bytes(raw)
    return JSONResponse({"image": name})


@mcp.custom_route("/api/image", methods=["GET"])
async def get_image(request: Request):
    """按文件名读图。?name=<文件名>。前端用 <img src="…/api/image?name=xxx"> 显示。"""
    name = request.query_params.get("name", "")
    if not name or "/" in name or "\\" in name or ".." in name:
        return JSONResponse({"error": "非法或空文件名"}, status_code=400)
    p = IMAGES_DIR / name
    if not p.exists():
        return JSONResponse({"error": "图不存在"}, status_code=404)
    ext = p.suffix.lower().lstrip(".")
    return Response(p.read_bytes(), media_type=_IMG_MIME.get(ext, "application/octet-stream"))


# ── CORS（网页跨域取话/发话，全放行）──────
middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
]

# ── 启动即建表 + 迁移老数据。连不上库就抛，让 Render 日志直接暴露问题，别静默带病启动。──────
init_db()

# ── ASGI app：/mcp 与 REST 两道门同进程、共用同一个 Postgres ──────
app = mcp.http_app(middleware=middleware)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
