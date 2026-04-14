import os
import json
import asyncio
import html
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from io import BytesIO

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import httpx
import pytz
import redis.asyncio as aioredis
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import PlainTextResponse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
TOKEN        = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379")
OWNER_ID = int(os.getenv("OWNER_ID"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

MAINTENANCE_MESSAGE = (
    "🌿 <b>The Enchanted Clearing is currently under maintenance</b>\n\n"
    "The ancient trees are resting and being prepared for new wonders...\n\n"
    "We will be back very soon with a smoother experience!\n\n"
    "<i>Thank you for your patience, kind wanderer.</i> 🍃✨"
)

CACHE_TTL            = 300
NETFLIX_ITEMS_PER_PAGE = 8
MAX_DAILY_REVEALS = 999

DISPLAY_NAME_MAP = {
    "netflix": "Netflix Cookie",
    "prime":   "PrimeVideo Cookie",
    "office":  "Office Key",
    "windows": "Win Key",
    "win":     "Win Key",
}

# ──────────────────────────────────────────────
# RENDER COLD-START DETECTION (free tier spin-down)
# ──────────────────────────────────────────────
COLD_START = True

# ──────────────────────────────────────────────
# REFERRAL CONFIG
# ──────────────────────────────────────────────
MAX_REFERRALS_PER_DAY = 8
REFERRAL_XP = 25
NEW_USER_WELCOME_BONUS_IF_REFERRED = 40

# ──────────────────────────────────────────────
# COOLDOWN CONFIG
# ──────────────────────────────────────────────
COOLDOWN_SECONDS = {
    "view_windows":  10,
    "view_office":   10,
    "view_netflix":  10,
    "view_prime":    10,
    "reveal_netflix": 18,
    "reveal_prime":   18,
    "profile":       12,
    "clear":         20,
    "guidance":      30,
    "lore":          30,
    "general":        5,
}

EVENT_BONUS_TIERS = {
    "netflix_double": {1: 2, 2: 6, 3: 6, 4: 8,  5: 8,  6: 14, 7: 18, 8: 24, 9: 30},
    "netflix_max":    {1: 5, 2: 8, 3: 8, 4: 12, 5: 12, 6: 16, 7: 22, 8: 28, 9: 35},
}

# ══════════════════════════════════════════════════════════════════════════════
# ATOMIC REVEAL CAP (Lua script — prevents TOCTOU race)
# ══════════════════════════════════════════════════════════════════════════════
REVEAL_CAP_SCRIPT = """
local key = KEYS[1]
local max_reveals = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local current = redis.call("INCR", key)

-- First time this key is created today → set proper TTL
if current == 1 then
    redis.call("EXPIRE", key, ttl)
end

-- Cap exceeded → rollback and return 0
if current > max_reveals then
    redis.call("DECR", key)
    return 0
end

return current
"""

limiter = Limiter(key_func=get_remote_address)

# ──────────────────────────────────────────────
# GIFS
# ──────────────────────────────────────────────
WELCOME_GIF   = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExeWZzOHRrYjRycTI4d2Z2eXR6bWNiMm1yYXVqbzVrb3NmczB2ZHdmayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/wsKqNQmHYZfs4/giphy.gif"
MENU_GIF      = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExczJsZ25kM2N1N2twOHhmNWRsd3N6eWlyZ3N5M29pdmxsdDMzOHVscCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"
INVENTORY_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM1OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
ABOUT_GIF     = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdTFqMHB0ODVxdmFoMHl3dzZyM2swanlicmRibGk1bjdpcjFsdnl1biZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/x5HlLDaLMZNVS/giphy.gif"
HELP_GIF      = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNWxybTY5bXA0ejg1cGxxNTY3d3IyY3A4NGtkZ2gyOXkxcnlwZzN2NCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/J4FsxFgZgN2HS/giphy.gif"
LOADING_GIF   = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
MYID_GIF      = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM1OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
CLEAN_GIF     = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
GUIDANCE_GIF  = "https://64.media.tumblr.com/129ee065eff5fee81fab81c4f8e2ed4f/tumblr_oui1cvflgE1r9i2iuo1_r7_540.gif"
HELLO_GIF     = "https://i.pinimg.com/originals/6a/a3/7f/6aa37fd0017bdb291ca8cbdd8b0ede52.gif"
CARETAKER_GIF = "https://i.pinimg.com/originals/86/d1/25/86d1259e1a62106509575ef75e9aeb09.gif"
INVITE_GIF = "https://images.gr-assets.com/hostedimages/1489696457ra/22241153.gif"
# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL SINGLETONS  (initialised in lifespan, never re-created)
# ══════════════════════════════════════════════════════════════════════════════
tg_app: Application = None
http:   httpx.AsyncClient = None
redis:  aioredis.Redis = None
db_sem  = asyncio.Semaphore(10)   # cap concurrent Supabase calls
forest_memory: dict[int, list[int]] = {}  # fallback only — Redis is primary
BOT_START_TIME = datetime.now(pytz.utc)
BOT_USERNAME: str | None = None

# ──────────────────────────────────────────────
# LIFESPAN  (replaces @app.on_event)
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg_app, http, redis

    # Shared HTTP connection pool (20 connections max)
    http = httpx.AsyncClient(
        timeout=12.0,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )

    # Redis for cooldowns + VAMT cache
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    # Telegram application
    tg_app = Application.builder().token(TOKEN).build()
    await tg_app.initialize()
    await tg_app.start()

    # ── ADD THESE 4 LINES ──
    global BOT_USERNAME
    me = await tg_app.bot.get_me()
    BOT_USERNAME = me.username
    print(f"✅ Bot username cached: @{BOT_USERNAME}")

    print("✅ Bot started — FastAPI lifespan ready")
    yield

    # ── teardown ──
    await tg_app.stop()
    await tg_app.shutdown()
    await http.aclose()
    await redis.aclose()
    print("🌿 Bot shut down cleanly")

app = FastAPI(lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ══════════════════════════════════════════════════════════════════════════════
# FASTAPI ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/keepalive")
async def health():
    return PlainTextResponse("🌿 Clyde's Enchanted Clearing is awake.")

@app.post("/")
@limiter.limit("60/minute")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if WEBHOOK_SECRET and token != WEBHOOK_SECRET:
        return PlainTextResponse("Forbidden", status_code=403)

    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 1_000_000:
        return PlainTextResponse("Payload too large", status_code=413)

    data = await request.json()
    if not data:
        return PlainTextResponse("No data", status_code=400)
    background_tasks.add_task(process_update, data)
    return PlainTextResponse("OK")

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _supabase_headers(extra: dict | None = None) -> dict:
    h = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if extra:
        h.update(extra)
    return h


async def _sb_get(path: str, **params) -> list | dict | None:
    """Safe Supabase GET with semaphore + timeout."""
    async with db_sem:
        try:
            r = await asyncio.wait_for(
                http.get(f"{SUPABASE_URL}/rest/v1/{path}",
                         headers=_supabase_headers(), params=params),
                timeout=10.0,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"🔴 SB GET {path}: {e}")
            return None


async def _sb_post(path: str, payload: dict | list) -> bool:
    async with db_sem:
        try:
            r = await asyncio.wait_for(
                http.post(
                    f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=_supabase_headers({
                        "Content-Type": "application/json",
                        "Prefer":       "return=minimal",
                    }),
                    json=payload,
                ),
                timeout=10.0,
            )
            # ✅ FIX: log the actual status so you can see what's happening
            print(f"🟢 SB POST {path}: status={r.status_code}")
            return r.status_code in (200, 201)
        except Exception as e:
            print(f"🔴 SB POST {path}: {e}")
            return False
        
async def _sb_upsert(path: str, payload: dict | list, on_conflict: str) -> bool:
    """Upsert with FULL error logging when it fails"""
    async with db_sem:
        try:
            data = [payload] if isinstance(payload, dict) else payload

            r = await asyncio.wait_for(
                http.post(
                    f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=_supabase_headers({
                        "Content-Type": "application/json",
                        "Prefer": "resolution=merge-duplicates,return=minimal",
                    }),
                    params={"on_conflict": on_conflict},
                    json=data,
                ),
                timeout=10.0,
            )

            print(f"🟢 SB UPSERT {path}: status={r.status_code}")

            if r.status_code not in (200, 201):
                error_body = await r.text() if r.text else "No body"
                print(f"🔴 SB UPSERT ERROR {r.status_code}: {error_body}")
                return False

            return True

        except Exception as e:
            print(f"🔴 SB UPSERT {path} EXCEPTION: {e}")
            return False

async def _sb_patch(path: str, payload: dict) -> bool:
    async with db_sem:
        try:
            r = await asyncio.wait_for(
                http.patch(
                    f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=_supabase_headers({
                        "Content-Type": "application/json",
                        "Prefer":       "return=minimal",
                    }),
                    json=payload,
                ),
                timeout=10.0,
            )
            # ✅ FIX: 204 is success — don't call .json() on empty body
            return r.status_code in (200, 204)
        except Exception as e:
            print(f"🔴 SB PATCH {path}: {e}")
            return False


async def _sb_delete(path: str) -> bool:
    async with db_sem:
        try:
            r = await asyncio.wait_for(
                http.delete(
                    f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=_supabase_headers(),
                ),
                timeout=10.0,
            )
            return r.status_code in (200, 204)
        except Exception as e:
            print(f"🔴 SB DELETE {path}: {e}")
            return False


# ──────────────────────────────────────────────
# VAMT CACHE  (Redis)
# ──────────────────────────────────────────────
async def get_vamt_data() -> list | None:
    cached = await redis.get("vamt_cache")
    if cached:
        print("⚡ [CACHE] VAMT from Redis")
        return json.loads(cached)

    print("📡 [SUPABASE] Fetching fresh VAMT data…")
    data = await _sb_get("vamt_keys", select="*", order="service_type.asc")
    if data is not None:
        await redis.setex("vamt_cache", CACHE_TTL, json.dumps(data))
        print(f"✅ VAMT cached — {len(data)} items")
    else:
        print("🔴 Supabase returned nothing for vamt_keys")
    return data


async def send_supabase_error(chat_id: int, query=None):
    """
    Shows a friendly error to the user when Supabase is unreachable.
    Works for both regular messages and callback queries.
    """
    msg = (
        "🌫️ <b>The forest is currently unreachable...</b>\n\n"
        "The ancient trees seem to be resting deeply.\n\n"
        "⚠️ Our database is temporarily unavailable.\n"
        "Please try again in a few moments.\n\n"
        "<i>The clearing will be back soon.</i> 🍃"
    )
    try:
        if query:
            # inside a button callback
            await query.answer(
                "🌫️ The forest is unreachable right now. Please try again shortly.",
                show_alert=True,
            )
        else:
            # regular command
            await tg_app.bot.send_message(chat_id, msg, parse_mode="HTML")
    except Exception:
        pass

async def handle_flushcache(chat_id: int):
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can refresh the forest cache.")
        return

    deleted = await redis.delete("vamt_cache")
    if deleted:
        await tg_app.bot.send_message(
            chat_id,
            "✅ <b>Forest Cache Cleared!</b>\n\n"
            "🌿 The VAMT cache has been flushed.\n"
            "Next inventory request will fetch fresh data from Supabase.\n\n"
            "<i>New cookies will now appear immediately.</i> 🍃",
            parse_mode="HTML",
        )
    else:
        await tg_app.bot.send_message(
            chat_id,
            "ℹ️ <b>Cache was already empty.</b>\n\n"
            "🌿 No cached data found — Supabase is already being hit directly.",
            parse_mode="HTML",
        )

# ══════════════════════════════════════════════════════════════════════════════
# ADMIN KEY UPLOAD (TXT → Supabase vamt_keys)
# ══════════════════════════════════════════════════════════════════════════════
async def handle_uploadkeys_command(chat_id: int):
    """Command /uploadkeys — tells admin how to format the file"""
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can upload keys.")
        return

    msg = (
        "📤 <b>Upload Keys via TXT</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Send me a <b>.txt</b> file now.\n\n"
        "<b>Supported formats:</b>\n"
        "1. Simple (one key per line):\n"
        "<code>ABCDE-FGHIJ-KLMNO-PQRST-UVWXY\n"
        "NETFLIX-COOKIE-1234567890</code>\n\n"
        "2. Advanced (with details):\n"
        "<code>KEY_HERE|50|windows|Windows 11 Pro\n"
        "cookie123|999|netflix|Netflix Premium</code>\n\n"
        "• Lines starting with # are ignored\n"
        "• remaining defaults to 999 if not specified\n"
        "• service_type examples: windows, office, netflix, prime, steam\n\n"
        "<i>Just send the .txt file now — I’ll import it instantly.</i> 🍃"
    )
    await tg_app.bot.send_message(chat_id, msg, parse_mode="HTML")

def detect_service_type(content: str, filename: str) -> tuple[str, str]:
    """
    Auto-detects service_type and display_name enum from file content/filename.
    Returns (service_type, display_name_enum)
    """
    content_lower = content.lower()
    filename_lower = filename.lower()

    # ── Netflix ──
    if (
        "netflixid" in content_lower or
        "netflix.com" in content_lower or
        "netflix" in filename_lower
    ):
        return "netflix", "Netflix Cookie"

    # ── PrimeVideo ──
    if (
        "primevideo" in content_lower or
        "amazon" in content_lower or
        "prime" in filename_lower or
        "prime" in content_lower
    ):
        return "prime", "PrimeVideo Cookie"

    # ── Office ──
    if (
        "office" in filename_lower or
        "microsoft office" in content_lower or
        "office" in content_lower
    ):
        return "office", "Office Key"

    # ── Windows ──
    if (
        "windows" in filename_lower or
        "win" in filename_lower or
        "windows" in content_lower
    ):
        return "windows", "Win Key"

    # ── Fallback ──
    return "unknown", "Netflix Cookie"

async def parse_and_import_keys(content: str, filename: str = "unknown.txt") -> tuple[int, int, list[str]]:
    imported = 0
    skipped = 0
    errors = []

    detected_service, detected_display = detect_service_type(content, filename)

    # ══════════════════════════════════════
    # FORMAT 1 — Cookie file with header block
    # (any stealer format, any service)
    # ══════════════════════════════════════
    is_cookie_file = (
        "NetflixId" in content or
        "SecureNetflixId" in content or
        "hacked by" in content.lower() or
        "stealer" in content.lower() or
        ("# copy the cookies from here" in content.lower()) or
        # Amazon/Prime detection
        ("amazon.com" in content.lower() and "ubid-main" in content.lower()) or
        ("primevideo.com" in content.lower())
    )

    if is_cookie_file:
        # Try to extract only the cookie block
        if "# copy the cookies from here" in content.lower():
            # Find the marker regardless of exact spacing/casing
            import re
            match = re.split(r"#\s*copy the cookies from here\s*:?", content, flags=re.IGNORECASE)
            cookie_block = match[-1].strip() if len(match) > 1 else content.strip()
        else:
            # No marker — use entire content as cookie
            cookie_block = content.strip()

        if not cookie_block:
            errors.append(f"❌ {filename}: Cookie block was empty after extraction")
            return 0, 1, errors

        payload = {
            "key_id":       cookie_block,
            "remaining":    999,
            "service_type": detected_service,
            "status":       "active",
            "display_name": detected_display,
        }

        success = await _sb_upsert("vamt_keys", payload, on_conflict="key_id")
        if success:
            imported += 1
        else:
            skipped += 1
            errors.append(
                f"❌ Cookie file skipped\n"
                f"   File: {filename}\n"
                f"   Service: {detected_service}\n"
                f"   Reason: Supabase upsert rejected"
            )
        return imported, skipped, errors

    # ══════════════════════════════════════
    # FORMAT 2 — JSON format
    # {"email": "x@x.com", "password": "pass"}
    # ══════════════════════════════════════
    stripped = content.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            import json
            data = json.loads(stripped)
            if isinstance(data, dict):
                data = [data]

            for i, item in enumerate(data):
                # Try to build a cookie-like string from key:value pairs
                key_id = "\n".join(f"{k}: {v}" for k, v in item.items())
                payload = {
                    "key_id":       key_id,
                    "remaining":    int(item.get("remaining", 999)),
                    "service_type": item.get("service_type", detected_service),
                    "status":       "active",
                    "display_name": detected_display,
                }
                success = await _sb_upsert("vamt_keys", payload, on_conflict="key_id")
                if success:
                    imported += 1
                else:
                    skipped += 1
                    errors.append(f"❌ JSON item {i+1} skipped: Supabase rejected")
        except Exception as e:
            errors.append(f"❌ JSON parse failed: {str(e)[:100]}")
            skipped += 1
        return imported, skipped, errors

    # ══════════════════════════════════════
    # FORMAT 3 — Key:Value block format
    # Email: x@x.com
    # Password: pass123
    # Plan: Premium
    # ══════════════════════════════════════
    lines = [l.strip() for l in content.splitlines() if l.strip() and not l.startswith("#")]
    is_keyvalue = sum(1 for l in lines if ":" in l) > len(lines) * 0.6  # >60% lines have ":"

    if is_keyvalue and not any("|" in l for l in lines):
        # Group into blocks separated by blank lines
        blocks = []
        current = []
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                if current:
                    blocks.append(current)
                    current = []
            elif not line.startswith("#"):
                current.append(line)
        if current:
            blocks.append(current)

        if not blocks:
            blocks = [lines]  # treat whole file as one block

        for i, block in enumerate(blocks):
            key_id = "\n".join(block)
            if not key_id:
                continue
            payload = {
                "key_id":       key_id,
                "remaining":    999,
                "service_type": detected_service,
                "status":       "active",
                "display_name": detected_display,
            }
            success = await _sb_upsert("vamt_keys", payload, on_conflict="key_id")
            if success:
                imported += 1
            else:
                skipped += 1
                errors.append(f"❌ Block {i+1} skipped: Supabase rejected")
        return imported, skipped, errors

    # ══════════════════════════════════════
    # FORMAT 4 — CSV format
    # key_id,service,remaining
    # ABCDE,windows,50
    # ══════════════════════════════════════
    if "," in content and "\n" in content:
        csv_lines = [l.strip() for l in content.splitlines() if l.strip() and not l.startswith("#")]
        first_line = csv_lines[0].lower() if csv_lines else ""
        has_header = "key" in first_line or "service" in first_line or "remaining" in first_line
        data_lines = csv_lines[1:] if has_header else csv_lines

        SERVICE_MAP = {
            "netflix":    ("netflix",  "Netflix Cookie"),
            "prime":      ("prime",    "PrimeVideo Cookie"),
            "primevideo": ("prime",    "PrimeVideo Cookie"),
            "office":     ("office",   "Office Key"),
            "windows":    ("windows",  "Win Key"),
            "win":        ("windows",  "Win Key"),
        }

        all_pipe = all("|" not in l and "," in l for l in data_lines[:5])
        if all_pipe or has_header:
            for line_num, line in enumerate(data_lines, 1):
                parts = [p.strip() for p in line.split(",")]
                if not parts[0]:
                    continue
                key_id    = parts[0]
                remaining = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 999
                raw_svc   = parts[2].lower() if len(parts) > 2 else detected_service
                svc, disp = SERVICE_MAP.get(raw_svc, (detected_service, detected_display))

                payload = {
                    "key_id":       key_id,
                    "remaining":    remaining,
                    "service_type": svc,
                    "status":       "active",
                    "display_name": disp,
                }
                success = await _sb_upsert("vamt_keys", payload, on_conflict="key_id")
                if success:
                    imported += 1
                else:
                    skipped += 1
                    errors.append(f"❌ CSV line {line_num} skipped: {key_id[:30]}")
            return imported, skipped, errors

    # ══════════════════════════════════════
    # FORMAT 5 — Pipe separated OR plain keys
    # KEY|remaining|service|display
    # or just: ABCDE-FGHIJ-KLMNO
    # ══════════════════════════════════════
    SERVICE_MAP = {
        "netflix":    ("netflix",  "Netflix Cookie"),
        "prime":      ("prime",    "PrimeVideo Cookie"),
        "primevideo": ("prime",    "PrimeVideo Cookie"),
        "office":     ("office",   "Office Key"),
        "windows":    ("windows",  "Win Key"),
        "win":        ("windows",  "Win Key"),
    }

    for line_num, raw_line in enumerate(content.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue

        try:
            if "|" in line:
                parts        = [p.strip() for p in line.split("|")]
                key_id       = parts[0]
                remaining    = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 999
                raw_svc      = parts[2].lower() if len(parts) > 2 else detected_service
                svc, disp    = SERVICE_MAP.get(raw_svc, (detected_service, detected_display))
            else:
                key_id    = line
                remaining = 999
                svc       = detected_service
                disp      = detected_display

            if not key_id:
                continue

            payload = {
                "key_id":       key_id,
                "remaining":    remaining,
                "service_type": svc,
                "status":       "active",
                "display_name": disp,
            }

            success = await _sb_upsert("vamt_keys", payload, on_conflict="key_id")
            if success:
                imported += 1
            else:
                skipped += 1
                errors.append(
                    f"❌ Line {line_num} skipped\n"
                    f"   Key: {key_id[:30]}...\n"
                    f"   Service: {svc}\n"
                    f"   Reason: Supabase upsert rejected"
                )

        except Exception as e:
            skipped += 1
            errors.append(
                f"❌ Line {line_num} skipped\n"
                f"   Raw: {line[:30]}...\n"
                f"   Error: {str(e)[:80]}"
            )

    return imported, skipped, errors

async def handle_document(update: Update):
    """Handles TXT file upload from admin — now supports Netflix cookie files"""
    message = update.message
    chat_id = message.chat_id
    if chat_id != OWNER_ID:
        return

    document = message.document
    if not document or document.mime_type != "text/plain":
        await message.reply_text("❌ Only .txt files are allowed.")
        return

    filename = document.file_name or "unknown.txt"

    try:
        file = await document.get_file()
        file_bytes = await file.download_as_bytearray()
        content = file_bytes.decode("utf-8")
    except Exception as e:
        await message.reply_text(f"❌ Failed to download file: {e}")
        return

    loading = await message.reply_animation(
        animation=LOADING_GIF,
        caption="🌿 <i>Planting new keys in the ancient library...</i>",
        parse_mode="HTML"
    )

    imported, skipped, errors = await parse_and_import_keys(content, filename)

    await redis.delete("vamt_cache")   # auto-refresh inventory

    result = (
        f"✅ <b>Keys Imported Successfully!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"🌱 <b>Imported:</b> {imported} keys\n"
        f"📦 <b>Skipped:</b> {skipped} keys\n\n"
        f"🧹 Redis cache flushed — inventory is now fresh!\n\n"
    )
    if skipped > 0 and errors:
        result += f"⚠️ <b>Skipped Details:</b>\n"
        for err in errors[:10]:  # show max 10
            result += f"• {err}\n"

    await loading.edit_caption(result, parse_mode="HTML")

    if errors:
        await tg_app.bot.send_message(OWNER_ID, f"🔴 Upload issues:\n\n" + "\n".join(errors[:10]), parse_mode="HTML")

# ──────────────────────────────────────────────
# MAINTENANCE_MODE CONFIG
# ──────────────────────────────────────────────
async def get_maintenance_mode() -> bool:
    val = await redis.get("maintenance_mode")
    return val == "1"

async def set_maintenance_mode(enabled: bool):
    await redis.set("maintenance_mode", "1" if enabled else "0")

async def has_seen_winoffice_guide(chat_id: int) -> bool:
    # Returns True if this user has already seen the guide
    return bool(await redis.get(f"seen_winoffice_guide:{chat_id}"))

async def mark_winoffice_guide_seen(chat_id: int):
    # Permanently marks guide as seen — no TTL, never expires
    await redis.set(f"seen_winoffice_guide:{chat_id}", 1)

# ──────────────────────────────────────────────
# BOT CONFIG
# ──────────────────────────────────────────────
async def get_bot_config() -> dict:
    data = await _sb_get("bot_info", select="*", limit=1)
    if data is None:
        return {"current_version": "1.3.2", "last_updated": "Not set yet"}
    return data[0] if data else {"current_version": "1.3.2", "last_updated": "Not set yet"}


async def set_bot_info(new_version: str, custom_datetime: str, chat_id: int):
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can change forest info.")
        return

    ok = await _sb_post(
        "bot_info",
        [{"id": 1, "current_version": new_version.strip(), "last_updated": custom_datetime.strip()}],
    )
    msg = (
        f"✅ <b>Forest info updated!</b>\n\n📜 Version: <b>{new_version}</b>\n🔄 Last Updated: <b>{custom_datetime}</b>"
        if ok else "❌ Failed to save."
    )
    await tg_app.bot.send_message(chat_id, msg, parse_mode="HTML")


# ──────────────────────────────────────────────
# UPTIME
# ──────────────────────────────────────────────
async def get_bot_uptime() -> str:
    try:
        config = await get_bot_config()
        raw = config.get("last_updated", "").strip()
        if not raw or raw == "Not set yet":
            return "Not set yet 🌱"
        clean = raw.replace("•", "·").strip()
        dt = datetime.strptime(clean, "%B %d, %Y · %I:%M %p")
        dt = pytz.timezone("Asia/Manila").localize(dt)
        delta = datetime.now(pytz.timezone("Asia/Manila")) - dt
        total_h = delta.days * 24 + delta.seconds // 3600
        mins    = (delta.seconds % 3600) // 60
        return f"{total_h}h {mins}m"
    except Exception as e:
        print(f"⚠️ Uptime error: {e}")
        return "Unknown 🌿"


# ══════════════════════════════════════════════════════════════════════════════
# USER PROFILE
# ══════════════════════════════════════════════════════════════════════════════
async def get_user_profile(chat_id: int) -> dict | None:
    data = await _sb_get(
        "user_profiles",
        **{
            "chat_id": f"eq.{chat_id}",
            "select": (
                "*,has_seen_menu,created_at,total_xp_earned,"
                "windows_views,office_views,netflix_views,netflix_reveals,"
                "prime_views,prime_reveals,times_cleared,guidance_reads,"
                "lore_reads,profile_views"
            ),
        },
    )
    if data is None:
        print(f"🔴 get_user_profile failed for {chat_id}")
    return data[0] if data else None


async def update_has_seen_menu(chat_id: int):
    await _sb_patch(f"user_profiles?chat_id=eq.{chat_id}", {"has_seen_menu": True})


async def update_last_active(chat_id: int):
    await _sb_patch(
        f"user_profiles?chat_id=eq.{chat_id}",
        {"last_active": datetime.now(pytz.utc).isoformat()},
    )


# ══════════════════════════════════════════════════════════════════════════════
# LEVELING SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
def get_cumulative_xp_for_level(target_level: int) -> int:
    levels = [0, 0, 240, 560, 980, 1520, 2180, 2980, 3950, 5050, 6300]
    if target_level < len(levels):
        return levels[target_level]
    return levels[-1] + (target_level - 10) * 1300


def get_level_title(level: int) -> str:
    titles = {
        1: "🌱 Young Sprout",
        2: "🌿 Forest Sprout",
        3: "🍃 Gentle Wanderer",
        4: "🌳 Woodland Explorer",
        5: "🌲 Whispering Wanderer",
        6: "🪵 Tree Guardian",
        7: "🌌 Mist Walker",
        8: "✨ Enchanted Keeper",
        9: "🌠 Ancient Soul",
        10: "🌟 Eternal Guardian",
    }
    return titles.get(level, f"🌟 Legend {level}")


def get_max_items(category: str, level: int, event: dict | None = None) -> int:
    level = int(level)

    if category in ("win", "windows", "office"):
        tiers = {1: 2, 2: 3, 3: 3, 4: 4, 5: 4, 6: 6, 7: 8, 8: 10, 9: 13}
        return tiers.get(level, 999)

    if category == "prime":
        tiers = {1: 2, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 5, 8: 7, 9: 9}
        return tiers.get(level, 999)

    if category == "netflix":
        if event:
            bonus_type = event.get("bonus_type", "").strip()
            if bonus_type in EVENT_BONUS_TIERS:
                tiers = EVENT_BONUS_TIERS[bonus_type]
                return tiers.get(level, 999)
            
        # Normal tiers
        tiers = {1: 2, 2: 3, 3: 3, 4: 5, 5: 5, 6: 7, 7: 9, 8: 12, 9: 15}
        return tiers.get(level, 999)

    return 0

# ──────────────────────────────────────────────
# EVENT COUNTDOWN HELPER (reused in menu + viewevent)
# ──────────────────────────────────────────────
async def get_event_countdown(event: dict) -> str:
    """Returns a nice countdown string like '⏳ Ends in 14h 32m' or empty if expired."""
    if not event or not event.get("event_date"):
        return ""
    
    try:
        manila = pytz.timezone("Asia/Manila")
        event_dt = datetime.strptime(event.get("event_date", "").strip(), "%B %d, %Y")
        event_dt = manila.localize(event_dt)
        expires_at = event_dt + timedelta(days=1)
        diff = expires_at - datetime.now(manila)
        
        if diff.total_seconds() > 0:
            hours = int(diff.total_seconds() // 3600)
            mins = int((diff.total_seconds() % 3600) // 60)
            return f"\n⏳ <b>Ends in {hours}h {mins}m</b>"
        else:
            return "\n⏳ <b>Event has ended</b>"
    except Exception:
        return ""  # fallback if date format is broken


def create_progress_bar(current_xp: int, required_xp: int, length: int = 12) -> str:
    if required_xp <= 0:
        return "🟩" * length
    pct     = min(current_xp / required_xp, 1.0)
    filled  = int(pct * length)
    bar     = "🟩" * filled + "⬜" * (length - filled)
    return f"[{bar}] {int(pct * 100)}%"


# ══════════════════════════════════════════════════════════════════════════════
# ANTI-ABUSE  (Redis-backed)
# ══════════════════════════════════════════════════════════════════════════════
MAX_ACTIONS_PER_MINUTE = 20  # raised from 8 — normal browsing hits ~8-10

async def check_xp_cooldown(chat_id: int, action: str) -> bool:
    """Per-action XP cooldown — prevents farming same button repeatedly.
    Returns True if XP is allowed, False if still on cooldown."""
    seconds = COOLDOWN_SECONDS.get(action, 8)
    key = f"xpcd:{chat_id}:{action}"
    if await redis.exists(key):
        return False
    await redis.setex(key, seconds, 1)
    return True

async def check_rate_limit(chat_id: int) -> bool:
    key = f"rl:{chat_id}"
    pipe = redis.pipeline()
    pipe.incr(key)
    pipe.expire(key, 60)
    results = await pipe.execute()
    return results[0] <= MAX_ACTIONS_PER_MINUTE

async def try_consume_reveal_cap(chat_id: int, service_type: str) -> bool:
    """
    Atomically consume one daily reveal slot using a Lua script.
    Completely eliminates the TOCTOU race condition.
    Returns True if the reveal is allowed, False if daily cap is reached.
    """
    key = f"daily_reveals:{chat_id}:{service_type}"
    
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    ttl = int((midnight - now).total_seconds())

    # Execute Lua script atomically
    result = await redis.eval(
        REVEAL_CAP_SCRIPT,
        1,                    # number of keys
        key,                  # KEYS[1]
        MAX_DAILY_REVEALS,    # ARGV[1]
        ttl                   # ARGV[2]
    )

    return result != 0

# ══════════════════════════════════════════════════════════════════════════════
# DAILY BONUS HELPER  (called inside add_xp, not exposed separately)
# ══════════════════════════════════════════════════════════════════════════════
async def _check_daily_bonus(chat_id: int, first_name: str, profile: dict) -> tuple[int, str]:
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ttl = int((midnight - now).total_seconds())
    key = f"daily_bonus:{chat_id}"

    # ATOMIC: only one concurrent request wins this SET
    # nx=True means "only set if key does NOT exist"
    # If the key already exists, set() returns None → bonus already claimed
    claimed = await redis.set(key, 1, ex=ttl, nx=True)
    if not claimed:
        return 0, ""  # already claimed today — bonus not available

    # Won the race — compute bonus amount
    streak = await calculate_streak(chat_id)
    if streak >= 7:
        bonus, label = 30, "🔥🔥 7-Day Streak Bonus!"
    elif streak >= 3:
        bonus, label = 20, "🔥 3-Day Streak Bonus!"
    else:
        bonus, label = 10, "🌅 Daily Login Bonus!"

    return bonus, label

# ══════════════════════════════════════════════════════════════════════════════
# REFERRAL SYSTEM (new dedicated table)
# ══════════════════════════════════════════════════════════════════════════════
async def get_referral_link(chat_id: int) -> str:
    """Returns clean referral link using cached bot username"""
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await tg_app.bot.get_me()
        BOT_USERNAME = me.username
        print(f"✅ Bot username auto-cached: @{BOT_USERNAME}")
    
    return f"https://t.me/{BOT_USERNAME}?start=ref_{chat_id}"


async def store_pending_referral(new_chat_id: int, referrer_id: int) -> bool:
    """Store pending referral. Returns True if stored, False if self-referral."""
    if referrer_id == new_chat_id:
        return False
    await redis.setex(f"pending_ref:{new_chat_id}", 86400, str(referrer_id))
    return True


async def get_pending_referrer(chat_id: int) -> int | None:
    key = f"pending_ref:{chat_id}"
    val = await redis.get(key)
    if val:
        await redis.delete(key)
        return int(val)
    return None


async def award_referral_bonus(referrer_id: int, referred_id: int, new_user_name: str):
    # Daily limit
    key = f"referral_daily:{referrer_id}"
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ttl = int((midnight - now).total_seconds())

    count = await redis.get(key) or "0"
    if int(count) >= MAX_REFERRALS_PER_DAY:
        return

    # Record in referrals table + award XP
    profile = await get_user_profile(referrer_id)
    if not profile:
        return

    old_xp = profile.get("xp", 0)
    old_level = profile.get("level", 1)
    new_xp = old_xp + REFERRAL_XP
    new_count = (profile.get("referral_count") or 0) + 1

    # Level-up logic
    new_level = old_level
    while True:
        needed = get_cumulative_xp_for_level(new_level + 1)
        if new_xp < needed:
            break
        new_level += 1

    # Update user_profiles
    ok = await _sb_patch(f"user_profiles?chat_id=eq.{referrer_id}", {
        "xp": new_xp,
        "level": new_level,
        "referral_count": new_count,
        "total_xp_earned": (profile.get("total_xp_earned") or 0) + REFERRAL_XP,
        "last_active": datetime.now(pytz.utc).isoformat(),
    })

    if ok:
        await redis.setex(key, ttl, int(count) + 1)
        await _log_xp_history(
            referrer_id, "Forest Wanderer", "referral", REFERRAL_XP,
            old_xp, new_xp, old_level, new_level
        )

        # Beautiful notification
        try:
            await tg_app.bot.send_message(
                referrer_id,
                f"🌲 <b>A new wanderer has joined the clearing!</b>\n\n"
                f"✨ <b>+{REFERRAL_XP} Forest Energy</b>\n"
                f"🌿 Your referral count is now <b>{new_count}</b>\n\n"
                f"Welcome, {html.escape(new_user_name)}! 🍃",
                parse_mode="HTML"
            )
        except Exception:
            pass

async def _try_award_referral(chat_id: int):
    """
    Safely award referral bonus exactly once.
    Redis lock prevents re-processing if DB write partially failed before.
    """
    lock_key = f"ref_awarding:{chat_id}"
    # nx=True: only one concurrent task can process this referral
    # ex=60: lock expires after 60s so it's not permanently stuck on crash
    lock = await redis.set(lock_key, 1, ex=60, nx=True)
    if not lock:
        return  # another task is already processing this referral

    try:
        referral = await _sb_get(
            "referrals",
            **{"referred_id": f"eq.{chat_id}", "awarded": "eq.false", "select": "*"}
        )
        if not referral:
            return  # no pending referral — nothing to do

        ref = referral[0]
        referrer_id = ref["referrer_id"]
        new_name = (await get_user_profile(chat_id) or {}).get("first_name", "Wanderer")

        # Mark as awarded FIRST before giving XP (fail-safe direction)
        marked = await _sb_patch(
            f"referrals?referred_id=eq.{chat_id}&awarded=eq.false",
            {"awarded": True, "awarded_at": datetime.now(pytz.utc).isoformat()}
        )
        if not marked:
            return  # couldn't mark — don't give XP, will retry next daily login

        # Now safely give XP — worst case: marked but no XP (not re-awardable)
        await award_referral_bonus(referrer_id, chat_id, new_name)
    finally:
        await redis.delete(lock_key)

# ══════════════════════════════════════════════════════════════════════════════
# XP ENGINE
# ══════════════════════════════════════════════════════════════════════════════
_XP_TABLE = {
    "guidance":      10, 
    "lore":          10, 
    "view_windows":   8,
    "view_office":    8,
    "view_netflix":   8,
    "view_prime":     8,
    "reveal_netflix": 14,
    "reveal_prime":   14,
    "profile":        6,
    "clear":          6,
    "daily_bonus":    0,
}

_STAT_FIELD = {
    "view_windows":   "windows_views",
    "view_office":    "office_views",
    "view_netflix":   "netflix_views",
    "view_prime":     "prime_views",
    "reveal_netflix": "netflix_reveals",
    "reveal_prime":   "prime_reveals",
    "clear":          "times_cleared",
    "guidance":       "guidance_reads",
    "lore":           "lore_reads",
    "profile":        "profile_views",
}

async def add_xp(chat_id: int, first_name: str, action: str = "general") -> tuple[int, int]:
    """
    Awards XP for an action, updates stats, checks level-ups.
    Returns XP actually awarded (0 if rate-limited or on cooldown).
    """
    # ── Hard spam block ──
    if not await check_rate_limit(chat_id):
        asyncio.create_task(send_temporary_message(
            chat_id,
            "🌿 <b>Slow down, wanderer!</b>\n\n"
            "The forest spirits need a moment to breathe... 🍃",
            duration=2
        ))
        return 0, 0
    
    # ── Per-action XP cooldown ──
    xp_allowed = await check_xp_cooldown(chat_id, action)

    # ── Fetch profile ──
    profile = await get_user_profile(chat_id)

    # ── Determine XP amount ──
    xp_amount = _XP_TABLE.get(action, 0) if xp_allowed else 0

    # ── Guidance: 10 XP first time, 2 XP recurring (never dead) ──
    if action == "guidance":
        if not xp_allowed:
            xp_amount = 0
        elif profile is None or profile.get("guidance_reads", 0) == 0:
            xp_amount = 10
        else:
            xp_amount = 2

    # ── Lore: same pattern ──
    elif action == "lore":
        if not xp_allowed:
            xp_amount = 0
        elif profile is None or profile.get("lore_reads", 0) == 0:
            xp_amount = 10
        else:
            xp_amount = 2

    # Save the pure action XP BEFORE we add any daily bonus
    action_xp = xp_amount

    # ── existing user ──
    if profile:
        stat_field = _STAT_FIELD.get(action)
        stats_update: dict = {}

        # ALWAYS increment stat counter regardless of cooldown
        if stat_field:
            stats_update[stat_field] = (profile.get(stat_field) or 0) + 1

        # ── Daily login bonus (injected here, once per day) ──
        daily_bonus, daily_label = await _check_daily_bonus(chat_id, first_name, profile)
        xp_amount += daily_bonus   # 0 if already claimed today

        # ✅ Set days_active BEFORE building payload
        if daily_bonus > 0:
            stats_update["days_active"] = (profile.get("days_active") or 0) + 1

        # Only add to total_xp_earned if XP was actually awarded
        if xp_amount > 0:
            stats_update["total_xp_earned"] = (
                profile.get("total_xp_earned") or 0) + xp_amount
            
        # ── Level-up calculation ──
        new_xp    = (profile.get("xp") or 0) + xp_amount
        old_level = profile.get("level") or 1
        new_level = old_level

        while True:
            needed = get_cumulative_xp_for_level(new_level + 1)
            if new_xp < needed:
                break
            new_level += 1

        payload = {
            "xp":          new_xp,
            "level":       new_level,
            "first_name":  first_name,
            "last_active": datetime.now(pytz.utc).isoformat(),
            **stats_update,
        }
        
        ok = await _sb_patch(f"user_profiles?chat_id=eq.{chat_id}", payload)
        print(f"🔵 PATCH result for {chat_id}: ok={ok}, daily_bonus={daily_bonus}")

        # ── Sequential XP values for clean history logging ──
        base_xp = profile.get("xp") or 0
        xp_after_action = base_xp + action_xp
        xp_after_all    = base_xp + action_xp + daily_bonus

        # Compute level at each step (action might level up before bonus is added)
        level_after_action = old_level
        while get_cumulative_xp_for_level(level_after_action + 1) <= xp_after_action:
            level_after_action += 1

        if daily_bonus > 0:
            if ok:
                await redis.delete(f"streak:{chat_id}")
                asyncio.create_task(_announce_daily_bonus(chat_id, daily_bonus, daily_label))
                asyncio.create_task(
                    _log_xp_history(
                        chat_id, first_name, "daily_bonus", daily_bonus,
                        xp_after_action, xp_after_all, level_after_action, new_level,
                    )
                )
                asyncio.create_task(_try_award_referral(chat_id))
            else:
                await redis.delete(f"daily_bonus:{chat_id}")

        # ✅ prev = base, new = after action only — not inflated by bonus
        if action_xp > 0:
            asyncio.create_task(
                _log_xp_history(
                    chat_id, first_name, action, action_xp,
                    base_xp, xp_after_action, old_level, level_after_action,
                )
            )

        if new_level > old_level:
            asyncio.create_task(
                send_level_up_message(chat_id, first_name, old_level, new_level)
            )

    # ── new user — first tap stat is recorded ──
    else:
        stat_field = _STAT_FIELD.get(action)
        initial_stats = {f: 0 for f in _STAT_FIELD.values()}
        if stat_field:
            initial_stats[stat_field] = 1

        # New users DO get XP for their first action
        # (previously they got 0 — bad first impression)
        first_xp = xp_amount  # whatever action triggered registration
        action_xp = xp_amount  # ✅ save action_xp before adding bonus
        
        # ✅ New users get daily bonus on first visit too
        daily_bonus, daily_label = await _check_daily_bonus(chat_id, first_name, {})
        first_xp += daily_bonus  # first_xp = action + bonus (what goes in DB)

        # NEW: Check for pending referral
        pending_referrer = await get_pending_referrer(chat_id)
        extra_welcome = 0
        if pending_referrer:
            extra_welcome = NEW_USER_WELCOME_BONUS_IF_REFERRED
            first_xp += extra_welcome

        payload = {
            "chat_id":        chat_id,
            "first_name":     first_name,
            "xp":             first_xp,
            "level":          1,
            "last_active":    datetime.now(pytz.utc).isoformat(),
            "has_seen_menu":  False,
            "created_at": datetime.now(pytz.utc).isoformat(),  # ✅ real timestamp
            "total_xp_earned": first_xp,
            "referral_count": 0,
            "days_active":     1 if daily_bonus > 0 else 0,
            **{f: 0 for f in _STAT_FIELD.values()},
            **initial_stats,
        }
        # Inside the new-user else: block, just before the upsert
        print("🔵 NEW USER PAYLOAD KEYS:", list(payload.keys()))
        ok = await _sb_upsert("user_profiles", payload, on_conflict="chat_id")
        print(f"🔵 NEW USER UPSERT for {chat_id}: ok={ok}")

        # Insert into referrals table
        if pending_referrer and ok:
            await _sb_post("referrals", {
                "referrer_id": pending_referrer,
                "referred_id": chat_id
            })

        if daily_bonus > 0:
            if ok:
                asyncio.create_task(_announce_daily_bonus(chat_id, daily_bonus, daily_label))
                asyncio.create_task(
                    _log_xp_history(
                        chat_id, first_name, "daily_bonus", daily_bonus,
                        0, first_xp, 1, 1,
                    )
                )
                asyncio.create_task(_try_award_referral(chat_id))  # ← ADD THIS LINE
            else:
                await redis.delete(f"daily_bonus:{chat_id}")

        # ✅ log action_xp only, not first_xp
        if action_xp > 0 and ok:
            asyncio.create_task(
                _log_xp_history(
                    chat_id, first_name, action, action_xp,
                    0, action_xp, 1, 1,
                )
            )
    return action_xp, xp_amount

async def _log_xp_history(
    chat_id: int, first_name: str, action: str,
    xp_earned: int, prev_xp: int, new_xp: int,
    prev_level: int, new_level: int,
):
    await _sb_post(
        "xp_history",
        {
            "chat_id":        chat_id,
            "first_name":     first_name,
            "action":         action,
            "xp_earned":      xp_earned,
            "previous_xp":    prev_xp,
            "new_xp":         new_xp,
            "previous_level": prev_level,
            "new_level":      new_level,
            "leveled_up":     new_level > prev_level,
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════
def kb_start():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")
    ]])

def kb_main_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🪄 Spirit Treasures",  url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("📜 Ancient Scrolls",   url="https://clyderesourcehub.short.gy/learn-and-guides"),
        ],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest",  url="https://clyderesourcehub.short.gy/")],
        [
            InlineKeyboardButton("❓ Guidance", callback_data="help"),
            InlineKeyboardButton("ℹ️ Lore",     callback_data="about"),
        ],
        [InlineKeyboardButton("🌲 Invite Friends • Earn XP", callback_data="invite_friends")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/caydigitals")],
    ])

def kb_first_time_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❓ Start Here → Guidance",   callback_data="help")],
        [InlineKeyboardButton("🪄 Spirit Treasures",        url="https://clyderesourcehub.short.gy/steam-account")],
        [InlineKeyboardButton("📜 Ancient Scrolls",         url="https://clyderesourcehub.short.gy/learn-and-guides")],
        [InlineKeyboardButton("🌿 Check Forest Inventory",  callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest",   url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("ℹ️ Lore",                    callback_data="about")],
        [InlineKeyboardButton("🌲 Invite Friends • Earn XP", callback_data="invite_friends")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind",  url="https://t.me/caydigitals")],
    ])

def kb_inventory():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🪟 Windows Keys",  callback_data="vamt_filter_win"),
            InlineKeyboardButton("📑 Office Keys",   callback_data="vamt_filter_office"),
        ],
        [InlineKeyboardButton("🍿 Netflix Premium Cookies",      callback_data="vamt_filter_netflix")],
        [InlineKeyboardButton("🎥 PrimeVideo Premium Cookies",   callback_data="vamt_filter_prime")],
        [InlineKeyboardButton("🎮 Steam Accounts",               callback_data="vamt_filter_steam")],
        [InlineKeyboardButton("⬅️ Back to Clearing",             callback_data="main_menu")],
    ])

def kb_back_inventory():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Scroll Selection", callback_data="check_vamt")],
        [InlineKeyboardButton("🏠 Main Menu",                callback_data="main_menu")],
    ])

def kb_back():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Return to the Clearing", callback_data="main_menu")
    ]])

def kb_caretaker():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📜 Add Patch", callback_data="caretaker_addupdate"),
            InlineKeyboardButton("📬 View Feedbacks", callback_data="caretaker_viewfeedback"),
        ],
        [
            InlineKeyboardButton("🎉 Create Event", callback_data="caretaker_addevent"),
            InlineKeyboardButton("🔴 End Event", callback_data="confirm_end_event"),
        ],
        [
            InlineKeyboardButton("👁️ View Event", callback_data="caretaker_viewevent"),
            InlineKeyboardButton("🔄 Flush Cookie", callback_data="caretaker_flushcache"),
        ],
        [InlineKeyboardButton("🛠️ Maintenance Mode", callback_data="confirm_toggle_maintenance")],
        [InlineKeyboardButton("⚠️ Full Reset", callback_data="caretaker_resetfirst")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")],
    ])

def kb_winoffice_guide():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Got it, show me the keys →",
            callback_data="winoffice_got_it"
        )
    ]])

# ══════════════════════════════════════════════════════════════════════════════
# UTILITY MESSAGES
# ══════════════════════════════════════════════════════════════════════════════
def _greeting(tz_str: str = "Asia/Manila") -> tuple[str, str]:
    hour = datetime.now(pytz.timezone(tz_str)).hour
    if 5 <= hour < 12:
        return "🌅", "Good morning"
    if 12 <= hour < 18:
        return "🌤️", "Good afternoon"
    return "🌙", "Good evening"


async def send_xp_feedback(chat_id: int, xp_amount: int, duration: int = 2):
    if xp_amount <= 0:
        return
    try:
        msg = await tg_app.bot.send_message(
            chat_id, f"✨ <b>+{xp_amount} XP</b> earned!", parse_mode="HTML"
        )
        await asyncio.sleep(duration)
        await msg.delete()
    except Exception:
        pass


async def send_temporary_message(chat_id: int, text: str, duration: int = 2):
    try:
        msg = await tg_app.bot.send_message(chat_id, text, parse_mode="HTML")
        await asyncio.sleep(duration)
        await msg.delete()
    except Exception:
        pass

async def send_loading(chat_id: int, caption: str = "🌫️ <i>The ancient mist begins to stir...</i>"):
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=LOADING_GIF,
        caption=caption,
        parse_mode="HTML"
    )
    await _remember(chat_id, msg.message_id)
    return msg

async def _announce_daily_bonus(chat_id: int, bonus: int, label: str):
    """
    Announces daily bonus after a short delay so it doesn't race
    with menu animations or other UI updates.
    """
    await asyncio.sleep(2.5)
    await send_temporary_message(
        chat_id,
        f"✨ <b>{label}</b>\n\n"
        f"<b>+{bonus} XP</b> for visiting the clearing today 🍃",
        duration=5,
    )

async def _remember(chat_id: int, message_id: int):
    try:
        await redis.lpush(f"mem:{chat_id}", message_id)
        await redis.expire(f"mem:{chat_id}", 86400)  # 24 hours TTL
    except Exception as e:
        print(f"⚠️ Redis remember failed: {e}")
        forest_memory.setdefault(chat_id, []).append(message_id)  # fallback to RAM


# ══════════════════════════════════════════════════════════════════════════════
# STREAK
# ══════════════════════════════════════════════════════════════════════════════
async def calculate_streak(chat_id: int) -> int:
    # ── Check Redis cache first ──
    cached = await redis.get(f"streak:{chat_id}")
    if cached is not None:
        return int(cached)

    
    data = await _sb_get(
        "xp_history",
        **{"chat_id": f"eq.{chat_id}", "select": "created_at", "order": "created_at.desc", "limit": 100},
    )
    if data is None:
        return 0
    data = data or []

    manila  = pytz.timezone("Asia/Manila")
    today   = datetime.now(manila).date()
    streak  = 0
    seen: set = set()
    for row in data:
        try:
            dt   = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
            date = dt.astimezone(manila).date()
            if date not in seen:
                seen.add(date)
                if (today - date).days == streak:
                    streak += 1
                else:
                    break
        except Exception:
            continue
            
    # ── Cache result for 5 minutes ──
    await redis.setex(f"streak:{chat_id}", 300, streak)
    return streak


# ══════════════════════════════════════════════════════════════════════════════
# MESSAGES / SCREENS
# ══════════════════════════════════════════════════════════════════════════════
async def send_initial_welcome(chat_id: int, first_name: str):
    icon, greeting = _greeting()
    caption = (
        f"{icon} {greeting}, {html.escape(str(first_name))}!\n\n"
        "🌿 Welcome, dear wanderer, to Clyde's Enchanted Clearing.\n\n"
        "Beneath the whispering ancient trees, a world of gentle magic awaits.\n"
        "Hidden wonders and peaceful moments are ready to be discovered.\n\n"
        "<i>Tap the button below to step into the heart of the forest.</i> 🍃✨"
    )
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id, animation=WELCOME_GIF,
        caption=caption, parse_mode="HTML", reply_markup=kb_start(),
    )
    await _remember(chat_id, msg.message_id)


async def send_full_menu(chat_id: int, first_name: str, is_first_time: bool = False):
    icon, greeting = _greeting()
    profile = await get_user_profile(chat_id)

    level      = profile.get("level", 1) if profile else 1
    title      = get_level_title(level)
    level_info = f"🏷️ {title} • ⭐ Level {level}"
    if profile:
        await redis.delete(f"streak:{chat_id}")
    streak = await calculate_streak(chat_id) if profile else 0
    if streak >= 2:
        streak_txt = f"🔥 <b>{streak}-day streak!</b> The forest fire burns bright!"
    else:
        streak_txt = "🌱 <b>First steps in the forest!</b>"

    # ── Check for active event ──
    event = await get_active_event()
    event_banner = ""
    if event:
        bonus_line = ""
        bonus_type = event.get("bonus_type", "").strip()

        if bonus_type == "netflix_double":
            bonus_line = "\n🍿 <b>Netflix slots are doubled for all levels today!</b>"
        elif bonus_type == "netflix_max":
            bonus_line = "\n🍿 <b>Netflix slots are maximized for all levels today!</b>"

        countdown = await get_event_countdown(event)
        
        event_banner = (
            f"\n✦ ─────────────────── ✦\n"
            f"🎪 <b>FOREST EVENT</b>\n"
            f"✦ ─────────────────── ✦\n\n"
            f"🌸 <b>{event.get('title', '')}</b>\n"
            f"🕰️ {event.get('event_date', '')}{countdown}\n\n"
            f"<i>{event.get('description', '')}</i>\n"
            f"{bonus_line}\n\n"
            f"✦ ─────────────────── ✦\n"
        )

    if is_first_time:
        caption = (
            f"{icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
            "🌿 <b>Welcome to the Enchanted Clearing</b>\n\n"
            f"{level_info} • {streak_txt}\n"
            f"{event_banner}"
            "Beneath the whispering ancient trees, many paths lie before you...\n\n"
            "🌱 <b>New wanderer?</b> We recommend starting with <b>Guidance</b> first.\n\n"
            "<i>Every view gives +8 XP • Every reveal gives +14 XP</i>\n\n"
            "<i>May your steps be guided by gentle forest magic.</i> 🍃✨"
        )
        keyboard = kb_first_time_menu()
    else:
        caption = (
            f"{icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
            "🌿 <b>Welcome back to the Enchanted Clearing</b>\n\n"
            f"{level_info} • {streak_txt}\n"
            f"{event_banner}"
            "The clearing welcomes you back, wanderer.\n\n"
            "<i>Every view gives +8 XP • Every reveal gives +14 XP</i>\n\n"
            "<i>May the forest welcome you once more.</i> 🍃✨"
        )
        keyboard = kb_main_menu()

    msg = await tg_app.bot.send_animation(
        chat_id=chat_id, animation=MENU_GIF,
        caption=caption, parse_mode="HTML", reply_markup=keyboard,
    )
    await _remember(chat_id, msg.message_id)


async def send_level_up_message(chat_id: int, first_name: str, old_level: int, new_level: int):

    # Show what they unlocked
    netflix_old = get_max_items("netflix", old_level)
    netflix_new = get_max_items("netflix", new_level)
    prime_old   = get_max_items("prime", old_level)
    prime_new   = get_max_items("prime", new_level)
    win_old     = get_max_items("win", old_level)
    win_new     = get_max_items("win", new_level)

    unlocks = []
    if netflix_new > netflix_old:
        unlocks.append(f"🍿 Netflix: {netflix_old} → <b>{netflix_new} cookies</b>")
    if prime_new > prime_old:
        unlocks.append(f"🎥 Prime: {prime_old} → <b>{prime_new} cookies</b>")
    if win_new > win_old:
        unlocks.append(f"🪟 Windows Keys: {win_old} → <b>{win_new} keys</b>")
    if new_level == 6:
        unlocks.append("✨ <b>You now receive the FRESHEST cookies first!</b>")

    unlock_text = "\n".join(unlocks) if unlocks else "More wonders await..."

    caption = (
        f"🌟 <b>LEVEL UP! Congratulations, {html.escape(first_name)}!</b>\n\n"
        f"⭐ <b>{old_level} → {new_level}</b>\n"
        f"🏷️ {get_level_title(new_level)}\n\n"
        f"🔓 <b>Newly Unlocked:</b>\n{unlock_text}\n\n"
        "<i>The forest grows with you.</i> 🍃✨"
    )
    try:
        await tg_app.bot.send_animation(
            chat_id=chat_id, animation=LOADING_GIF, caption=caption, parse_mode="HTML"
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# INVENTORY — PAGINATED COOKIES
# ══════════════════════════════════════════════════════════════════════════════
async def show_paginated_cookie_list(
    service_type: str, chat_id: int, query, page: int = 0
):
    loading = await send_loading(
        chat_id,
        f"{'🍿' if service_type == 'netflix' else '🎥'} <i>Opening the ancient scroll of {service_type.title()} cookies...</i>"
    )

    profile   = await get_user_profile(chat_id)
    user_level = profile.get("level", 1) if profile else 1
    event      = await get_active_event() 
    max_items  = get_max_items(service_type, user_level, event)
    
    event_bonus_txt = ""
    if event:
        bonus_type = event.get("bonus_type", "").strip()
        if bonus_type == "netflix_double":
            event_bonus_txt = "🎉 <b>Event Bonus Active!</b> Netflix slots are <b>doubled</b> today!\n\n"
        elif bonus_type == "netflix_max":
            event_bonus_txt = "🎉 <b>Event Bonus Active!</b> Netflix slots are <b>maximized</b> today!\n\n"
    
    title = "Netflix" if service_type == "netflix" else "PrimeVideo"
    emoji = "🍿"    if service_type == "netflix" else "🎥"
    data = await get_vamt_data()

    if not data:
        await send_supabase_error(chat_id)
        await query.message.edit_caption(
            "🌫️ <b>The forest is unreachable right now...</b>\n\nPlease try again shortly. 🍃",
            parse_mode="HTML",
            reply_markup=kb_back_inventory(),
        )
        await loading.delete()
        return

    filtered = [
        item for item in data
        if service_type in str(item.get("service_type", "")).lower()
        and str(item.get("status", "")).lower() == "active"
        and int(item.get("remaining", 0)) > 0
    ]

    if not filtered:
        await query.message.edit_caption(
            caption=(
                f"<b>{emoji} Secret {title} Premium Cookies</b>\n\n"
                "🌫️ No working cookies available in the forest right now...\n\n"
                "The trees are resting. Please check back later or explore other scrolls 🍃"
            ),
            parse_mode="HTML",
            reply_markup=kb_back_inventory(),
        )
        return

    filtered.sort(key=lambda x: x.get("last_updated", ""))

    if user_level >= 6:
        filtered  = filtered[-max_items:]
        priority  = "✨ You get the freshest cookies first!"
    else:
        filtered  = filtered[:max_items]
        priority  = "🌱 You get older but still working cookies."

    start      = page * NETFLIX_ITEMS_PER_PAGE
    end        = start + NETFLIX_ITEMS_PER_PAGE
    page_items = filtered[start:end]
    total_pages = (len(filtered) + NETFLIX_ITEMS_PER_PAGE - 1) // NETFLIX_ITEMS_PER_PAGE

    report = (
        f"<b>{emoji} Secret {title} Premium Cookies</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"{event_bonus_txt}"
        f"📦 <b>{len(filtered)} {title} available</b>\n"
        f"📄 Page {page + 1} of {total_pages}\n\n"
        "<i>Which one whispers to your spirit?</i>\n\n"
    )

    buttons = []
    for idx, item in enumerate(page_items, start=start + 1):
        name = str(item.get("display_name") or "").strip() or f"{title} Cookie"
        report += f"✨ <b>{name}</b>\n Status: ✅ Working\n Remaining: {item.get('remaining', 0)}\n\n"
        buttons.append([
            InlineKeyboardButton(f"🔓 Reveal {name}", callback_data=f"reveal_{service_type}|{idx}|{page}")
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("↼ Previous", callback_data=f"{service_type}_page_{page - 1}"))
    if end < len(filtered):
        nav.append(InlineKeyboardButton("Next ⇀",     callback_data=f"{service_type}_page_{page + 1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("⬅️ Back to the Clearing", callback_data="check_vamt")])

    report += (
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🌿 Level {user_level} → Up to {max_items} items\n"
        f"{priority}\n"
    )
    if len(filtered) < max_items:
        report += f"\n✅ Only {len(filtered)} working {title} cookies currently in the forest.\n\n"
    report += "⚠️ Cookies can stop working without notice. Test quickly after revealing."

    await query.message.edit_caption(
        caption=report, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons)
    )

    try:
        await loading.delete()
    except Exception:
        pass


async def reveal_cookie(service_type: str, chat_id: int, first_name: str, query, idx: int, page: int):
    emoji = "🍿" if service_type == "netflix" else "🎥"

    # Prevent button spam on reveal
    spam_key = f"reveal_spam:{chat_id}:{service_type}"
    if await redis.exists(spam_key):
        await query.answer("🌿 Slow down, wanderer! One reveal at a time 🍃", show_alert=True)
        return
    await redis.setex(spam_key, 3, 1)
    
    if not await try_consume_reveal_cap(chat_id, service_type):
        await query.answer(
            "🌿 You've revealed enough cookies for today.\n"
            "The forest needs to rest — come back tomorrow! 🍃",
            show_alert=True
        )
        return
    
    loading = await send_loading(
        chat_id,
        f"{emoji} <i>Searching deep within the glowing glade for your cookie...</i>"
    )
        
    await asyncio.sleep(1.3)
    await loading.edit_caption(
        f"🌟 <i>The hidden {service_type} cookie spirit is slowly awakening...</i>\n\nPlease wait...",
        parse_mode="HTML"
    )
    await asyncio.sleep(1.5)

    profile = await get_user_profile(chat_id)
    user_level = profile.get("level", 1) if profile else 1
    event      = await get_active_event()  
    max_items  = get_max_items(service_type, user_level, event)
    data = await get_vamt_data()
    if not data:
        await query.answer("🌫️ The forest is unreachable right now. Please try again shortly.", show_alert=True)
        await loading.delete()
        return

    filtered = [
        item for item in data
        if service_type in str(item.get("service_type", "")).lower()
        and str(item.get("status", "")).lower() == "active"
        and int(item.get("remaining", 0)) > 0
    ]
    filtered.sort(key=lambda x: x.get("last_updated", ""))
    if user_level >= 6:
        filtered = filtered[-max_items:]
    else:
        filtered = filtered[:max_items]

    if idx < 1 or idx > len(filtered):
        await query.answer("❌ Cookie no longer available", show_alert=True)
        await loading.delete()
        await show_paginated_cookie_list(service_type, chat_id, query, page)
        return

    item = filtered[idx - 1]
    if str(item.get("status", "")).lower() != "active" or int(item.get("remaining", 0)) <= 0:
        await query.answer("⚠️ This cookie has expired.", show_alert=True)
        await loading.delete()
        await show_paginated_cookie_list(service_type, chat_id, query, page)
        return

    # ── Validate item is still good FIRST ──
    cookie = str(item.get("key_id", "")).strip()
    display_name = str(item.get("display_name") or "").strip() or f"{service_type.title()} Cookie"

    action_name = "reveal_netflix" if service_type == "netflix" else "reveal_prime"

    caption = (
        f"📄 <b>{display_name.replace(' ', '_')}.txt</b>\n\n"
        f"{emoji} <b>{display_name} Revealed</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🌿 Status: <b>✅ Awakened</b>\n"
        f"📦 Remaining: <b>{item.get('remaining', 0)}</b>\n\n"
        "📥 The forest has wrapped your cookie in an ancient scroll.\n"
        "<i>Tap the file below to receive its magic.</i> 🍃"
    )

    file_content = (
        f"🌿🍃 Clyde's Enchanted Clearing — Secret {service_type.title()} Cookie 🌿🍃\n"
        "══════════════════════════════════════════════════════════════\n"
        f"🌳 Cookie Spirit Awakened\n"
        f"Name : {display_name}\n"
        f"Status : ✅ Working\n"
        f"Remaining: {item.get('remaining', 0)} uses\n"
        f"Revealed on : {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}\n"
        "🌲 Guard it well, wanderer.\n"
        "══════════════════════════════════════════════════════════════\n"
        f"{cookie}\n"
        "══════════════════════════════════════════════════════════════\n"
        "🍃 May this cookie bring you peaceful streams.\n"
        "— The Caretaker of the Enchanted Clearing 🌿\n"
    )

    file_bytes = BytesIO(file_content.encode("utf-8"))
    file_bytes.name = f"{display_name.replace(' ', '_')}.txt"

    # Store key_id in Redis so we can retrieve it from the short idx reference
    await redis.setex(f"reveal_key:{chat_id}:{service_type}:{idx}", 3600, cookie)

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Working", callback_data=f"kfb_ok|{service_type}|{idx}"),
            InlineKeyboardButton("❌ Not Working", callback_data=f"kfb_bad|{service_type}|{idx}"),
        ],
        [
            InlineKeyboardButton(
                f"⬅️ Back to {service_type.title()} Cookies",
                callback_data=f"back_to_{service_type}_list|{page}"
            )
        ]
    ])

    # NOW send the document
    try:
        doc_msg = await tg_app.bot.send_document(
            chat_id=chat_id,
            document=file_bytes,
            caption=caption,
            parse_mode="HTML",
            reply_markup=kb,
            filename=file_bytes.name
        )
    except Exception as e:
        await query.answer("🌿 Delivery failed, please try again.", show_alert=True)
        await loading.delete()
        print(f"🔴 Document send failed for {chat_id}: {e}")
        return
    
    # Cleanup loading
    try:
        await loading.delete()
    except Exception:
        pass

    # Only reached on successful send
    action_xp, _ = await add_xp(chat_id, first_name, action_name)
    if action_xp:
        asyncio.create_task(send_xp_feedback(chat_id, action_xp))

    # Store message ID for later cleanup
    await redis.setex(
        f"reveal_msg:{chat_id}:{service_type}",
        3600,
        str(doc_msg.message_id)
    )

    # Optional success message (auto-deletes)
    asyncio.create_task(send_temporary_message(
            chat_id, f"✨ <i>{display_name} successfully delivered!</i>", duration=3
        ))

    try:
        await query.message.delete()
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
# PROFILE / STATS / LEADERBOARD / HISTORY
# ══════════════════════════════════════════════════════════════════════════════
async def handle_profile(chat_id: int, first_name: str):
    loading = await send_loading(chat_id, "🌿 <i>Reading your forest soul...</i>")

    # NEW: add_xp now returns TWO values
    action_xp, _ = await add_xp(chat_id, first_name, "profile")
    profile = await get_user_profile(chat_id)
    if not profile:
        await send_supabase_error(chat_id)
        return

    level            = profile["level"]
    current_xp       = profile["xp"]
    xp_required_next = get_cumulative_xp_for_level(level + 1)

    caption = (
        f"🌿 <b>{html.escape(first_name)}'s Forest Profile</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ <b>Title:</b> {get_level_title(level)}\n"
        f"⭐ <b>Level:</b> {level}\n\n"
        f"✨ <b>Experience:</b> {current_xp:,} / {xp_required_next:,} XP\n"
        f"{create_progress_bar(current_xp, xp_required_next, 10)}\n\n"
        f"📈 <b>To Next Level:</b> {max(0, xp_required_next - current_xp):,} XP\n\n"
        "🌱 <b>Quick Rewards:</b>\n"
        "• View any list → <b>+8 XP</b>\n"
        "• Reveal Netflix Cookie → <b>+14 XP</b>\n"
        "• Profile / Clear → <b>+6 XP</b>\n\n"
        "<i>The more you explore the clearing, the stronger your bond with the forest grows.</i> 🍃"
    )
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id, animation=MYID_GIF, caption=caption, parse_mode="HTML"
    )

    try:
        await loading.delete()
    except:
        pass

    # ✅ ONLY CHANGE: use action_xp instead of xp
    if action_xp:
        asyncio.create_task(send_xp_feedback(chat_id, action_xp))
    
    await _remember(chat_id, msg.message_id)


async def handle_stats(chat_id: int, first_name: str):
    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_message(
            chat_id, "🌿 You haven't started your journey yet. Use /profile to begin!"
        )
        return

    level            = profile.get("level", 1)
    xp               = profile.get("xp", 0)
    xp_required_next = get_cumulative_xp_for_level(level + 1)

    joined_date = "Unknown"
    if profile.get("created_at"):
        try:
            dt = datetime.fromisoformat(profile["created_at"].replace("Z", "+00:00"))
            joined_date = dt.strftime("%B %d, %Y")
        except Exception:
            pass

    last_active = "Just now"
    if profile.get("last_active"):
        try:
            dt      = datetime.fromisoformat(profile["last_active"].replace("Z", "+00:00"))
            dt      = dt.astimezone(pytz.timezone("Asia/Manila"))
            diff_m  = int((datetime.now(pytz.timezone("Asia/Manila")) - dt).total_seconds() / 60)
            if diff_m < 2:
                last_active = "Just now"
            elif diff_m < 60:
                last_active = f"{diff_m} minutes ago"
            else:
                last_active = dt.strftime("%B %d, %Y • %I:%M %p")
        except Exception:
            pass

    caption = (
        f"🌲 <b>{html.escape(first_name)}'s Forest Statistics</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ <b>Title:</b> {get_level_title(level)}\n"
        f"⭐ <b>Level:</b> {level}\n\n"
        f"✨ <b>Experience:</b> {xp:,} / {xp_required_next:,} XP\n"
        f"{create_progress_bar(xp, xp_required_next, 10)}\n\n"
        "📊 <b>Detailed Stats:</b>\n"
        f"• Total XP Earned: <b>{profile.get('total_xp_earned', xp):,}</b>\n"
        f"• Profile Views: <b>{profile.get('profile_views', 0)}</b>\n"
        f"• Windows Keys Viewed: <b>{profile.get('windows_views', 0)}</b>\n"
        f"• Office Keys Viewed: <b>{profile.get('office_views', 0)}</b>\n"
        f"• Netflix Keys Viewed: <b>{profile.get('netflix_views', 0)}</b>\n"
        f"• PrimeVideo Keys Viewed: <b>{profile.get('prime_views', 0)}</b>\n"
        f"• Netflix Cookies Revealed: <b>{profile.get('netflix_reveals', 0)}</b>\n"
        f"• PrimeVideo Cookies Revealed: <b>{profile.get('prime_reveals', 0)}</b>\n"
        f"• Times Cleared: <b>{profile.get('times_cleared', 0)}</b>\n"
        f"• Guidance Read: <b>{profile.get('guidance_reads', 0)}</b>\n"
        f"• Lore Read: <b>{profile.get('lore_reads', 0)}</b>\n\n"
        f"• Days Active: <b>{profile.get('days_active', 0)}</b>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🌱 <b>Joined:</b> {joined_date}\n"
        f"🌲 <b>Last Active:</b> {last_active}\n\n"
        "<i>The trees remember every step you've taken...</i> 🍃"
    )
    msg = await tg_app.bot.send_message(chat_id=chat_id, text=caption, parse_mode="HTML")
    await _remember(chat_id, msg.message_id)


async def handle_history(chat_id: int, first_name: str, page: int = 0):
    limit  = 8
    offset = page * limit

    profile     = await get_user_profile(chat_id)
    level       = profile.get("level", 1) if profile else 1
    total_xp    = profile.get("total_xp_earned", 0) if profile else 0
    title_str   = get_level_title(level)

    manila = pytz.timezone("Asia/Manila")
    today_utc = (
        datetime.now(manila).replace(hour=0, minute=0, second=0, microsecond=0)
        .astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    )

    # ── single Supabase call, everything filtered in Python ──
    all_logs = await _sb_get(
        "xp_history",
        **{
            "chat_id": f"eq.{chat_id}",
            "select":  "*",
            "order":   "created_at.desc",
            "limit":   1000,
        },
    )

    if all_logs is None:
        await send_supabase_error(chat_id)
        return
    all_logs = all_logs or []  # empty list is valid
    # ── NEW PATTERN END ──
    
    # total count
    total_entries = len(all_logs)

    # paged logs
    logs = all_logs[offset: offset + limit]

    # xp earned today (filter in Python)
    xp_today = sum(
        r.get("xp_earned", 0) for r in all_logs
        if r.get("created_at", "") >= today_utc
    )

    streak = await calculate_streak(chat_id)
    streak_txt = f"You're on a {streak}-day XP streak! 🔥" if streak >= 2 else "Welcome to your journey! 🌱"

    action_count = Counter(r.get("action") for r in all_logs if r.get("action"))
    top_action_name  = "None yet"
    top_action_count = 0
    if action_count:
        top, cnt = action_count.most_common(1)[0]
        top_action_name  = top.replace("_", " ").title()
        top_action_count = cnt

    if not logs and page == 0:
        await tg_app.bot.send_message(
            chat_id, "🌱 No steps recorded yet.\nStart exploring the clearing to grow!"
        )
        return

    lines = [
        f"🌟 <b>{html.escape(first_name)}'s XP Journey</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🏷️ <b>Current Title:</b> {title_str} • Level {level}",
        f"✨ <b>Total XP Earned:</b> {total_xp:,}",
        f"🌞 <b>XP Today:</b> {xp_today:,}",
        f"🔥 <b>Streak:</b> {streak_txt}",
        f"🏆 <b>Most Used:</b> {top_action_name} ({top_action_count} times)",
        "━━━━━━━━━━━━━━━━━━",
    ]

    for log in logs:
        action_name = log.get("action", "Unknown").replace("_", " ").title()
        try:
            dt       = datetime.fromisoformat(log["created_at"].replace("Z", "+00:00"))
            time_str = dt.astimezone(manila).strftime("%Y-%m-%d %H:%M")
        except Exception:
            time_str = str(log.get("created_at", ""))[:16]

        main_line = f" {action_name} → +{log.get('xp_earned', 0)} XP"
        if log.get("leveled_up") and log.get("new_level", 1) > 1:
            main_line += f" → <b>Level {log.get('new_level')} 🎉</b>"

        lines += [
            f"🕒 {time_str}",
            main_line,
            f" {log.get('previous_xp', 0)} → {log.get('new_xp', 0)} XP",
            "",
        ]

    total_pages = max(1, (total_entries + limit - 1) // limit)
    lines += [
        "━━━━━━━━━━━━━━━━━━",
        f"🌱 Page {page + 1} of {total_pages} • The trees remember every step of your growth... 🍃",
    ]

    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("↼ Previous", callback_data=f"history_page_{page - 1}"))
    if (page + 1) * limit < total_entries:
        buttons.append(InlineKeyboardButton("Next ⇀", callback_data=f"history_page_{page + 1}"))

    await tg_app.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([buttons]) if buttons else None,
    )


async def handle_leaderboard(chat_id: int):
    MIN_LEVEL = 3

    profile = await get_user_profile(chat_id)
    if chat_id != OWNER_ID:
        if not profile or profile.get("level", 1) < MIN_LEVEL:
            current = profile.get("level", 1) if profile else 1
            await tg_app.bot.send_message(
                chat_id,
                f"🏆 <b>Guardians of the Enchanted Clearing</b>\n\n"
                f"🌲 The ancient trees guard the leaderboard.\n\n"
                f"Reach <b>Level {MIN_LEVEL}</b> to unlock it.\n\n"
                f"You are currently <b>Level {current}</b>.\n\n"
                "Keep exploring, gaining XP, and the trees will reveal the rankings. 🌱",
                parse_mode="HTML",
            )
            return

    top_data = await _sb_get(
        "user_profiles",
        **{"select": "first_name,xp,level,chat_id", "xp": "gt.0", "order": "xp.desc", "limit": 10},
    )

    if top_data is None:
        await tg_app.bot.send_message(
            chat_id,
            "🏆 <b>Guardians of the Enchanted Clearing</b>\n\n"
            "🌲 No one has earned any XP yet.\nBe the first to explore! 🌱✨",
            parse_mode="HTML",
        )
        return
    
    top_data = top_data or []
    if not top_data:
        await tg_app.bot.send_message(chat_id, "🌲 No one has earned any XP yet...")
        return

    text = "🏆 <b>Guardians of the Enchanted Clearing</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for rank, u in enumerate(top_data, 1):
        name  = "The Forest Warden" if str(u.get("chat_id")) == str(OWNER_ID) else html.escape(u.get("first_name", "Unknown"))
        medal = medals.get(rank, f"{rank}.")
        text += f"{medal} <b>{name}</b>\n   {get_level_title(u.get('level', 1))} • Level {u.get('level', 1)}\n   ✨ {u.get('xp', 0):,} XP\n\n"

    if profile and profile.get("xp", 0) > 0:
        rank_data = await _sb_get("user_leaderboard", **{"chat_id": f"eq.{chat_id}", "select": "rank"}) or []
        real_rank = rank_data[0].get("rank", "?") if rank_data else "?"
        text += "━━━━━━━━━━━━━━━━━━\n"
        display = "The Forest Warden" if chat_id == OWNER_ID else f"ranked #{real_rank}"
        text += f"📍 <b>You are currently {display}</b>\n   {get_level_title(profile.get('level', 1))} • Level {profile.get('level', 1)}\n   ✨ {profile.get('xp', 0):,} XP\n"

    text += "\n<i>May your roots grow deep and your light shine through the canopy.</i> 🍃✨"
    await tg_app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")


# ══════════════════════════════════════════════════════════════════════════════
# FEEDBACK
# ══════════════════════════════════════════════════════════════════════════════
async def handle_feedback(chat_id: int, first_name: str, feedback_text: str):
    # ── Rate limit: 3 per day ──
    key = f"feedback_limit:{chat_id}"
    count = await redis.incr(key)
    await redis.expire(key, 86400)  # 24 hours

    if count > 3:
        await tg_app.bot.send_message(
            chat_id,
            "🍃 <b>You've already used all your feedback today.</b>\n\nThe forest appreciates your voice — please return tomorrow. 🌿",
            parse_mode="HTML",
        )
        return

    remaining = 3 - count    
    
    manila    = pytz.timezone("Asia/Manila")
    timestamp = datetime.now(manila).strftime("%B %d, %Y • %I:%M %p")

    saved = await _sb_post(
        "feedback",
        {"chat_id": int(chat_id), "first_name": str(first_name), "feedback_text": feedback_text.strip()},
    )

    caption = (
        "🕊️ <b>A Message Carried by the Wind</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Dear <b>{html.escape(str(first_name))}</b>,\n\n"
        "The gentle breeze has carried your words through the ancient trees...\n"
        "They have reached the caretaker of the Enchanted Clearing.\n\n"
        "Thank you for sharing your thoughts with this small, magical corner of the forest.\n\n"
        "<i>May your voice help the clearing bloom even brighter.</i> 🍃✨\n\n"
        f"🕒 <b>Sent:</b> {timestamp}\n"
        f"📬 <b>Remaining feedback today:</b> {remaining}"
    )
    await tg_app.bot.send_animation(
        chat_id=chat_id, animation=HELP_GIF, caption=caption, parse_mode="HTML"
    )

    status = "✅ Saved to database" if saved else "⚠️ Failed to save"
    try:
        await tg_app.bot.send_message(
            OWNER_ID,
            f"🌿 <b>New Feedback</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 From: <b>{first_name}</b>\n🆔 <code>{chat_id}</code>\n\n"
            f"💬 {feedback_text}\n\n━━━━━━━━━━━━━━━━━━\n"
            f"🕒 {timestamp}\n💾 {status}",
            parse_mode="HTML",
        )
    except Exception as e:
        print(f"Owner notify failed: {e}")


async def handle_view_feedback(chat_id: int):
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the caretaker can view the feedback scrolls.")
        return

    data = await _sb_get("feedback", select="*", order="created_at.desc", limit=15)
    if data is None:
        await tg_app.bot.send_message(chat_id, "🌿 The feedback scroll is empty.")
        return
    data = data or []
    if not data:
        await tg_app.bot.send_message(chat_id, "🌿 The feedback scroll is empty.")
        return

    manila = pytz.timezone("Asia/Manila")
    msg    = "🌿 <b>Recent Feedback from the Forest</b>\n━━━━━━━━━━━━━━━━━━\n\n"

    for idx, item in enumerate(data, 1):
        try:
            dt       = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
            time_str = dt.astimezone(manila).strftime("%b %d, %Y • %I:%M %p")
        except Exception:
            time_str = str(item.get("created_at", ""))[:16]

        msg += (
            f"✨ <b>{idx}.</b> From <b>{html.escape(str(item.get('first_name', 'Unknown')))}</b>\n"
            f"🆔 <code>{item.get('chat_id')}</code>\n"
            f"🕒 {time_str}\n\n"
            f"💬 {html.escape(str(item.get('feedback_text', '')).strip())}\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
        )

    await tg_app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")


# ══════════════════════════════════════════════════════════════════════════════
# PATCH NOTES
# ══════════════════════════════════════════════════════════════════════════════
async def handle_updates(chat_id: int):
    data = await _sb_get("patch_notes", order="created_at.desc", limit=5)
    if data is None:
        await tg_app.bot.send_message(chat_id, "🌱 No patch notes yet.")
        return
    data = data or []
    if not data:
        await tg_app.bot.send_message(chat_id, "🌱 No patch notes yet.")
        return
    
    text = "🌿 <b>Patch Notes — Recent Updates</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    for i, u in enumerate(data, 1):
        prefix = f"✨ <b>{i}. {u.get('date', '')}</b>\n" if len(data) > 1 else f"✨ <b>{u.get('date', '')}</b>\n"
        text += prefix + f"<b>{u.get('title', '')}</b>\n\n{u.get('content', '').strip()}\n\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "🍃 <i>May these updates bring more magic to your journey.</i> ✨"

    await tg_app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)

# ══════════════════════════════════════════════════════════════════════════════
# EVENTS
# ══════════════════════════════════════════════════════════════════════════════
async def get_active_event() -> dict | None:
    cached = await redis.get("active_event")
    if cached:
        result = json.loads(cached)
    else:
        data = await _sb_get("events", **{"is_active": "eq.true", "order": "created_at.desc", "limit": 1})
        result = data[0] if data else None
        if result:
            await redis.setex("active_event", 300, json.dumps(result))

    if result:
        # ── Auto-expire check ──
        try:
            manila = pytz.timezone("Asia/Manila")
            event_dt = datetime.strptime(result.get("event_date", "").strip(), "%B %d, %Y")
            event_dt = manila.localize(event_dt)
            expires_at = event_dt + timedelta(days=1)
            if datetime.now(manila) >= expires_at:
                await _sb_patch("events?is_active=eq.true", {"is_active": False})
                await redis.delete("active_event")
                return None
        except Exception:
            pass  # unparseable date — just show event normally

    return result

async def handle_add_event(chat_id: int, title: str, description: str, event_date: str, bonus_type: str = ""):
    if chat_id != OWNER_ID:
        return

    # Deactivate all previous events first
    await _sb_patch("events?is_active=eq.true", {"is_active": False})

    ok = await _sb_post("events", {
        "title":       title.strip(),
        "description": description.strip(),
        "event_date":  event_date.strip(),
        "is_active":   True,
        "bonus_type":  bonus_type.strip(),
    })

    if ok:
        await tg_app.bot.send_message(
            chat_id,
            f"✅ <b>Event created!</b>\n\n"
            f"🎉 <b>{title}</b>\n"
            f"📅 {event_date}\n\n"
            f"{description}\n\n"
            f"<i>Event is now live and visible to all wanderers.</i> 🌿",
            parse_mode="HTML",
        )
        await redis.delete("active_event")
    else:
        await tg_app.bot.send_message(chat_id, "❌ Failed to save event.")
    
async def handle_end_event(chat_id: int):
    if chat_id != OWNER_ID:
        return

    ok = await _sb_patch("events?is_active=eq.true", {"is_active": False})
    if ok:
        await tg_app.bot.send_message(
            chat_id,
            "✅ <b>Event ended!</b>\n\n"
            "🌿 The forest has returned to its peaceful state.\n"
            "<i>No active events running.</i> 🍃",
            parse_mode="HTML",
        )
        await redis.delete("active_event")
    else:
        await tg_app.bot.send_message(chat_id, "ℹ️ No active event found to end.")

async def handle_view_event(chat_id: int):
    event = await get_active_event()
    if not event:
        await tg_app.bot.send_message(
            chat_id,
            "🌿 <b>No active event right now.</b>\n\n"
            "<i>Use the caretaker menu to create one.</i>",
            parse_mode="HTML",
        )
        return
    
    countdown = await get_event_countdown(event)

    text = (
            f"🎉 <b>Current Active Event</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>{event.get('title', '')}</b>\n"
            f"📅 {event.get('event_date', '')}{countdown}\n\n"
            f"{event.get('description', '')}\n\n"
            f"🎁 <b>Bonus:</b> {event.get('bonus_type') or 'None'}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<i>Event is currently live and visible to all wanderers.</i> 🌿"
    )

    await tg_app.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML"
    )

async def add_new_update(title: str, content: str, owner_chat_id: int):
    date = datetime.now(pytz.timezone("Asia/Manila")).strftime("%B %d, %Y")
    ok   = await _sb_post("patch_notes", {"date": date, "title": title, "content": content})
    msg  = f"✅ Patch note added!\n\n📅 {date}\n📌 {title}" if ok else "❌ Failed to save to database."
    await tg_app.bot.send_message(owner_chat_id, msg)


# ══════════════════════════════════════════════════════════════════════════════
# MISC COMMANDS
# ══════════════════════════════════════════════════════════════════════════════
async def send_myid(chat_id: int):
    caption = (
        "🌿 <b>Your Eternal Forest ID</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        f"🪪 <code>{chat_id}</code>\n\n"
        "Keep this ID safe — the caretaker may ask for it if you ever need help.\n\n"
        "<i>May your roots stay strong in the Enchanted Clearing.</i> 🍃"
    )
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id, animation=MYID_GIF, caption=caption, parse_mode="HTML"
    )
    await _remember(chat_id, msg.message_id)


async def handle_info(chat_id: int):
    config = await get_bot_config()
    if not config:
        await send_supabase_error(chat_id)
        return

    version = config.get("current_version", "?")
    updated = config.get("last_updated", "?")

    # Total users
    total_data = await _sb_get("user_profiles", select="chat_id")
    total_users = len(total_data) if total_data else 0

    # ── Active Now (last 15 minutes) ──
    manila = pytz.timezone("Asia/Manila")
    ago_15min = (datetime.now(manila) - timedelta(minutes=15)).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    active_data = await _sb_get(
        "user_profiles",
        **{"select": "chat_id", "last_active": f"gte.{ago_15min}"}
    )
    active_count = len(active_data) if active_data else 0

    text = (
        "🌿 <b>Enchanted Clearing Status</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        "🌳 The forest is thriving peacefully.\n\n"
        f"🕒 <b>Uptime:</b> {await get_bot_uptime()}\n"
        f"🌱 <b>Total Wanderers:</b> {total_users:,}\n"
        f"✨ <b>Active Now:</b> {active_count:,}\n\n"
        f"📜 <b>Current Version:</b> {version}\n"
        f"🔄 <b>Last Updated:</b> {updated}\n\n"
        "⚠️ <i>For personal and educational use only.</i>\n\n"
        "Made with care by the Forest Caretaker 🍃"
    )

    await tg_app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")


async def handle_caretaker(chat_id: int, first_name: str):
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker may enter this sacred glade.")
        return
    text = (
        "🌲 <b>Welcome back, Forest Caretaker</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Hello, {html.escape(first_name)}!\n\n"
        "You stand in the hidden grove reserved only for the caretaker.\n"
        "Choose your action below:\n"
    )
    await tg_app.bot.send_animation(
        chat_id=chat_id, animation=CARETAKER_GIF,
        caption=text, parse_mode="HTML", reply_markup=kb_caretaker(),
    )

# ──────────────────────────────────────────────
# INVITE HANDLER (used by both /invite and menu button)
# ──────────────────────────────────────────────
async def handle_invite(chat_id: int, first_name: str):
    """Show referral link to caretaker or 'coming soon' to regular users."""
    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_message(chat_id, "🌿 Start your journey first with /start!")
        return

    # ── ONLY THE FOREST CARETAKER gets the real invite link ──
    if chat_id == OWNER_ID:
        link = await get_referral_link(chat_id)
        count = profile.get("referral_count", 0)

        caption = (
            f"🌲 Your Personal Invite Link\n\n"
            f"✨ Invite a friend → both get rewards!\n\n"
            f"🌿 You earn +{REFERRAL_XP} XP per successful referral\n"
            f"🌱 Friend gets +{NEW_USER_WELCOME_BONUS_IF_REFERRED} XP welcome bonus\n\n"
            f"🔗 {link}\n\n"
            f"📊 You have invited {count} wanderers so far\n\n"
            f"Share the magic of the Enchanted Clearing, {html.escape(first_name)}! 🍃"
        )

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🔗 Share Invite Link 🌲",
                    url=f"https://t.me/share/url?url={link}&text=🌲%20Join%20me%20in%20Clyde%27s%20Enchanted%20Clearing!%0A%0AGet%20premium%20resources%20%2B%20XP%20bonuses%20%F0%9F%8C%BF"
                )
            ],
            [InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_ref_link|{chat_id}")]
        ])

        await tg_app.bot.send_animation(
            chat_id=chat_id,
            caption=caption,
            animation=INVITE_GIF,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    # ── REGULAR USERS see the "Coming Soon" message ──
    caption = (
        f"🌲 The Referral Grove is Still Growing...\n\n"
        f"Hey {html.escape(first_name)},\n\n"
        "We’re preparing a beautiful new feature for you:\n\n"
        "• Bring a new friend → you earn 25 XP Forest Energy\n"
        "• Your friend gets a +40 XP welcome bonus on their first daily login\n"
        "• Only counts when they claim their first daily bonus\n"
        "• Max 8 referrals per day (to keep the forest fair)\n\n"
        "The ancient trees are quietly preparing this path.\n"
        "Thank you for your patience, kind wanderer.\n\n"
        "Something wonderful is growing in the clearing... 🍃✨"
    )

    await tg_app.bot.send_message(
        chat_id,
        caption,
        parse_mode="HTML"
    )


async def handle_toggle_maintenance(chat_id: int):
    if chat_id != OWNER_ID:
        return

    current = await get_maintenance_mode()
    new_state = not current
    await set_maintenance_mode(new_state)

    status = "🔴 ON" if new_state else "🟢 OFF"
    label  = "active" if new_state else "lifted"

    await tg_app.bot.send_message(
        chat_id,
        f"🌿 <b>Maintenance Mode {status}</b>\n\n"
        f"The Enchanted Clearing is now <b>{label}</b>.\n\n"
        f"{'Wanderers will see the maintenance message.' if new_state else 'Wanderers can access the clearing again.'} 🍃",
        parse_mode="HTML",
    )


async def handle_clear(chat_id: int, user_msg_id: int, first_name: str):
    try:
        await tg_app.bot.delete_message(chat_id, user_msg_id)
    except Exception:
        pass

    # ── fetch from Redis first, fallback to RAM ──
    try:
        message_ids = await redis.lrange(f"mem:{chat_id}", 0, -1)
        await redis.delete(f"mem:{chat_id}")
    except Exception as e:
        print(f"⚠️ Redis clear failed: {e}")
        message_ids = forest_memory.get(chat_id, [])
        forest_memory[chat_id] = []

    for mid in message_ids:
        try:
            await tg_app.bot.delete_message(chat_id, int(mid))
        except Exception:
            pass

    loading = await tg_app.bot.send_animation(
        chat_id=chat_id, animation=CLEAN_GIF,
        caption="🌫️ <b>The ancient mist begins to thicken...</b>", parse_mode="HTML",
    )
    await asyncio.sleep(1.8)
    await loading.edit_caption("🍃 <b>The wind spirit awakens...</b>", parse_mode="HTML")
    await asyncio.sleep(2.0)
    await loading.edit_caption("✨ <b>The forest is resetting...</b>", parse_mode="HTML")
    await asyncio.sleep(1.2)
    try:
        await tg_app.bot.delete_message(chat_id, loading.message_id)
    except Exception:
        pass

    action_xp, _ = await add_xp(chat_id, first_name, "clear")
    if action_xp:
        asyncio.create_task(send_xp_feedback(chat_id, action_xp))

    await send_full_menu(chat_id, first_name, is_first_time=False)

async def handle_status(chat_id: int):
    async def check_redis():
        try:
            info = await asyncio.wait_for(redis.info("memory"), timeout=3.0)
            used = round(info["used_memory"] / 1024 / 1024, 2)
            key_count = await asyncio.wait_for(redis.dbsize(), timeout=3.0)
            return f"✅ OK ({used} MB, {key_count} keys)"
        except Exception as e:
            return f"❌ {e}"

    async def check_supabase():
        try:
            result = await _sb_get("user_profiles", select="chat_id", limit=1)
            slots_used = 10 - db_sem._value
            flag = "⚠️" if slots_used >= 8 else "✅"
            return f"{flag} OK ({slots_used}/10 slots used)" if result is not None else "❌ Query returned None"
        except Exception as e:
            return f"❌ {e}"

    async def check_telegram():
        try:
            me = await asyncio.wait_for(tg_app.bot.get_me(), timeout=5.0)
            return f"✅ OK (@{me.username})"
        except Exception as e:
            return f"❌ {e}"

    async def check_env():
        required = ["BOT_TOKEN", "SUPABASE_URL", "SUPABASE_KEY", "REDIS_URL", "OWNER_ID", "WEBHOOK_SECRET"]
        missing = [v for v in required if not os.getenv(v)]
        return "✅ All set" if not missing else f"❌ Missing: {', '.join(missing)}"

    redis_status, supabase_status, telegram_status, env_status, maintenance = await asyncio.gather(
        check_redis(),
        check_supabase(),
        check_telegram(),
        check_env(),
        get_maintenance_mode(),
    )

    uptime = datetime.now(pytz.utc) - BOT_START_TIME
    hours, rem = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(rem, 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s"
    maintenance_status = "🔴 ON (users blocked)" if maintenance else "🟢 OFF"

    await tg_app.bot.send_message(
        chat_id,
        f"🌿 <b>System Health Check</b>\n\n"
        f"<b>Redis:</b>       {redis_status}\n"
        f"<b>Supabase:</b>    {supabase_status}\n"
        f"<b>Telegram:</b>    {telegram_status}\n"
        f"<b>Env Vars:</b>    {env_status}\n"
        f"<b>Maintenance:</b> {maintenance_status}\n\n"
        f"<b>Uptime:</b> {uptime_str}",
        parse_mode="HTML",
    )

async def handle_key_feedback(chat_id: int, first_name: str, key_id: str, service_type: str, is_working: bool, query):
    status = "working" if is_working else "not_working"
    emoji  = "✅" if is_working else "🔴"
    label  = "✅ Working" if is_working else "❌ Not Working"

    # Save to Supabase
    await _sb_post("key_reports", {
        "chat_id":      chat_id,
        "first_name":   first_name,
        "key_id":       key_id,
        "service_type": service_type,
        "status":       status,
    })

    # Notify owner in DM
    await tg_app.bot.send_message(
        OWNER_ID,
        f"📋 <b>Key Feedback Report</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 <b>User:</b> {html.escape(str(first_name))} (<code>{chat_id}</code>)\n"
        f"🗂 <b>Service:</b> {service_type.title()}\n\n"
        f"🔑 <b>Key/Cookie:</b>\n<code>{html.escape(str(key_id))}</code>\n\n"
        f"Status: {emoji} <b>{label}</b>\n"
        f"🕐 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode="HTML",
    )

    # Acknowledge user
    await query.answer(
        "✅ Thanks! Feedback sent to the Caretaker." if is_working else
        "❌ Reported! The Caretaker will look into it.",
        show_alert=True,
    )

async def handle_view_reports(chat_id: int):
    reports = await _sb_get(
        "key_reports",
        select="*",
        order="reported_at.desc",
        limit=20,
    )

    if not reports:
        await tg_app.bot.send_message(chat_id, "🌿 No feedback reports yet.")
        return

    lines = []
    for r in reports:
        emoji = "✅" if r.get("status") == "working" else "❌"
        lines.append(
            f"{emoji} <b>{r.get('service_type', '').title()}</b>\n"
            f"└ 👤 {html.escape(str(r.get('first_name', 'Unknown')))}\n"
            f"└ 🔑 <code>{str(r.get('key_id', ''))[:40]}</code>\n"
            f"└ 🕐 {str(r.get('reported_at', ''))[:19]}\n"
        )

    text = "📋 <b>Latest Key Reports</b>\n━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines)
    await tg_app.bot.send_message(chat_id, text, parse_mode="HTML")

async def handle_reset_first_time(chat_id: int):
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the caretaker can reset the forest memory.")
        return
    confirm_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✔️ Yes, Reset Everything", callback_data="confirm_full_reset")],
        [InlineKeyboardButton("❌ No, Cancel",            callback_data="cancel_reset")],
    ])
    await tg_app.bot.send_message(
        chat_id,
        "⚠️ <b>Full Reset Confirmation</b>\n\n"
        "This will permanently reset Level, XP, all stats, and XP history.\n\n"
        "This action cannot be undone.",
        parse_mode="HTML",
        reply_markup=confirm_kb,
    )

# ──────────────────────────────────────────────
# CONFIRMATION: END EVENT
# ──────────────────────────────────────────────
async def handle_confirm_end_event(chat_id: int):
    if chat_id != OWNER_ID:
        return
    confirm_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✔️ Yes, End Event Now", callback_data="yes_end_event")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data="cancel_end_event")],
    ])
    await tg_app.bot.send_message(
        chat_id,
        "⚠️ <b>End Current Event?</b>\n\n"
        "This will immediately remove the event banner for <b>all users</b>.\n"
        "The forest will return to its normal state.\n\n"
        "Are you sure?",
        parse_mode="HTML",
        reply_markup=confirm_kb,
    )

# ──────────────────────────────────────────────
# CONFIRMATION: MAINTENANCE MODE
# ──────────────────────────────────────────────
async def handle_confirm_toggle_maintenance(chat_id: int):
    if chat_id != OWNER_ID:
        return
    current = await get_maintenance_mode()
    status = "🔴 CURRENTLY ON" if current else "🟢 CURRENTLY OFF"
    action = "ENABLE" if not current else "DISABLE"

    confirm_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✔️ Yes, {action} Maintenance Mode", callback_data="yes_toggle_maintenance")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data="cancel_toggle_maintenance")],
    ])

    await tg_app.bot.send_message(
        chat_id,
        f"🛠️ <b>Maintenance Mode {status}</b>\n\n"
        f"This will {action.lower()} maintenance mode for <b>ALL users</b>.\n\n"
        "Are you sure you want to continue?",
        parse_mode="HTML",
        reply_markup=confirm_kb,
    )

# ══════════════════════════════════════════════════════════════════════════════
# CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════════════════════
async def handle_callback(update: Update):
    query     = update.callback_query
    chat_id   = update.effective_chat.id
    first_name = update.effective_user.first_name if update.effective_user else "Wanderer"
    data      = query.data

    # Feedback callbacks answer themselves with show_alert — skip early answer
    FEEDBACK_PREFIXES = ("kfb_ok|", "kfb_bad|", "wkfb_ok|", "wkfb_bad|", "key_feedback_ok|", "key_feedback_bad|", "winoffice_help", "copy_ref_link|")
    if not data.startswith(FEEDBACK_PREFIXES):
        await query.answer()

    # ── MAIN MENU ──
    if data in ("show_main_menu", "main_menu"):
        try:
            await query.message.delete()
        except Exception:
            pass
        await asyncio.sleep(0.8)

        loading = await tg_app.bot.send_animation(
            chat_id=chat_id, animation=LOADING_GIF,
            caption="🌫️ <i>The ancient mist begins to lift once more...</i>", parse_mode="HTML",
        )
        await asyncio.sleep(1.3)
        await loading.edit_caption("🌿 <i>The whispering trees lean in to welcome you home...</i>", parse_mode="HTML")
        await asyncio.sleep(1.3)
        await loading.edit_caption("✨ <i>You stand again in the heart of the Enchanted Clearing...</i>", parse_mode="HTML")
        await asyncio.sleep(1.0)

        profile = await get_user_profile(chat_id)
        if not profile:
            action_xp, _ = await add_xp(chat_id, first_name, "general")
            profile = await get_user_profile(chat_id)

        is_first = not bool(profile.get("has_seen_menu", False)) if profile else True

        if profile and not profile.get("has_seen_menu", False):
            asyncio.create_task(update_has_seen_menu(chat_id))
    
        try:
            await tg_app.bot.delete_message(chat_id, loading.message_id)
        except Exception:
            pass

        await send_full_menu(chat_id, first_name, is_first_time=is_first)
        return

    # ── REGISTRATION GUARD (all other callbacks) ──
    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_animation(
            chat_id=chat_id, animation=HELLO_GIF,
            caption="🌿 <b>A gentle breeze rustles the leaves...</b>\n\n"
                    "To step into the Enchanted Clearing, please press the button below.",
            parse_mode="HTML", reply_markup=kb_start(),
        )
        return

    asyncio.create_task(update_last_active(chat_id))

    # ── INVENTORY ──
    if data == "check_vamt":
        try:
            await query.message.delete()
        except Exception:
            pass
        await tg_app.bot.send_animation(
            chat_id=chat_id, animation=INVENTORY_GIF,
            caption=(
                "📜 <b>Ancient Library — Resource Scrolls</b>\n\n"
                "Choose the type of resource you need today:\n\n"
                "<i>Viewing items earns XP and helps you level up.</i>"
            ),
            parse_mode="HTML", reply_markup=kb_inventory(),
        )

    # ── FULL RESET ──
    elif data == "confirm_full_reset":
        reset_payload = {
            "has_seen_menu":   False, "level": 1, "xp": 0, "total_xp_earned": 0,
            "windows_views": 0, "office_views": 0, "netflix_views": 0, "netflix_reveals": 0,
            "prime_views": 0,   "prime_reveals": 0, "times_cleared": 0, "guidance_reads": 0,
            "lore_reads": 0,    "profile_views": 0,
            "last_active": datetime.now(pytz.utc).isoformat(),
        }
        ok1 = await _sb_patch(f"user_profiles?chat_id=eq.{chat_id}", reset_payload)
        ok2 = await _sb_delete(f"xp_history?chat_id=eq.{chat_id}")
        if ok1 and ok2:
            await query.message.edit_text(
                "✨ <b>Full Forest Reset Complete!</b>\n\nYou are now like a completely new wanderer. 🌱",
                parse_mode="HTML",
            )
        else:
            await query.message.edit_text("❌ Failed to perform full reset.")

    elif data == "cancel_reset":
        await query.message.edit_text("❌ Reset cancelled. Your data is safe.")

    # ── WIN/OFFICE GUIDE ALTER ──
    elif data == "winoffice_help":
        await query.answer(
            "📜 THE ANCIENT SCROLLS GUIDE\n\n"
            "🔵 VAMT: Official Microsoft 'Volume' keys. One key can activate multiple different PCs at once.\n\n"
            "📦 REMAINING: This shows exactly how many activations are left. \n"
            "Example: 'Remaining: 10' means 10 more people can use it!\n\n"
            "⚠️ Tips: Use them quickly before the magic runs out! 🍃",
            show_alert=True,
        )

    # ── INVITE COPY LINK ──
    elif data.startswith("copy_ref_link|"):
        try:
            await query.answer("✅ Link copied to your clipboard!", show_alert=True)
        except:
            pass
        # Send clean link so user can long-press to copy
        link = await get_referral_link(chat_id)
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=f"🌲 <b>Here is your invite link:</b>\n\n<code>{link}</code>\n\n"
                 f"👉 Long-press the link above and tap <b>Copy</b>",
            parse_mode="HTML",
            disable_web_page_preview=True
        )

    # ── INVITE FRIENDS BUTTON (from main menu)
    elif data == "invite_friends":
        await handle_invite(chat_id, first_name)
        return   # important

    # ── INVENTORY FILTERS ──
    elif data.startswith("vamt_filter_"):
        category = data.replace("vamt_filter_", "").lower()

        action_map = {
            "win": "view_windows", "windows": "view_windows",
            "office": "view_office", "netflix": "view_netflix", "prime": "view_prime",
        }

        action = action_map.get(category)
        if action:
            action_xp, _ = await add_xp(chat_id, first_name, action)
            if action_xp:
                asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        await query.message.edit_caption(
            caption=f"✨ <i>Searching the glade for {category.upper()}...</i>", parse_mode="HTML"
        )

        # Steam
        if category == "steam":
            level = profile.get("level", 1)
            if level <= 6:
                msg = ("🎮 <b>Steam Accounts — Public Drop Only</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                       "Steam accounts are released daily at <b>8:00 PM</b>.\n\n"
                       "🔗 https://clydehub.notion.site/Clyde-s-Resource-Hub-ae102294d90682dbaeed81459b131eed")
            elif level <= 8:
                msg = (f"🎮 <b>Steam Accounts — Early Preview</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                       f"🌟 Level {level} Wanderer — you have <b>Early Preview</b> access!")
            elif level == 9:
                msg = ("🎮 <b>Steam — Early Preview + Sunday Double</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                       "🌟 Level 9 → Early Preview + Sunday Double Drop!")
            else:
                msg = "🎮 <b>Steam Accounts — Legend Tier</b>\n\n👑 You have priority access to all Steam accounts."
            await query.message.edit_caption(caption=msg, parse_mode="HTML", reply_markup=kb_back_inventory())
            return

        # Cookie types
        if category in ("netflix", "prime"):
            await show_paginated_cookie_list(category, chat_id, query, page=0)
            return
        
        # ── NEW: First-time guide for Windows / Office ──────────────────
        if category in ("win", "windows", "office"):
            if not await has_seen_winoffice_guide(chat_id):
                cat_label = "Windows" if category in ("win", "windows") else "Office"
                cat_emoji = "🪟" if category in ("win", "windows") else "📑"

                await redis.setex(f"winoffice_pending_cat:{chat_id}", 3600, category)
        
                await query.message.edit_caption(
                    caption=(
                        f"{cat_emoji} <b>Before you open the {cat_label} scrolls...</b>\n\n"
                        "━━━━━━━━━━━━━━━━━━\n\n"
                        "🔵 <b>What is a VAMT Key?</b>\n"
                        "It is a <b>Volume Activation</b> key. Instead of a standard code that only works for one person, these are official Microsoft keys designed to activate multiple PCs. We share them so everyone in the clearing can benefit.\n\n"
                        "━━━━━━━━━━━━━━━━━━\n\n"
                        "📦 <b>What does \"Remaining\" mean?</b>\n"
                        "Because these keys are shared, they have a strict activation limit.\n\n"
                        "• <b>Remaining: 5</b> = Works on exactly 5 more devices.\n"
                        "• <b>Remaining: 0</b> = The key is fully exhausted.\n\n"
                        "<i>Apply them swiftly! The highest remaining keys vanish fast.</i> 🍃\n\n"
                        "━━━━━━━━━━━━━━━━━━"
                    ),
                    parse_mode="HTML",
                    reply_markup=kb_winoffice_guide(),
                )
                return
            else:
                await show_winoffice_keys(chat_id, category, profile, query)

    elif data.startswith("key_feedback_ok|") or data.startswith("key_feedback_bad|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, key_id, service_type = parts
            is_working = data.startswith("key_feedback_ok|")
            await handle_key_feedback(chat_id, first_name, key_id, service_type, is_working, query)

    # Netflix/Prime reveal feedback (uses idx + Redis to avoid 64-byte limit)
    elif data.startswith("kfb_ok|") or data.startswith("kfb_bad|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, service_type, idx_str = parts
            is_working = data.startswith("kfb_ok|")
            real_key = await redis.get(f"reveal_key:{chat_id}:{service_type}:{idx_str}")
            key_id = real_key if real_key else idx_str
            await handle_key_feedback(chat_id, first_name, key_id, service_type, is_working, query)

    # Windows/Office per-item feedback (uses token + Redis)
    elif data.startswith("wkfb_ok|") or data.startswith("wkfb_bad|"):
        parts = data.split("|", 1)
        if len(parts) == 2:
            _, token = parts
            is_working = data.startswith("wkfb_ok|")
            stored = await redis.get(f"winkey:{token}")
            if stored:
                real_key, svc = stored.split("||", 1)
            else:
                real_key, svc = token, "windows"
            await handle_key_feedback(chat_id, first_name, real_key, svc, is_working, query)

    # ── HISTORY PAGINATION ──
    elif data.startswith("history_page_"):
        try:
            page = int(data.split("_")[2])
            await query.message.delete()
        except Exception:
            page = 0
        await handle_history(chat_id, first_name, page=page)

    # ── GUIDANCE (3 pages) ──
    elif data == "help" or data.startswith("guidance_page_"):
        if data == "help":
            try:
                await query.message.delete()
            except Exception:
                pass
            action_xp, _ = await add_xp(chat_id, first_name, "guidance")
            if action_xp > 0:
                asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        page = 1
        if data.startswith("guidance_page_"):
            try:
                page = int(data.split("_")[2])
            except Exception:
                page = 1

        level_req_text = "\n".join(
            f"• Level {lvl} → {get_cumulative_xp_for_level(lvl):,} XP"
            for lvl in range(2, 11)
        )

        pages = {
            1: (
                "<b>❓ Guidance - Page 1/3</b>\n\n"
                "🌿 <b>How to Navigate the Clearing</b>\n"
                "• Tap any button to explore the paths\n"
                "• Use /menu to return here anytime\n"
                "• Use /clear to renew your path\n\n"
                "📜 <b>Available Commands</b>\n"
                "• /start — Begin your journey\n"
                "• /menu — Return to the Clearing\n"
                "• /profile — View your Forest Profile\n"
                "• /mystats — Detailed statistics\n"
                "• /leaderboard — See Top Wanderers\n"
                "• /myid — Reveal your Forest ID\n"
                "• /clear — Cleanse the clearing\n"
                "• /feedback — Message the caretaker\n"
                "• /update — View patch notes\n\n"
                "🌲 <b>Treasures You Can Discover</b>\n"
                "• 🪄 Spirit Treasures — Steam accounts\n"
                "• 📜 Ancient Scrolls — Learning guides\n"
                "• 🌿 Forest Inventory — Keys & Cookies\n\n"
                "<i>Tap Next → to learn about the Leveling System</i>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("Next ⇀", callback_data="guidance_page_2")],
                    [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")],
                ]),
            ),
            2: (
                "<b>❓ Guidance - Page 2/3</b>\n\n"
                "✨ <b>Forest Leveling System</b>\n\n"
                "🪟 <b>Windows & Office Keys</b>\n"
                "• Lv1: 2 • Lv2-3: 3 • Lv4-5: 4\n"
                "• Lv6: 6 • Lv7: 8 • Lv8: 10\n"
                "• Lv9: 13 • Lv10+: Unlimited\n\n"
                "🍿 <b>Netflix Premium Cookies</b>\n"
                "• Lv1: 2 • Lv2-3: 3 • Lv4-5: 5\n"
                "• Lv6: 7 • Lv7: 9 • Lv8: 12\n"
                "• Lv9: 15 • Lv10+: Unlimited\n\n"
                "🎥 <b>PrimeVideo Premium Cookies</b>\n"
                "• Lv1: 2 • Lv2-3: 2 • Lv4-5: 3\n"
                "• Lv6: 4 • Lv7: 5 • Lv8: 7\n"
                "• Lv9: 9 • Lv10+: Unlimited\n\n"
                "<i>Tap Next → for Steam & XP Rewards</i>",
                InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("↼ Previous", callback_data="guidance_page_1"),
                        InlineKeyboardButton("Next ⇀",     callback_data="guidance_page_3"),
                    ],
                    [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")],
                ]),
            ),
            3: (
                f"<b>❓ Guidance - Page 3/3</b>\n\n"
                "🎮 <b>Steam Accounts</b>\n"
                "• Lv1-6: Public Drop Only (Website)\n"
                "• Lv7-8: Early Preview\n"
                "• Lv9: Early Preview + Sunday Double\n"
                "• Lv10+: 👑 Legend Tier\n\n"
                "<b>XP Rewards:</b>\n"
                "• Viewing any list → <b>+8 XP</b>\n"
                "• Revealing a cookie → <b>+14 XP</b>\n"
                "• /profile or /clear → <b>+6 XP</b>\n"
                "• First Guidance / Lore → <b>+10 XP</b>\n"
                "• Returning Guidance / Lore → <b>+2 XP</b>\n"
                "• Daily Login Bonus → <b>+10–30 XP</b> (streak scales)\n\n"
                f"<b>Cumulative XP Requirements:</b>\n\n{level_req_text}\n\n"
                "<i>The more you explore, the more the forest opens up to you.</i> 🍃✨",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("↼ Previous", callback_data="guidance_page_2")],
                    [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")],
                ]),
            ),
        }

        text, keyboard = pages.get(page, pages[1])

        if data == "help":
            msg = await tg_app.bot.send_animation(
                chat_id=chat_id, animation=GUIDANCE_GIF,
                caption=text, parse_mode="HTML", reply_markup=keyboard,
            )
            await _remember(chat_id, msg.message_id)
            if not profile.get("has_seen_menu", False):
                asyncio.create_task(update_has_seen_menu(chat_id))
        else:
            try:
                await query.message.edit_caption(caption=text, parse_mode="HTML", reply_markup=keyboard)
            except Exception:
                msg = await tg_app.bot.send_animation(
                    chat_id=chat_id, animation=GUIDANCE_GIF,
                    caption=text, parse_mode="HTML", reply_markup=keyboard,
                )
                await _remember(chat_id, msg.message_id)

    # ── COOKIE PAGINATION ──
    elif "_page_" in data and data.split("_page_")[0] in ("netflix", "prime"):
        try:
            service_type = data.split("_page_")[0]
            new_page     = int(data.split("_page_")[1])
        except Exception:
            return
        loading = await tg_app.bot.send_animation(
            chat_id=chat_id, animation=INVENTORY_GIF,
            caption=f"{'🍿' if service_type == 'netflix' else '🎥'} <i>Loading {service_type.title()}...</i>",
            parse_mode="HTML",
        )
        class _FQ:
            message = loading
        await show_paginated_cookie_list(service_type, chat_id, _FQ(), page=new_page)
        try:
            await query.message.delete()
        except Exception:
            pass

    # ── REVEAL COOKIE ──
    elif data.startswith("reveal_netflix|") or data.startswith("reveal_prime|"):
        try:
            parts        = data.split("|")
            service_type = "netflix" if parts[0] == "reveal_netflix" else "prime"
            idx          = int(parts[1])
            page         = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            await query.answer("Invalid selection", show_alert=True)
            return
        await reveal_cookie(service_type, chat_id, first_name, query, idx, page)

    # ── BACK TO COOKIE LIST (with cleanup) ──
    elif data.startswith("back_to_netflix_list") or data.startswith("back_to_prime_list"):
        service_type = "netflix" if data.startswith("back_to_netflix_list") else "prime"
        try:
            page = int(data.split("|")[1]) if "|" in data else 0
        except Exception:
            page = 0

        # NEW — fetch from Redis, delete from Telegram, clean up key
        stored_msg_id = await redis.get(f"reveal_msg:{chat_id}:{service_type}")
        if stored_msg_id:
            try:
                await tg_app.bot.delete_message(chat_id, int(stored_msg_id))
            except Exception:
                pass
            await redis.delete(f"reveal_msg:{chat_id}:{service_type}")

        # Show fresh list
        loading = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=INVENTORY_GIF,
            caption=f"{'🍿' if service_type == 'netflix' else '🎥'} <i>Loading {service_type.title()} Cookies...</i>",
            parse_mode="HTML",
        )
        class FakeQuery:
            message = loading
        await show_paginated_cookie_list(service_type, chat_id, FakeQuery(), page=page)

    # ── LORE / ABOUT ──
    elif data == "about":
        try:
            await query.message.delete()
        except Exception:
            pass
        action_xp, _ = await add_xp(chat_id, first_name, "lore")
        if action_xp > 0:
            asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        loading = await tg_app.bot.send_animation(
            chat_id=chat_id, animation=LOADING_GIF,
            caption="🌌 <i>The oldest spirits of the forest begin to stir...</i>", parse_mode="HTML",
        )
        await asyncio.sleep(1.2)
        await loading.edit_caption("📜 <i>They gather beneath the ancient canopy...</i>", parse_mode="HTML")
        await asyncio.sleep(1.3)
        await loading.edit_caption("✨ <i>The story of this sacred clearing gently unfolds...</i>", parse_mode="HTML")
        await asyncio.sleep(1.0)

        lore = (
            "<b>🌿 About Clyde's Resource Hub</b>\n\n"
            "This peaceful sanctuary was created to make useful digital resources easy and "
            "stress-free to access — all wrapped in a calm, Studio Ghibli-inspired forest theme.\n\n"
            "You can find:\n"
            "• Windows & Office activation keys\n"
            "• Netflix premium cookies\n"
            "• PrimeVideo premium cookies\n"
            "• Steam accounts\n"
            "• Learning guides\n\n"
            "<b>Current Rewards:</b> View lists = +8 XP | Reveal Netflix = +14 XP\n\n"
            "<i>May this small enchanted clearing bring you both practical resources and a moment of peace.</i> 🍃✨"
        )
        final = await tg_app.bot.send_animation(
            chat_id=chat_id, animation=ABOUT_GIF,
            caption=lore, parse_mode="HTML", reply_markup=kb_back(),
        )
        try:
            await tg_app.bot.delete_message(loading.chat_id, loading.message_id)
        except Exception:
            pass
        await _remember(chat_id, final.message_id)

    # ── MAINTENANCE CONFIRMATION ──
    elif data in ("confirm_toggle_maintenance", "yes_toggle_maintenance", "cancel_toggle_maintenance"):
        if chat_id != OWNER_ID:
            await query.answer("🌿 Only the Forest Caretaker may do this.", show_alert=True)
            return
        if data == "confirm_toggle_maintenance":
            await handle_confirm_toggle_maintenance(chat_id)
        elif data == "yes_toggle_maintenance":
            await handle_toggle_maintenance(chat_id)
            await query.message.edit_text("✅ <b>Maintenance mode updated.</b> 🌿", parse_mode="HTML")
        elif data == "cancel_toggle_maintenance":
            await query.message.edit_text("❌ Maintenance mode change cancelled.")
    
    # ── END EVENT CONFIRMATION ──
    elif data in ("confirm_end_event", "yes_end_event", "cancel_end_event"):
        if chat_id != OWNER_ID:
            await query.answer("🌿 Only the Forest Caretaker may do this.", show_alert=True)
            return
        if data == "confirm_end_event":
            await handle_confirm_end_event(chat_id)
        elif data == "yes_end_event":
            await handle_end_event(chat_id)
            await query.message.edit_text("✅ <b>Event ended successfully.</b> 🌿", parse_mode="HTML")
        elif data == "cancel_end_event":
            await query.message.edit_text("❌ Event ending cancelled.")

    # ── CARETAKER ADMIN ──
    elif data.startswith("caretaker_"):
        if chat_id != OWNER_ID:
            await query.answer("🌿 Only the Forest Caretaker may enter this sacred glade.", show_alert=True)
            return

        if data == "caretaker_addupdate":
            await tg_app.bot.send_animation(
                chat_id=chat_id, animation=ABOUT_GIF,
                caption=(
                    "📜 <b>Add New Patch Note</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Reply with:\n\n"
                    "<code>/addupdate\nYour Title Here\nYour full description here...</code>"
                ),
                parse_mode="HTML",
            )
        elif data == "caretaker_addevent":
            await tg_app.bot.send_message(
                chat_id,
                "🎉 <b>Create New Event</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                "Reply with:\n\n"
                "<code>/addevent\nEvent Title\nApril 12, 2026\nYour description here...\n"
                "Can span multiple lines!\nbonus:netflix_double</code>\n\n"
                "• Line 1 = Title\n"
                "• Line 2 = Date\n"
                "• Line 3+ = Description (any length)\n"
                "• Last line = <code>bonus:netflix_double</code> <i>(optional)</i>\n\n"
                "🎁 <b>Available bonus types:</b>\n"
                "• <code>bonus:netflix_double</code> — doubles Netflix slots\n"
                "• <code>bonus:netflix_max</code> — maximizes Netflix slots\n"
                "• <i>Omit the bonus line for a normal event</i>\n\n"
                "<i>This will replace any currently active event.</i>",
                parse_mode="HTML",
            )

        elif data == "caretaker_viewevent":
            await handle_view_event(chat_id)
        elif data == "caretaker_viewfeedback":
            await handle_view_feedback(chat_id)
        elif data == "caretaker_flushcache":
            await handle_flushcache(chat_id)   
        elif data == "caretaker_resetfirst":
            await handle_reset_first_time(chat_id)

    elif data == "winoffice_got_it":
        await mark_winoffice_guide_seen(chat_id)
        
        # Read which category triggered the guide
        pending_cat = await redis.get(f"winoffice_pending_cat:{chat_id}") or "win"
        await redis.delete(f"winoffice_pending_cat:{chat_id}")
        await show_winoffice_keys(chat_id, pending_cat, profile, query)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN UPDATE PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════
async def show_winoffice_keys(chat_id: int, category: str, profile: dict, query):
    """Shared key display logic for Windows and Office."""
    pending_cat = category
    cat_label = "Windows" if pending_cat in ("win", "windows") else "Office"
    cat_emoji = "🪟" if pending_cat in ("win", "windows") else "📑"

    loading = await send_loading(
        chat_id,
        f"{cat_emoji} <i>Opening the {cat_label} key scroll from the ancient library...</i>"
    )

    await query.message.edit_caption(
        caption=f"{cat_emoji} <i>Opening the {cat_label} key scroll...</i>",
        parse_mode="HTML",
    )
    await asyncio.sleep(0.8)

    user_level = profile.get("level", 1)
    vamt = await get_vamt_data()
    if not vamt:
        await send_supabase_error(chat_id, query)
        await loading.delete()
        return

    if pending_cat in ("win", "windows"):
        filtered = [
            item for item in vamt
            if any(x in str(item.get("service_type", "")).lower() for x in ("windows", "win"))
            and int(item.get("remaining") or 0) > 0
        ]
    else:
        filtered = [
            item for item in vamt
            if "office" in str(item.get("service_type", "")).lower()
            and int(item.get("remaining") or 0) > 0
        ]

    if not filtered:
        await query.message.edit_caption(
            caption=f"🍃 No {cat_label} keys available right now. Check back later!",
            parse_mode="HTML",
            reply_markup=kb_back_inventory(),
        )
        return

    max_items = get_max_items(pending_cat, user_level)
    filtered.sort(key=lambda x: (str(x.get("service_type", "")), str(x.get("key_id", ""))))
    display_items = filtered[:max_items]

    report = f"{cat_emoji} <b>{cat_label} Activation Keys</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    report += f"📋 <b>{len(display_items)} key(s) available for your level</b>\n\n"

    for item in display_items:
        stock = str(item.get("remaining", 0))
        report += (
            f"✨ <b>{item.get('service_type', 'Unknown')}</b>\n"
            f"└ 🔑 Key: <code>{item.get('key_id', 'HIDDEN')}</code>\n"
            f"└ 📦 Remaining: <b>{stock}</b>\n\n"
        )

    report += (
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🌿 Level {user_level} → Up to <b>{max_items}</b> {cat_label} keys\n\n"
        "Tap ✅ if it worked, ❌ if it did not — Caretaker will be notified. 🍃"
    )

    buttons = []
    for item in display_items:
        raw_key = str(item.get("key_id", "")).strip()
        svc = str(item.get("service_type", pending_cat)).strip()
        short = raw_key[:20] + "…" if len(raw_key) > 20 else raw_key
        token = f"{chat_id}:{raw_key[:40]}"
        await redis.setex(f"winkey:{token}", 3600, f"{raw_key}||{svc}")
        buttons.append([
            InlineKeyboardButton(f"✅ {short}", callback_data=f"wkfb_ok|{token}"),
            InlineKeyboardButton(f"❌", callback_data=f"wkfb_bad|{token}"),
        ])

    buttons.append([InlineKeyboardButton(
        "❓ What is VAMT / Remaining?", callback_data="winoffice_help"
    )])
    buttons.append([InlineKeyboardButton(
        "⬅️ Back to Inventory", callback_data="check_vamt"
    )])

    await query.message.edit_caption(
        caption=report,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

    try:
        await loading.delete()
    except Exception:
        pass

async def process_update(update_data: dict):
    global COLD_START

    update = Update.de_json(update_data, tg_app.bot)

    # ── COLD START CUTE MESSAGE (only once after Render spin-down) ──
    if COLD_START:
        chat_id = update.effective_chat.id if update.effective_chat else None
        if chat_id:
            try:
                await tg_app.bot.send_message(
                    chat_id=chat_id,
                    text="Yawnnn~ 😴💤\nThe forest just woke me up!\nReady for you now, wanderer ✨🍃",
                    parse_mode="HTML"
                )
            except Exception as e:
                print(f"⚠️ Cold-start message failed: {e}")
        
        COLD_START = False

    # ── Maintenance mode ──
    if await get_maintenance_mode():
        chat_id = update.effective_chat.id if update.effective_chat else None
        if chat_id and chat_id != OWNER_ID:
            try:
                if update.message:
                    await tg_app.bot.send_message(chat_id, MAINTENANCE_MESSAGE, parse_mode="HTML")
                elif update.callback_query:
                    await update.callback_query.answer(
                        "🌿 The Enchanted Clearing is under maintenance.\nPlease come back later!",
                        show_alert=True,
                    )
            except Exception:
                pass
            return

    # ── Callback queries ──
    if update.callback_query:
        await handle_callback(update)
        return

    # ── DOCUMENT HANDLER (Admin TXT key upload) ──
    if update.message and update.message.document:
        await handle_document(update)
        return

    # ── Text messages ──
    if not (update.message and update.message.text):
        return

    raw       = update.message.text.strip()
    text      = raw.lower()
    chat_id   = update.effective_chat.id
    msg_id    = update.message.message_id
    first_name = update.effective_user.first_name if update.effective_user else "Traveler"

    await _remember(chat_id, msg_id)

    # Registration guard (except /start)
    if not text.startswith("/start"):
        profile = await get_user_profile(chat_id)
        if not profile:
            await tg_app.bot.send_animation(
                chat_id=chat_id, animation=HELLO_GIF,
                caption=(
                    "<b>🌲 You stand at the edge of a mysterious forest.</b>\n\n"
                    "The ancient trees watch you with quiet curiosity.\n\n"
                    "To step into the Enchanted Clearing..."
                ),
                parse_mode="HTML", reply_markup=kb_start(),
            )
            return

    asyncio.create_task(update_last_active(chat_id))

    # ── Command dispatch ──
    if text.startswith("/start"):
        referral_stored = True  # default

        if " " in raw:
            payload = raw.split(maxsplit=1)[1]
            if payload.startswith("ref_"):
                try:
                    referrer_id = int(payload[4:])
                    referral_stored = await store_pending_referral(chat_id, referrer_id)

                    if not referral_stored:
                        # Self-referral → show temporary message
                        asyncio.create_task(
                            send_temporary_message(
                                chat_id,
                                "🌿 Nice try, wanderer!\n\n"
                                "You can't refer yourself in the forest 🍃\n"
                                "The trees are watching 👀",
                                duration=10
                            )
                        )
                except:
                    pass

        await send_initial_welcome(chat_id, first_name)

    elif text.startswith("/uploadkeys"):
        await handle_uploadkeys_command(chat_id)

    elif text.startswith("/addevent"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the caretaker can create events.")
            return

        body = raw[9:].strip()
        if not body:
            await tg_app.bot.send_message(chat_id, "📌 Usage:\n`/addevent\nTitle\nDate\nDescription\nbonus:netflix_double`")
            return

        lines = body.split("\n")

        if len(lines) < 3:
            await tg_app.bot.send_message(
                chat_id, "❌ Need at least 3 lines: title, date, description."
            )
            return
        
        title      = lines[0].strip()
        event_date = lines[1].strip()

        # Extract bonus_type if last line starts with "bonus:"
        bonus_type = ""
        desc_lines = lines[2:]
        if desc_lines and desc_lines[-1].lower().startswith("bonus:"):
            bonus_type = desc_lines[-1].split(":", 1)[1].strip()
            desc_lines = desc_lines[:-1]

        description = "\n".join(desc_lines).strip()

        if not description:
            await tg_app.bot.send_message(chat_id, "❌ Description cannot be empty.")
            return

        await handle_add_event(chat_id, title, description, event_date, bonus_type)

    elif text.startswith("/forest"):
        await handle_info(chat_id)
        
    elif text.startswith("/health"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can check the system status.")
            return
        await handle_status(chat_id)
    
    elif text.startswith("/history"):
        await handle_history(chat_id, first_name)

    elif text.startswith("/leaderboard"):
        await handle_leaderboard(chat_id)

    elif text.startswith("/profile"):
        await handle_profile(chat_id, first_name)

    elif text.startswith("/viewreports"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can view reports.")
            return
        await handle_view_reports(chat_id)

    elif text.startswith("/caretaker"):
        await handle_caretaker(chat_id, first_name)

    elif text.startswith("/mystats"):
        await handle_stats(chat_id, first_name)

    elif text.startswith("/flushcache"):
        await handle_flushcache(chat_id)

    elif text.startswith("/menu"):
        profile = await get_user_profile(chat_id)
        is_first = not bool(profile.get("has_seen_menu", False)) if profile else True

        # === FIX FOR FIX 6 ===
        if profile and not profile.get("has_seen_menu", False):
            asyncio.create_task(update_has_seen_menu(chat_id))
        # ======================

        await send_full_menu(chat_id, first_name, is_first_time=is_first)

    elif text.startswith("/myid"):
        await send_myid(chat_id)

    elif text.startswith("/resetguide"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can use this.")
            return
        try:
            target_id = int(text.split()[1]) if len(text.split()) > 1 else chat_id
            deleted1 = await redis.delete(f"seen_winoffice_guide:{target_id}")
            deleted2 = await redis.delete(f"winoffice_pending_cat:{target_id}")
            await tg_app.bot.send_message(
                chat_id,
                f"✅ Guide reset for <code>{target_id}</code>\n\n"
                f"Deleted: {deleted1 + deleted2} key(s)",
                parse_mode="HTML"
            )
        except Exception as e:
            await tg_app.bot.send_message(chat_id, f"❌ Error: {e}")

    elif text.startswith("/testdaily"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can use this.")
            return
        try:
            target_id = int(text.split()[1]) if len(text.split()) > 1 else chat_id
            deleted = await redis.delete(f"daily_bonus:{target_id}")
            await tg_app.bot.send_message(
                chat_id,
                f"✅ Daily bonus key for <code>{target_id}</code> has been reset.\n\n"
                f"Deleted: {deleted} key(s)",
                parse_mode="HTML"
            )
        except Exception as e:
            await tg_app.bot.send_message(chat_id, f"❌ Error: {e}")

    elif text.startswith("/clear"):
        await handle_clear(chat_id, msg_id, first_name)

    elif text.startswith("/feedback"):
        feedback_text = raw[9:].strip()   # preserve original casing
        if not feedback_text:
            await tg_app.bot.send_message(
                chat_id,
                "🌿 Please write your feedback after the command.\n\nExample: `/feedback I love the forest!`",
            )
        elif len(feedback_text) > 500:
            await tg_app.bot.send_message(
                chat_id,
                f"🍃 <b>Your message is too long!</b>\n\n"
                f"The forest can only carry <b>500 characters</b> at a time.\n"
                f"Your message is <b>{len(feedback_text)}</b> characters.\n\n"
                f"<i>Please shorten it and try again.</i> 🌿",
                parse_mode="HTML",
            )
        else:
            await handle_feedback(chat_id, first_name, feedback_text)

    elif text.startswith("/setforestinfo"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can change forest info.")
            return
        parts = raw[14:].strip().split(maxsplit=1)
        if len(parts) < 2:
            await tg_app.bot.send_message(
                chat_id,
                "📝 Usage:\n`/setforestinfo 1.4.5 April 10, 2026 · 03:58 PM`\n\n"
                "• First word = version\n• Everything after = date & time",
            )
            return
        await set_bot_info(parts[0].strip(), parts[1].strip(), chat_id)

    elif text.startswith("/addupdate"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the caretaker can add updates.")
            return

        body = raw[10:].strip()
        if not body:
            await tg_app.bot.send_message(
                chat_id,
                "📌 Usage:\n"
                "`/addupdate\nTitle Here\nYour full description...`\n\n"
                "or\n"
                "`/addupdate | Title | Your full description...`",
            )
            return

        title = ""
        content = ""

        if "\n" in body:
            lines = body.split("\n", 1)
            title = lines[0].strip()
            content = lines[1].strip() if len(lines) > 1 else ""
        elif "|" in body:
            parts = [p.strip() for p in body.split("|") if p.strip()]
            if len(parts) >= 2:
                title = parts[0]
                content = " | ".join(parts[1:])
            else:
                title = ""
                content = ""

        if not title or not content:
            await tg_app.bot.send_message(chat_id, "❌ Title and content cannot be empty.")
            return

        await add_new_update(title, content, chat_id)

    elif text.startswith("/invite"):
        await handle_invite(chat_id, first_name)

    elif text.startswith(("/updates", "/update")):
        await handle_updates(chat_id)

    elif text.startswith(("/viewfeedback", "/feedbacks")):
        await handle_view_feedback(chat_id)

    elif text.startswith(("/resetfirst", "/reset")):
        if chat_id == OWNER_ID:
            await handle_reset_first_time(chat_id)
        else:
            await tg_app.bot.send_message(chat_id, "🌿 Only the caretaker can reset the forest memory.")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        log_level="info",
    )
