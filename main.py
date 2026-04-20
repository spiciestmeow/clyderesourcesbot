import os
import asyncio
import re
import json
import random
import zipfile
import io
import html
import httpx
import pytz
import redis.asyncio as aioredis
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from telegram import BotCommandScopeChat
from io import BytesIO
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import PlainTextResponse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from deep_translator import GoogleTranslator
from gifs import *
# ──────────────────────────────────────────────
# AUTO TRANSLATION SYSTEM — English + Tagalog + Bisaya (using deep-translator)
# ──────────────────────────────────────────────
SUPPORTED_LANGUAGES = {
    "en":  ("🇬🇧", "English"),
    "tl":  ("🇵🇭", "Tagalog"),
    "ceb": ("🇵🇭", "Bisaya"),
}

async def get_user_language(chat_id: int) -> str:
    profile = await get_user_profile(chat_id)
    return profile.get("preferred_language", "en") if profile else "en"

async def set_user_profile_gif(chat_id: int, file_id: str) -> bool:
    """Save custom GIF as profile logo — now uses safe UPSERT"""
    payload = {
        "chat_id": chat_id,
        "profile_gif_id": file_id
    }
    
    success = await _sb_upsert(
        "user_profiles", 
        payload, 
        on_conflict="chat_id"
    )
    
    if success:
        print(f"✅ Profile GIF saved for user {chat_id} → {file_id[:30]}...")
        # Extra safety: verify it actually wrote to DB
        profile = await get_user_profile(chat_id)
        if profile and profile.get("profile_gif_id") == file_id:
            print(f"✅ Verified in Supabase: profile_gif_id = {file_id}")
            return True
        else:
            print(f"🔴 Save succeeded but verification failed for {chat_id}")
    else:
        print(f"🔴 Failed to save GIF for {chat_id}")
    
    return success

async def set_user_language(chat_id: int, lang_code: str):
    if lang_code not in SUPPORTED_LANGUAGES:
        lang_code = "en"
    await _sb_patch(f"user_profiles?chat_id=eq.{chat_id}", {"preferred_language": lang_code})

async def translate_text(text: str, target_lang: str) -> str:
    """Auto translate using Google Translate (safe & compatible)"""
    if not text or target_lang == "en" or not text.strip():
        return text
    
    try:
        # Run in background thread so it doesn't block your async bot
        translated = await asyncio.to_thread(
            lambda: GoogleTranslator(source='auto', target=target_lang).translate(text)
        )
        return translated
    except Exception:
        return text  # fallback to original text if anything fails
    
# ══════════════════════════════════════════════════════════════════════════════
# STEAM AUTOMATED DISTRIBUTION SYSTEM
# ══════════════════════════════════════════════════════════════════════════════

STEAM_DAILY_LIMITS = {
    7:  1,
    8:  1,
    9:  2,
    10: 999,
}

def get_early_access_hour(level: int) -> int | None:
    """Returns the hour (Manila time) when this level gets early access. None = no early access."""
    if level >= 10:
        return 0   # immediate — sees it as soon as uploaded
    elif level == 9:
        return 8  # 8 AM
    elif level == 8:
        return 12  # 12 PM
    elif level == 7:
        return 16  # 4 PM
    else:
        return None  # Lv1-6 = website only, no bot access

def is_early_access_time(level: int) -> bool:
    """Returns True if current Manila time has passed this level's early access hour."""
    hour = get_early_access_hour(level)
    if hour is None:
        return False
    manila = pytz.timezone("Asia/Manila")
    return datetime.now(manila).hour >= hour

def get_time_until_early_access(level: int) -> tuple[int, int]:
    """Returns (hours, minutes) until this level's early access opens."""
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    access_hour = get_early_access_hour(level)
    
    if access_hour is None:
        return 0, 0

    target = now.replace(hour=access_hour, minute=0, second=0, microsecond=0)
    if now >= target:
        return 0, 0  # already unlocked

    diff = target - now
    hours = int(diff.total_seconds() // 3600)
    mins = int((diff.total_seconds() % 3600) // 60)
    return hours, mins

def get_steam_tier(level: int) -> str:
    if level >= 10:
        return "legend"
    elif level == 9:
        return "early_sunday"
    elif level in (7, 8):
        return "early"
    else:
        return "public"

def is_public_drop_time() -> bool:
    """Returns True if Manila time is past 8 PM"""
    manila = pytz.timezone("Asia/Manila")
    return datetime.now(manila).hour >= 20

def is_sunday_manila() -> bool:
    manila = pytz.timezone("Asia/Manila")
    return datetime.now(manila).weekday() == 6

def is_sunday_noon_manila() -> bool:
    """Returns True if Sunday and past 12 PM Manila"""
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    return now.weekday() == 6 and now.hour >= 12

def get_time_until_drop() -> tuple[int, int]:
    """Returns (hours, minutes) until next drop"""
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)

    # Sunday before noon — show time until noon drop
    if now.weekday() == 6 and now.hour < 12:
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        diff = target - now
        hours = int(diff.total_seconds() // 3600)
        mins = int((diff.total_seconds() % 3600) // 60)
        return hours, mins

    # Default: next 8 PM drop
    target = now.replace(hour=20, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    diff = target - now
    hours = int(diff.total_seconds() // 3600)
    mins = int((diff.total_seconds() % 3600) // 60)
    return hours, mins

async def release_daily_steam_accounts():
    """Just returns count of accounts that are now past their release_at time"""
    manila = pytz.timezone("Asia/Manila")
    now_iso = datetime.now(manila).astimezone(pytz.utc).isoformat()

    newly_visible = await _sb_get(
        "steamCredentials",
        **{
            "select": "*",
            "status": "eq.Available",
            "release_at": f"lte.{now_iso}",
        }
    ) or []

    return len(newly_visible)

async def get_steam_claims_today(chat_id: int) -> int:
    """How many accounts this user claimed today"""
    manila = pytz.timezone("Asia/Manila")
    today_start = datetime.now(manila).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(pytz.utc).isoformat()

    data = await _sb_get(
        "steam_claims",
        **{
            "chat_id": f"eq.{chat_id}",
            "claimed_at": f"gte.{today_start}",
            "select": "id",
        }
    ) or []
    return len(data)

async def claim_steam_account(chat_id: int, first_name: str, account_email: str, game_name: str) -> bool:
    existing_claim = await _sb_get(
        "steam_claims",
        **{
            "chat_id": f"eq.{chat_id}",
            "account_email": f"eq.{account_email}",
            "select": "id",
        }
    ) or []

    if existing_claim:
        return False
        
    await _sb_post("steam_claims", {
        "chat_id": chat_id,
        "first_name": first_name,
        "account_email": account_email,
        "game_name": game_name,
    })
    return True
    
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

DISPLAY_NAME_MAP = {
    "netflix": "Netflix Cookie",
    "prime":   "PrimeVideo Cookie",
    "office":  "Office Key",
    "windows": "Win Key",
    "win":     "Win Key",
}

# ──────────────────────────────────────────────
# NEW HELPER: Fix Windows vs "win" key mismatch
# ──────────────────────────────────────────────
def normalize_view_category(cat: str) -> str:
    """Make sure "win" and "windows" always use the same Redis key"""
    c = str(cat).lower().strip()
    if c in ("win", "windows"):
        return "windows"
    return c  # office stays "office"

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
    "steam_claim": 86400,
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


# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL SINGLETONS  (initialised in lifespan, never re-created)
# ══════════════════════════════════════════════════════════════════════════════
tg_app: Application = None
http:   httpx.AsyncClient = None
redis_client: aioredis.Redis = None
db_sem  = asyncio.Semaphore(10)   # cap concurrent Supabase calls
forest_memory: dict[int, list[int]] = {}  # fallback only — Redis is primary
BOT_START_TIME = datetime.now(pytz.utc)
BOT_USERNAME: str | None = None

# ──────────────────────────────────────────────
# LIFESPAN  (replaces @app.on_event)
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg_app, http, redis_client

    # Shared HTTP connection pool (20 connections max)
    http = httpx.AsyncClient(
        timeout=12.0,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )

    # Redis for cooldowns + VAMT cache
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    # Telegram application
    tg_app = Application.builder().token(TOKEN).build()
    await tg_app.initialize()
    await tg_app.start()

    # ── ADD THIS ──
    asyncio.create_task(steam_daily_release_scheduler())
    print("⏰ Steam daily release scheduler started")

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
    await redis_client.aclose()
    print("🌿 Bot shut down cleanly")

app = FastAPI(lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

async def steam_daily_release_scheduler():
    """Runs forever — triggers steam releases at 8 PM daily and 12 PM on Sundays"""
    manila = pytz.timezone("Asia/Manila")

    while True:
        now = datetime.now(manila)

        # Build list of today's upcoming drop times
        candidates = []

        # 8 PM daily drop
        drop_8pm = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now < drop_8pm:
            candidates.append(("8pm", drop_8pm))

        # 12 PM Sunday drop
        if now.weekday() == 6:
            drop_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
            if now < drop_noon:
                candidates.append(("noon", drop_noon))

        if candidates:
            next_label, next_drop = min(candidates, key=lambda x: x[1])
        else:
            # Both passed today — sleep until 8 PM tomorrow
            next_label = "8pm"
            next_drop = (now + timedelta(days=1)).replace(
                hour=20, minute=0, second=0, microsecond=0
            )

        wait_seconds = (next_drop - now).total_seconds()
        print(f"⏰ Next steam drop ({next_label}) in {wait_seconds/3600:.1f}h")
        await asyncio.sleep(wait_seconds)
        await asyncio.sleep(5) 
        count = await release_daily_steam_accounts()

        label_text = (
            "🌟 Sunday Noon Drop!"
            if next_label == "noon"
            else "🎮 Daily 8PM Drop!"
        )

        try:
            await tg_app.bot.send_message(
                OWNER_ID,
                f"🎮 <b>{label_text}</b>\n\n"
                f"✅ <b>{count}</b> accounts are now publicly visible.\n"
                f"🕗 Released at "
                f"{'12:00 PM' if next_label == 'noon' else '8:00 PM'} Manila time.\n\n"
                f"<i>Level 1–6 users can now see today's accounts on the website.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass

        await asyncio.sleep(60)

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

# ──────────────────────────────────────────────
# NOTION WEBHOOK FROM SUPABASE (Minimal version)
# ──────────────────────────────────────────────
@app.post("/webhook/steam-live")
async def steam_to_notion_webhook(request: Request):
    secret = request.headers.get("X-Supabase-Secret")
    if secret != WEBHOOK_SECRET:
        return PlainTextResponse("Unauthorized", status_code=401)

    data = await request.json()
    record = data.get("record") or {}

    if record.get("action") != "live":
        return PlainTextResponse("Ignored - action not 'live'", status_code=200)

    # Minimal payload - only what you asked for
    success = await send_to_notion(
        title=record.get("game_name") or record.get("email") or "Steam Account",
        properties={
            "Game Name": {
                "title": [{"text": {"content": record.get("game_name") or "Unknown"}}]
            },
            "Email": {
                "rich_text": [{"text": {"content": record.get("email", "")}}]
            },
            "Password": {
                "rich_text": [{"text": {"content": record.get("password", "")}}]
            }
        },
        emoji="🎮"
    )

    return PlainTextResponse("OK" if success else "Notion failed", status_code=200)

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

def _guidance_tier_text(category: str) -> str:
    tiers = [get_max_items(category, lvl) for lvl in range(1, 10)]
    parts = " • ".join(f"Lv{i+1}: {v}" for i, v in enumerate(tiers))
    return parts + " • Lv10+: Unlimited"


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
        

# ──────────────────────────────────────────────
# NOTION INTEGRATION (Ready for Render)
# ──────────────────────────────────────────────
async def send_to_notion(title: str, properties: dict, emoji: str = "🌿"):
    """Send data to your Notion database"""
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_DATABASE_ID")
    
    if not notion_token or not database_id:
        print("⚠️ Notion not configured — check Render environment variables")
        return False

    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    payload = {
        "parent": {"database_id": database_id},
        "icon": {"type": "emoji", "emoji": emoji},
        "properties": properties
    }

    try:
        r = await http.post(url, headers=headers, json=payload, timeout=10.0)
        if r.status_code in (200, 201):
            print(f"✅ [Notion] Saved: {title}")
            return True
        else:
            print(f"❌ Notion error {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"❌ Notion exception: {e}")
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
                error_body = r.text if r.text else "No body"
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

async def _sb_patch_check(path: str, payload: dict) -> tuple[bool, bool]:
    """
    Like _sb_patch but also returns whether any rows were actually updated.
    Returns (http_ok, rows_updated)
    """
    async with db_sem:
        try:
            r = await asyncio.wait_for(
                http.patch(
                    f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=_supabase_headers({
                        "Content-Type": "application/json",
                        "Prefer": "return=representation",  # ✅ returns updated rows
                    }),
                    json=payload,
                ),
                timeout=10.0,
            )
            if r.status_code in (200, 204):
                body = r.json() if r.status_code == 200 else []
                rows_updated = len(body) > 0 if isinstance(body, list) else True
                return True, rows_updated
            return False, False
        except Exception as e:
            print(f"🔴 SB PATCH CHECK {path}: {e}")
            return False, False

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
    cached = await redis_client.get("vamt_cache")
    if cached:
        print("⚡ [CACHE] VAMT from Redis")
        return json.loads(cached)

    print("📡 [SUPABASE] Fetching fresh VAMT data…")
    data = await _sb_get("vamt_keys", select="*", order="service_type.asc")
    if data is not None:
        await redis_client.setex("vamt_cache", CACHE_TTL, json.dumps(data))
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

    deleted = await redis_client.delete("vamt_cache")
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

async def broadcast_new_resources(added_counts: dict):
    if not added_counts or not any(added_counts.values()):
        return

    service_emojis = {
        "netflix": "🍿", "prime": "🎥", "windows": "🪟",
        "win": "🪟", "office": "📑", "steam": "🎮",
    }
    service_names = DISPLAY_NAME_MAP.copy()
    service_names["steam"] = "Steam Account"

    # Build per-service lines so we can send targeted messages
    service_lines = {}
    for svc, count in added_counts.items():
        if count > 0:
            emoji = service_emojis.get(svc.lower(), "✨")
            name  = service_names.get(svc.lower(), svc.title())
            service_lines[svc.lower()] = (
                f"🌱 {emoji} +{count} {name}{'s' if count > 1 else ''} just added!"
            )

    if not service_lines:
        return

    # Fetch all users including their notif prefs
    all_users = []
    limit, offset = 1000, 0
    while True:
        batch = await _sb_get(
            "user_profiles",
            select="chat_id,notif_netflix,notif_prime,notif_windows,notif_steam",
            limit=limit,
            offset=offset,
            order="chat_id.asc"
        ) or []
        all_users.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    print(f"📣 Targeted broadcast → {len(all_users)} users")

    sem = asyncio.Semaphore(20)
    success_count = 0

    # Normalize col names for lookup
    notif_col = {
        "netflix": "notif_netflix",
        "prime":   "notif_prime",
        "windows": "notif_windows",
        "win":     "notif_windows",
        "office":  "notif_windows",
        "steam":   "notif_steam",
    }

    async def safe_notify(user: dict):
        nonlocal success_count
        uid = int(user.get("chat_id"))

        # Build lines this user is subscribed to
        user_lines = []
        for svc, line in service_lines.items():
            col = notif_col.get(svc)
            # Default True if column missing; False only if explicitly False
            subscribed = user.get(col, True)
            if subscribed is not False:
                user_lines.append(line)

        if not user_lines:
            print(f"   ⏭ Skipped (all opted out): {uid}")
            return

        final_line = "\n".join(user_lines)

        async with sem:
            try:
                msg = await tg_app.bot.send_message(
                    chat_id=uid,
                    text=final_line,
                    parse_mode="HTML",
                    disable_notification=False,
                    disable_web_page_preview=True,
                    protect_content=True
                )
                await asyncio.sleep(10.0 + random.uniform(0.0, 1.0))
                await tg_app.bot.delete_message(chat_id=uid, message_id=msg.message_id)
                success_count += 1
                print(f"   ✓ Sent & cleaned: {uid}")
            except Exception as e:
                print(f"   ⚠️ Failed for {uid}: {e}")

    await asyncio.gather(
        *(safe_notify(u) for u in all_users),
        return_exceptions=True
    )

    try:
        await tg_app.bot.send_message(
            OWNER_ID,
            f"📣 <b>Targeted Broadcast Complete</b>\n\n"
            + "\n".join(service_lines.values())
            + f"\n\n👥 Total users: <b>{len(all_users)}</b>\n"
            f"✅ Delivered: <b>{success_count}</b>\n\n"
            f"<i>Users who opted out of a service were skipped.</i>",
            parse_mode="HTML",
        )
    except Exception:
        pass

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

async def parse_and_import_keys(content: str, filename: str = "unknown.txt") -> tuple[int, int, list[str], dict]:
    imported = 0
    skipped = 0
    errors = []
    added_counts = Counter()

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
            match = re.split(r"#\s*copy the cookies from here\s*:?", content, flags=re.IGNORECASE)
            cookie_block = match[-1].strip() if len(match) > 1 else content.strip()
        else:
            # Try to extract only Netscape-format cookie lines
            # These always start with a domain like .netflix.com or .amazon.com
            COOKIE_DOMAINS = (
                ".netflix.com",
                ".amazon.com",
                ".primevideo.com",
                ".hotstar.com",
                ".disneyplus.com",
                ".hulu.com",
            )
            cookie_lines = [
                line for line in content.splitlines()
                if line.strip() and line.strip().lower().startswith(COOKIE_DOMAINS)
            ]
            if cookie_lines:
                cookie_block = "\n".join(cookie_lines)
            else:
                # No recognizable cookie lines found — fallback to full content
                cookie_block = content.strip()

        if not cookie_block:
            errors.append(f"❌ {filename}: Cookie block was empty after extraction")
            return 0, 1, errors, {}

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
            added_counts[detected_service] += 1
        else:
            skipped += 1
            errors.append(
                f"❌ Cookie file skipped\n"
                f"   File: {filename}\n"
                f"   Service: {detected_service}\n"
                f"   Reason: Supabase upsert rejected"
            )
        return imported, skipped, errors, dict(added_counts)

    # ══════════════════════════════════════
    # FORMAT 2 — JSON format
    # {"email": "x@x.com", "password": "pass"}
    # ══════════════════════════════════════
    stripped = content.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
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
                    added_counts[item.get("service_type", detected_service)] += 1
                else:
                    skipped += 1
                    errors.append(f"❌ JSON item {i+1} skipped: Supabase rejected")
        except Exception as e:
            errors.append(f"❌ JSON parse failed: {str(e)[:100]}")
            skipped += 1
        return imported, skipped, errors, dict(added_counts)

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
                "remaining":    1,
                "service_type": detected_service,
                "status":       "active",
                "display_name": detected_display,
            }
            success = await _sb_upsert("vamt_keys", payload, on_conflict="key_id")
            if success:
                imported += 1
                added_counts[detected_service] += 1
            else:
                skipped += 1
                errors.append(f"❌ Block {i+1} skipped: Supabase rejected")
        return imported, skipped, errors, dict(added_counts)

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
                    added_counts[svc] += 1
                else:
                    skipped += 1
                    errors.append(f"❌ CSV line {line_num} skipped: {key_id[:30]}")
            return imported, skipped, errors, dict(added_counts)

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
                added_counts[svc] += 1
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

    return imported, skipped, errors, dict(added_counts)

# ──────────────────────────────────────────────
# VIEW LIBRARY (newest updated first + empty name safe)
# ──────────────────────────────────────────────
async def view_notion_steam_library(chat_id: int, page: int = 0, query=None, filter_mode: str = "all"):
    """Improved UX - Detailed View with inline edit buttons"""
    database_id = os.getenv("NOTION_DATABASE_ID")
    if not database_id:
        await tg_app.bot.send_message(chat_id, "❌ Notion database not configured.")
        return

    headers = {
        "Authorization": f"Bearer {os.getenv('NOTION_TOKEN')}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    try:
        r = await http.post(
            f"https://api.notion.com/v1/databases/{database_id}/query",
            headers=headers,
            json={"page_size": 50}
        )
        results = r.json().get("results", [])
    except Exception as e:
        await tg_app.bot.send_message(chat_id, f"❌ Could not load Notion data: {e}")
        return

    if not results:
        await tg_app.bot.send_message(chat_id, "🌫️ No games found.")
        return

    has_update = []
    no_update = []

    for item in results:
        props = item.get("properties", {})

        # Game Name
        title_list = props.get("Game Name", {}).get("title", [])
        game_name = title_list[0].get("text", {}).get("content", "Untitled Game") if title_list else "Untitled Game"

        # Availability
        avail_prop = props.get("Availability", {})
        if avail_prop.get("type") == "multi_select":
            availability = ", ".join([m.get("name", "") for m in avail_prop.get("multi_select", [])]) or "—"
        else:
            availability = avail_prop.get("select", {}).get("name", "—") or "—"

        # Display
        display = props.get("Display", {}).get("formula", {}).get("string", "—")

        # Updated time (emoji column)
        updated = "—"
        for prop_name, prop in props.items():
            if prop and prop.get("type") == "formula":
                formula_str = prop.get("formula", {}).get("string", "").strip()
                if formula_str and any(k in formula_str.lower() for k in ["ago", "just now", "minute", "hour", "day"]):
                    updated = formula_str
                    break

        item_data = {
            "page_id": item["id"],
            "game_name": game_name,
            "availability": availability,
            "display": display,
            "updated": updated,
            "sort_key": parse_updated_for_sort(updated)
        }

        if updated != "—":
            has_update.append(item_data)
        else:
            no_update.append(item_data)

    # Sorting: Newest → Oldest → No update at bottom
    has_update.sort(key=lambda x: x["sort_key"], reverse=True)
    no_update.sort(key=lambda x: x["game_name"].lower())

    all_items = has_update + no_update

    # Simple filter support
    if filter_mode == "available":
        all_items = [g for g in all_items if "Available" in g["availability"]]
    elif filter_mode == "expired":
        all_items = [g for g in all_items if "Expired" in g["availability"]]
    elif filter_mode == "recent":
        all_items = [g for g in all_items if g["updated"] != "—" and ("day" in g["updated"].lower() or "hour" in g["updated"].lower() or "just now" in g["updated"].lower())]

    start = page * 8
    page_items = all_items[start:start + 8]

    # Build message
    text = "📋 <b>Notion STEAM LIBRARY</b>\n━━━━━━━━━━━━━━━━━━\n\n"

    # Top filter row
    text += "🔎 <b>Filter:</b> All • ✅ Available • ❌ Expired • 🔥 Recently Updated\n\n"

    buttons = []

    for item in page_items:
        text += f"🎮 <b>{item['game_name']}</b>\n"
        text += f"   {'✅' if 'Available' in item['availability'] else '❌'} <b>{item['availability']}</b>\n"
        text += f"   Display: {item['display']}\n"
        text += f"   Updated: {item['updated']}\n\n"

        # Inline edit buttons
        buttons.append([
            InlineKeyboardButton("✅ Mark Available", callback_data=f"quick_set|{item['page_id']}|Available"),
            InlineKeyboardButton("❌ Mark Expired",  callback_data=f"quick_set|{item['page_id']}|Expired")
        ])

    # Navigation + extras
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("↼ Previous", callback_data=f"notion_page_{page-1}"))
    if start + 8 < len(all_items):
        nav.append(InlineKeyboardButton("Next ⇀", callback_data=f"notion_page_{page+1}"))

    buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔄 Refresh Library", callback_data="view_notion_steam_refresh")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Caretaker Menu", callback_data="caretaker_menu")])

    final_text = text + f"━━━━━━━━━━━━━━━━━━\n📄 Page {page+1} of {((len(all_items)-1)//8)+1} • {len(all_items)} games • Last synced just now"

    # Edit existing message when possible (clean UX)
    if query and query.message:
        try:
            await query.message.edit_text(
                text=final_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return
        except:
            pass

    await tg_app.bot.send_message(
        chat_id=chat_id,
        text=final_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_document(update: Update):
    """Handle document uploads (.txt or .zip) - Fast single file + Batch for multiple"""
    message = update.message
    chat_id = message.chat_id

    # ── USER CUSTOM PROFILE GIF UPLOAD (anyone can do this) ──
    waiting = await redis_client.get(f"waiting_for_logo:{chat_id}")
    if waiting:
        await redis_client.delete(f"waiting_for_logo:{chat_id}")

        can_change, hours_left = await can_change_profile_gif(chat_id)
        if not can_change:
            await message.reply_text(
                f"🌿 You can only change your profile logo once per week.\n"
                f"Come back in {hours_left} hours!"
            )
            return

        document = message.document
        animation = message.animation

        file_id = None
        if animation:
            file_id = animation.file_id
        elif document and (document.mime_type == "image/gif" or document.file_name.lower().endswith(".gif")):
            if document.file_size and document.file_size > 10 * 1024 * 1024:
                await message.reply_text("❌ GIF is too big. Maximum 10 MB.")
                return
            file_id = document.file_id
        else:
            await message.reply_text("❌ Please send a real GIF (animation or .gif file).")
            return

        if file_id:
            success = await set_user_profile_gif(chat_id, file_id)
            if success:
                await redis_client.setex(f"profile_gif_cooldown:{chat_id}", 7*24*3600, "1")
                await message.reply_animation(
                    animation=file_id,
                    caption="✨ <b>Your profile logo has been enchanted and SAVED!</b>\n\n"
                            "It will now appear every time you view your profile 🌿\n\n"
                            "<i>Try /profile to see it live.</i>",
                    parse_mode="HTML"
                )
            else:
                await message.reply_text("❌ Failed to save your logo. Please try again.")
        return
    
    # ── KEY UPLOAD: Owner only ──
    if chat_id != OWNER_ID:
        return

    document = message.document
    if not document:
        return

    filename = document.file_name or "unknown.txt"
    mime = document.mime_type or ""

    # ── Accept only .txt and .zip ──
    is_txt = mime == "text/plain" or filename.lower().endswith(".txt")
    is_zip = mime in ("application/zip", "application/x-zip-compressed") or filename.lower().endswith(".zip")

    if not is_txt and not is_zip:
        await message.reply_text("❌ Only .txt or .zip files are allowed.")
        return

    # ── File size limit for stability ──
    if document.file_size and document.file_size > 50 * 1024 * 1024:  # 50 MB
        await message.reply_text("❌ File is too large. Maximum allowed is 50 MB.")
        return

    try:
        file = await document.get_file()
        file_bytes = await file.download_as_bytearray()
    except Exception as e:
        await message.reply_text(f"❌ Failed to download file: {e}")
        return

    loading = await message.reply_animation(
        animation=LOADING_GIF,
        caption="🌿 <i>Unpacking the ancient scrolls...</i>",
        parse_mode="HTML"
    )

    # ══════════════════════════════════════
    # ZIP HANDLING (unchanged - bulk)
    # ══════════════════════════════════════
    if is_zip:
        total_imported = 0
        total_skipped = 0
        all_errors = []
        processed_files = []
        total_added = Counter()

        try:
            with zipfile.ZipFile(io.BytesIO(bytes(file_bytes))) as zf:
                txt_files = [
                    name for name in zf.namelist()
                    if name.endswith(".txt") and not name.startswith("__MACOSX")
                ]
                if not txt_files:
                    await loading.edit_caption("❌ No .txt files found inside the ZIP.", parse_mode="HTML")
                    return

                await loading.edit_caption(f"🌿 <i>Found {len(txt_files)} scrolls inside the archive...</i>", parse_mode="HTML")

                for txt_name in txt_files:
                    try:
                        raw_bytes = zf.read(txt_name)
                        content = raw_bytes.decode("utf-8", errors="replace")
                        basename = txt_name.split("/")[-1]
                        imported, skipped, errors, file_added = await parse_and_import_keys(content, basename)
                        total_added.update(file_added)
                        total_imported += imported
                        total_skipped += skipped
                        all_errors.extend(errors)
                        icon = "✅" if imported > 0 else "⚠️"
                        processed_files.append(f"{icon} <code>{basename}</code> → +{imported} imported")
                    except Exception as e:
                        total_skipped += 1
                        all_errors.append(f"❌ {txt_name}: {str(e)[:80]}")
                        processed_files.append(f"❌ <code>{txt_name.split('/')[-1]}</code> → Failed")

        except zipfile.BadZipFile:
            await loading.edit_caption("❌ Invalid or corrupted ZIP file.")
            return
        except Exception as e:
            await loading.edit_caption(f"❌ Failed to extract ZIP: {e}")
            return

        await redis_client.delete("vamt_cache")
        if sum(total_added.values()) > 0:
            asyncio.create_task(broadcast_new_resources(dict(total_added)))

        # Build result message
        files_summary = "\n".join(processed_files[:20])
        result = (
            f"✅ <b>ZIP Import Complete!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>Files processed:</b> {len(txt_files)}\n"
            f"🌱 <b>Total Imported:</b> {total_imported}\n"
            f"📦 <b>Total Skipped:</b> {total_skipped}\n\n"
            f"<b>Per-file results:</b>\n{files_summary}\n\n"
            f"🧹 Cache refreshed!"
        )
        if len(processed_files) > 20:
            result += f"\n<i>...and {len(processed_files) - 20} more files</i>"

        if len(result) <= 1000:
            await loading.edit_caption(result, parse_mode="HTML")
        else:
            await loading.edit_caption(
                f"✅ <b>ZIP Import Complete!</b>\n\n"
                f"📦 {len(txt_files)} files · 🌱 {total_imported} imported · ⚠️ {total_skipped} skipped",
                parse_mode="HTML"
            )
            # Send detailed breakdown
            chunk = ""
            for line in processed_files:
                addition = line + "\n"
                if len(chunk) + len(addition) > 4000:
                    await tg_app.bot.send_message(OWNER_ID, chunk, parse_mode="HTML")
                    chunk = addition
                else:
                    chunk += addition
            if chunk:
                await tg_app.bot.send_message(OWNER_ID, chunk, parse_mode="HTML")

        if all_errors:
            await tg_app.bot.send_message(OWNER_ID, "🔴 <b>ZIP Upload Issues:</b>\n\n" + "\n".join(all_errors[:10]), parse_mode="HTML")
        return

    # ══════════════════════════════════════
    # SINGLE TXT FILE - FAST PATH (NEW)
    # ══════════════════════════════════════
    try:
        content = file_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        await loading.edit_caption(f"❌ Failed to read file: {e}", parse_mode="HTML")
        return

    detected_service, detected_display = detect_service_type(content, filename)
    imported, skipped, errors, added_counts = await parse_and_import_keys(content, filename)

    await redis_client.delete("vamt_cache")     

    if imported > 0:
        temp_key = f"pending_broadcast:{chat_id}"
        
        existing = await redis_client.get(temp_key)
        if existing:
            existing_counts = json.loads(existing)
            for k, v in added_counts.items():
                existing_counts[k] = existing_counts.get(k, 0) + v
            added_counts = existing_counts

        await redis_client.setex(temp_key, 15, json.dumps(added_counts))

        asyncio.create_task(_debounced_broadcast(chat_id))

    # Immediate result for single file (no 4-second wait)
    result = (
        f"✅ <b>Import Complete!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"📄 <b>File:</b> <code>{filename}</code>\n"
        f"🔍 <b>Detected as:</b> {detected_display}\n\n"
        f"🌱 <b>Imported:</b> {imported}\n"
        f"📦 <b>Skipped:</b> {skipped}\n\n"
        f"🧹 Cache refreshed!"
    )

    if skipped > 0 and errors:
        result += f"\n\n⚠️ <b>Skipped Details:</b>\n" + "\n".join([f"• {err}" for err in errors[:6]])

    try:
        await loading.edit_caption(result, parse_mode="HTML")
    except Exception:
        # Fallback if edit fails
        await tg_app.bot.send_message(chat_id, result, parse_mode="HTML")

    # Optional: small success feedback
    if imported > 0:
        await asyncio.sleep(0.8)
        await tg_app.bot.send_message(
            chat_id,
            f"✨ Successfully imported <b>{imported}</b> key(s) from <code>{filename}</code>",
            parse_mode="HTML"
        )
# ──────────────────────────────────────────────
# MAINTENANCE_MODE CONFIG
# ──────────────────────────────────────────────
async def get_maintenance_mode() -> bool:
    val = await redis_client.get("maintenance_mode")
    return val == "1"

async def set_maintenance_mode(enabled: bool):
    await redis_client.set("maintenance_mode", "1" if enabled else "0")

async def has_seen_winoffice_guide(chat_id: int) -> bool:
    # Returns True if this user has already seen the guide
    return bool(await redis_client.get(f"seen_winoffice_guide:{chat_id}"))

async def mark_winoffice_guide_seen(chat_id: int):
    # Permanently marks guide as seen — no TTL, never expires
    await redis_client.set(f"seen_winoffice_guide:{chat_id}", 1)

# ──────────────────────────────────────────────
# ONBOARDING TUTORIAL (3-step flow for new users)
# ──────────────────────────────────────────────
async def can_change_profile_gif(chat_id: int) -> tuple[bool, int]:
    """
    Returns (can_change: bool, hours_remaining: int)
    Cooldown = 7 days (1 change per week)
    """
    key = f"profile_gif_cooldown:{chat_id}"
    ttl = await redis_client.ttl(key)
    
    if ttl > 0:
        hours_left = ttl // 3600
        return False, max(1, hours_left)
    
    return True, 0

async def has_completed_onboarding(chat_id: int) -> bool:
    if await redis_client.get(f"onboarding_done:{chat_id}"):
        return True
    profile = await get_user_profile(chat_id)
    if profile and profile.get("onboarding_completed"):
        await redis_client.set(f"onboarding_done:{chat_id}", 1)
        return True
    return False

async def mark_onboarding_complete(chat_id: int):
    await redis_client.set(f"onboarding_done:{chat_id}", 1)
    await _sb_patch(f"user_profiles?chat_id=eq.{chat_id}", {
        "onboarding_completed": True
    })

async def send_onboarding_step(chat_id: int, first_name: str, step: int):
    """Send the correct onboarding step. Steps 1–3."""

    await redis_client.setex(f"onboarding_step:{chat_id}", 3600, step)

    if step == 1:
        caption = (
            f"🌿 <b>Welcome to Clyde's Enchanted Clearing, {html.escape(first_name)}!</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "🍃 This bot gives you access to:\n\n"
            "• 🪟 <b>Windows & Office</b> activation keys\n"
            "• 🍿 <b>Netflix</b> premium cookies\n"
            "• 🎥 <b>PrimeVideo</b> premium cookies\n"
            "• 🎮 <b>Steam</b> accounts\n\n"
            "Everything is free. No tricks. Just a peaceful forest. 🌲\n\n"
            "<i>Step 1 of 3 — Tap below to continue your journey.</i>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Next → How XP & Levels Work 🌱", callback_data="onboarding_step_2")],
            [InlineKeyboardButton("⏩ Skip Tour", callback_data="onboarding_skip")],
        ])

    elif step == 2:
        caption = (
            "✨ <b>The Forest Energy System</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Every action in the clearing earns you <b>XP</b>:\n\n"
            "• View any list → <b>+8 XP</b>\n"
            "• Reveal a Netflix cookie → <b>+14 XP</b>\n"
            "• Daily login bonus → <b>+10 to +30 XP</b>\n"
            "• Spin the Wheel of Whispers → <b>+13 to +100 XP</b>\n\n"
            "⭐ <b>Why level up?</b>\n"
            "Higher levels unlock more items per day!\n\n"
            "• Level 1 → 5 Netflix cookies/day\n"
            "• Level 5 → 14 Netflix cookies/day\n"
            "• Level 9 → 35 Netflix cookies/day\n\n"
            "<i>Step 2 of 3 — Almost there!</i>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("← Back", callback_data="onboarding_step_1"),
             InlineKeyboardButton("Next → Your First Action 🎯", callback_data="onboarding_step_3")],
            [InlineKeyboardButton("⏩ Skip Tour", callback_data="onboarding_skip")],
        ])

    elif step == 3:
        caption = (
            "🎯 <b>Ready to Begin, Wanderer?</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Here's what the forest caretaker recommends:\n\n"
            "1️⃣ Tap <b>Check Forest Inventory</b>\n"
            "   → Browse today's available keys & cookies\n\n"
            "2️⃣ <b>Reveal a cookie</b> you need\n"
            "   → +14 XP earned instantly!\n\n"
            "3️⃣ Come back <b>every day</b>\n"
            "   → Streak bonuses stack up to +30 XP/day\n\n"
            "🎁 <b>Bonus:</b> You'll earn <b>+15 XP</b> just for completing this tour!\n\n"
            "<i>Step 3 of 3 — The clearing awaits you.</i> 🍃✨"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("← Back", callback_data="onboarding_step_2")],
            [InlineKeyboardButton("🌿 Enter the Enchanted Clearing! (+15 XP)", callback_data="onboarding_complete")],
        ])

    else:
        return

    await send_animated_translated(
        chat_id=chat_id,
        animation_url=ONBOARDING_GIF,
        caption=caption,
        reply_markup=keyboard,
    )

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

    payload = {
        "id": 1,
        "current_version": new_version.strip(),
        "last_updated": custom_datetime.strip()
    }

    # Use _sb_upsert instead of _sb_post
    ok = await _sb_upsert("bot_info", payload, on_conflict="id")

    if ok:
        msg = (
            f"✅ <b>Forest info updated!</b>\n\n"
            f"📜 Version: <b>{new_version}</b>\n"
            f"🔄 Last Updated: <b>{custom_datetime}</b>"
        )
    else:
        msg = "❌ Failed to save. Check server logs for details."

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

        # Just grab the date part before any separator
        date_only = raw.split("·")[0].strip()
        dt = datetime.strptime(date_only, "%B %d, %Y")
        dt = pytz.timezone("Asia/Manila").localize(dt)

        delta = datetime.now(pytz.timezone("Asia/Manila")) - dt
        total_h = delta.days * 24 + delta.seconds // 3600
        mins = (delta.seconds % 3600) // 60
        return f"{total_h}h {mins}m"
    except Exception as e:
        return f"Unknown ({e})"

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
                "lore_reads,profile_views,"
                "total_wheel_spins,wheel_xp_earned,legendary_spins,"
                "onboarding_completed,"
                "notif_netflix,notif_prime,notif_windows,notif_steam,"
                "profile_gif_id"
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

# Helper to turn "9 days ago" into a number for sorting
def parse_updated_for_sort(updated_str: str) -> int:
    """Higher number = more recent. Fixed so any hour beats any day."""
    if not updated_str or updated_str.strip() in ("—", "-", "", None):
        return -9999999

    s = updated_str.lower().strip()

    if "just now" in s or "minute" in s:
        return 1_000_000

    # Days ago
    match = re.search(r'(\d+)\s*day', s)
    if match:
        days = int(match.group(1))
        return 100_000 - (days * 1440)

    # Hours ago - boosted base so 13hrs > 1 day
    match = re.search(r'(\d+)\s*hour', s)
    if match:
        hours = int(match.group(1))
        return 200_000 - (hours * 60)   # ← this is the key change

    # Minutes fallback
    match = re.search(r'(\d+)\s*min', s)
    if match:
        return 300_000 - int(match.group(1))

    return 0

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
        tiers = {1: 4, 2: 5, 3: 6, 4: 7, 5: 9, 6: 11, 7: 13, 8: 16, 9: 20}
        return tiers.get(level, 999)

    if category == "prime":
        tiers = {1: 4, 2: 5, 3: 6, 4: 7, 5: 9, 6: 11, 7: 13, 8: 16, 9: 20}
        return tiers.get(level, 999)

    if category == "netflix":
        if event:
            bonus_type = event.get("bonus_type", "").strip()
            if bonus_type in EVENT_BONUS_TIERS:
                tiers = EVENT_BONUS_TIERS[bonus_type]
                return tiers.get(level, 999)
            
        # Normal tiers
        tiers = {1: 5, 2: 7, 3: 9, 4: 11, 5: 14, 6: 17, 7: 21, 8: 25, 9: 35}
        return tiers.get(level, 999)

    return 0

# ══════════════════════════════════════════════════════════════════════════════
# DAILY CAP HELPERS (Level-based)
# ══════════════════════════════════════════════════════════════════════════════

def get_max_daily_reveals(level: int, service_type: str) -> int:
    """Daily REVEAL limit for Netflix and Prime cookies"""
    level = int(level)
    if service_type == "netflix":
        tiers = {1: 15, 2: 20, 3: 25, 4: 32, 5: 40, 6: 50, 7: 65, 8: 85, 9: 110, 10: 999}
        return tiers.get(level, 999)
    else:  # prime
        tiers = {1: 12, 2: 16, 3: 20, 4: 26, 5: 32, 6: 40, 7: 52, 8: 68, 9: 90, 10: 999}
        return tiers.get(level, 999)


def get_max_daily_views(level: int, category: str) -> int:
    """Daily VIEW limit for Windows and Office keys"""
    level = int(level)
    if category in ("win", "windows", "office"):
        tiers = {1: 12, 2: 16, 3: 20, 4: 25, 5: 32, 6: 40, 7: 50, 8: 65, 9: 90, 10: 999}
        return tiers.get(level, 999)
    return 999

# ──────────────────────────────────────────────
# EVENT COUNTDOWN HELPER (reused in menu + viewevent)
# ──────────────────────────────────────────────
async def _debounced_broadcast(chat_id: int):
    """Wait 3 seconds and send only ONE combined notification"""
    await asyncio.sleep(3.0)
    temp_key = f"pending_broadcast:{chat_id}"
    
    data = await redis_client.get(temp_key)
    if not data:
        return
        
    added_counts = json.loads(data)
    await redis_client.delete(temp_key)
    
    if any(added_counts.values()):
        await broadcast_new_resources(added_counts)

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

def create_daily_progress_bar(used: int, max_allowed: int, length: int = 10) -> str:
    """Visual bar for daily limits (Netflix, Prime, Windows, Office)"""
    if max_allowed <= 0:
        return "🟩" * length
    pct = min(used / max_allowed, 1.0)
    filled = int(pct * length)
    return "🟩" * filled + "⬜" * (length - filled)

# ══════════════════════════════════════════════════════════════════════════════
# ANTI-ABUSE  (Redis-backed)
# ══════════════════════════════════════════════════════════════════════════════
MAX_ACTIONS_PER_MINUTE = 20  # raised from 8 — normal browsing hits ~8-10

async def check_xp_cooldown(chat_id: int, action: str) -> bool:
    """Per-action XP cooldown — prevents farming same button repeatedly.
    Returns True if XP is allowed, False if still on cooldown."""
    seconds = COOLDOWN_SECONDS.get(action, 8)
    key = f"xpcd:{chat_id}:{action}"
    if await redis_client.exists(key):
        return False
    await redis_client.setex(key, seconds, 1)
    return True

async def check_rate_limit(chat_id: int) -> bool:
    key = f"rl:{chat_id}"
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, 60)
    results = await pipe.execute()
    return results[0] <= MAX_ACTIONS_PER_MINUTE

async def try_consume_reveal_cap(chat_id: int, service_type: str) -> tuple[bool, int]:
    """
    Atomic daily REVEAL cap with FULL debug logging + error handling
    """
    try:
        profile = await get_user_profile(chat_id)
        user_level = profile.get("level", 1) if profile else 1
        max_reveals = get_max_daily_reveals(user_level, service_type)

        key = f"daily_reveals:{chat_id}:{service_type}"

        manila = pytz.timezone("Asia/Manila")
        now = datetime.now(manila)
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        ttl = int((midnight - now).total_seconds())

        print(f"[DEBUG REVEAL CAP] {chat_id} | {service_type} | Lv{user_level} | Max={max_reveals} | TTL={ttl}s")

        result = await redis_client.eval(
            REVEAL_CAP_SCRIPT,
            1, key, max_reveals, ttl
        )

        print(f"[DEBUG REVEAL RESULT] Raw Lua result = {result}")

        if result == 0:
            current = int(await redis_client.get(key) or 0)
            remaining = max(0, max_reveals - current)
            print(f"[DEBUG] CAP REACHED. Currently used = {current}")
            return False, remaining
        else:
            used_after = int(result)
            remaining = max(0, max_reveals - used_after)
            print(f"[DEBUG] REVEAL SUCCESS → Used now = {used_after} | Remaining = {remaining}")
            return True, remaining
    except Exception as e:
        print(f"🔴 CRITICAL Redis error in try_consume_reveal_cap({chat_id}, {service_type}): {e}")
        import traceback
        traceback.print_exc()
        return False, 0

async def get_remaining_reveals_and_views(chat_id: int) -> dict:
    try:
        profile = await get_user_profile(chat_id)
        if not profile:
            return {"netflix": 0, "prime": 0, "windows": 0, "office": 0}
        
        level = profile.get("level", 1)
        remaining = {}

        for svc in ["netflix", "prime"]:
            try:
                key = f"daily_reveals:{chat_id}:{svc}"
                used = int(await redis_client.get(key) or 0)
                max_allowed = get_max_daily_reveals(level, svc)
                left = max(0, max_allowed - used)
                remaining[svc] = left
                print(f"[DEBUG REMAINING] {svc} → used={used}/{max_allowed} → left={left}")
            except Exception as e:
                print(f"🔴 Redis error reading {svc} remaining: {e}")
                remaining[svc] = 0

        for cat in ["windows", "office"]:
            try:
                key = f"daily_views:{chat_id}:{cat}"
                used = int(await redis_client.get(key) or 0)
                max_allowed = get_max_daily_views(level, cat)
                left = max(0, max_allowed - used)
                remaining[cat] = left
                print(f"[DEBUG REMAINING] {cat} → used={used}/{max_allowed} → left={left}")
            except Exception as e:
                print(f"🔴 Redis error reading {cat} remaining: {e}")
                remaining[cat] = 0

        return remaining

    except Exception as e:
        print(f"🔴 Critical error in get_remaining_reveals_and_views: {e}")
        return {"netflix": 0, "prime": 0, "windows": 0, "office": 0}

async def try_consume_view_cap(chat_id: int, category: str) -> tuple[bool, int]:
    """
    Atomic daily VIEW cap with FULL debug logging + error handling
    """
    try:
        profile = await get_user_profile(chat_id)
        user_level = profile.get("level", 1) if profile else 1
        max_views = get_max_daily_views(user_level, category)
        
        # ←←← FIXED: Use normalized key
        normalized = normalize_view_category(category)
        key = f"daily_views:{chat_id}:{normalized}"
        
        manila = pytz.timezone("Asia/Manila")
        now = datetime.now(manila)
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        ttl = int((midnight - now).total_seconds())

        print(f"[DEBUG VIEW CAP] {chat_id} | {category} (→{normalized}) | Lv{user_level} | Max={max_views} | TTL={ttl}s")

        result = await redis_client.eval(
            REVEAL_CAP_SCRIPT,
            1, key, max_views, ttl
        )
        print(f"[DEBUG VIEW RESULT] Raw Lua result = {result}")

        if result == 0:
            current = int(await redis_client.get(key) or 0)
            remaining = max(0, max_views - current)
            print(f"[DEBUG] VIEW CAP REACHED for {category}. Currently used = {current}")
            return False, remaining
        else:
            used_after = int(result)
            remaining = max(0, max_views - used_after)
            print(f"[DEBUG] VIEW SUCCESS → Used now = {used_after} | Remaining = {remaining}")
            return True, remaining
    except Exception as e:
        print(f"🔴 CRITICAL Redis error in try_consume_view_cap({chat_id}, {category}): {e}")
        import traceback
        traceback.print_exc()
        return False, 0

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
    claimed = await redis_client.set(key, 1, ex=ttl, nx=True)
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

# ──────────────────────────────────────────────
# NOTIFICATION PREFERENCE HELPERS
# ──────────────────────────────────────────────
async def get_daily_bonus_notif(chat_id: int) -> bool:
    """Daily bonus pref lives in Redis. Default = ON (missing key = ON)"""
    val = await redis_client.get(f"notif:daily_bonus:{chat_id}")
    return val != "0"

async def toggle_daily_bonus_notif(chat_id: int) -> bool:
    """Toggle and return NEW state"""
    current = await get_daily_bonus_notif(chat_id)
    new_state = not current
    await redis_client.set(f"notif:daily_bonus:{chat_id}", "1" if new_state else "0")
    return new_state

async def toggle_service_notif(chat_id: int, service: str) -> bool:
    """
    Toggle notif_netflix / notif_prime / notif_windows / notif_steam
    directly in user_profiles. Returns NEW state.
    """
    profile = await get_user_profile(chat_id)
    if not profile:
        return True

    col_map = {
        "netflix": "notif_netflix",
        "prime":   "notif_prime",
        "windows": "notif_windows",
        "steam":   "notif_steam",
    }
    col = col_map.get(service)
    if not col:
        return True

    current = profile.get(col, True)
    new_state = not current
    await _sb_patch(
        f"user_profiles?chat_id=eq.{chat_id}",
        {col: new_state}
    )
    return new_state

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
    await redis_client.setex(f"pending_ref:{new_chat_id}", 86400, str(referrer_id))
    return True


async def get_pending_referrer(chat_id: int) -> int | None:
    key = f"pending_ref:{chat_id}"
    val = await redis_client.get(key)
    if val:
        await redis_client.delete(key)
        return int(val)
    return None


async def award_referral_bonus(referrer_id: int, referred_id: int, new_user_name: str):
    # Daily limit
    key = f"referral_daily:{referrer_id}"
    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ttl = int((midnight - now).total_seconds())

    count = await redis_client.get(key) or "0"
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
        await redis_client.setex(key, ttl, int(count) + 1)
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
    lock_key = f"ref_awarding:{chat_id}"
    lock = await redis_client.set(lock_key, 1, ex=300, nx=True)
    if not lock:
        return

    try:
        referral = await _sb_get(
            "referrals",
            **{"referred_id": f"eq.{chat_id}", "awarded": "eq.false", "select": "*"}
        )
        if not referral:
            return

        ref = referral[0]
        referrer_id = ref["referrer_id"]

        marked, rows_updated = await _sb_patch_check(
            f"referrals?referred_id=eq.{chat_id}&awarded=eq.false",
            {"awarded": True, "awarded_at": datetime.now(pytz.utc).isoformat()}
        )
        if not marked or not rows_updated:
            return
        
        new_name = (await get_user_profile(chat_id) or {}).get("first_name", "Wanderer")
        await award_referral_bonus(referrer_id, chat_id, new_name)
    finally:
        await redis_client.delete(lock_key)

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
    "wheel_spin": 0,
    "onboarding_complete": 15,
    "onboarding_skip": 0,
    "steam_claim": 0,
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
    "steam_claim": "steam_claims_count",
}

async def add_xp(chat_id: int, first_name: str, action: str = "general", xp_override: int = None) -> tuple[int, int]:
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
    if xp_override is not None:
        xp_amount = xp_override  # wheel spin passes exact amount
    else:
        xp_amount = _XP_TABLE.get(action, 0) if xp_allowed else 0

    # Guidance: 10 XP first time, 2 XP recurring
    if action == "guidance":
        if not xp_allowed:
            xp_amount = 0
        elif profile is None or profile.get("guidance_reads", 0) == 0:
            xp_amount = 10
        else:
            xp_amount = 2

    # Lore: same pattern
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
                await redis_client.delete(f"streak:{chat_id}")
                asyncio.create_task(_announce_daily_bonus(chat_id, daily_bonus, daily_label))
                asyncio.create_task(
                    _log_xp_history(
                        chat_id, first_name, "daily_bonus", daily_bonus,
                        xp_after_action, xp_after_all, level_after_action, new_level,
                    )
                )
                asyncio.create_task(_try_award_referral(chat_id))
            else:
                await redis_client.delete(f"daily_bonus:{chat_id}")

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
            "onboarding_completed": False,
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

        # ── Sequential XP cursor — each log entry shows its own exact before/after ──
        xp_cursor = 0

        if daily_bonus > 0:
            if ok:
                asyncio.create_task(_announce_daily_bonus(chat_id, daily_bonus, daily_label))
                asyncio.create_task(
                    _log_xp_history(
                        chat_id, first_name, "daily_bonus", daily_bonus,
                        xp_cursor, xp_cursor + daily_bonus, 1, 1,
                    )
                )
                asyncio.create_task(_try_award_referral(chat_id))
                xp_cursor += daily_bonus
            else:
                await redis_client.delete(f"daily_bonus:{chat_id}")

        if action_xp > 0 and ok:
            asyncio.create_task(
                _log_xp_history(
                    chat_id, first_name, action, action_xp,
                    xp_cursor, xp_cursor + action_xp, 1, 1,
                )
            )
            xp_cursor += action_xp

        # Notify friend + log welcome bonus separately
        if extra_welcome > 0 and ok:
            asyncio.create_task(
                _log_xp_history(
                    chat_id, first_name, "welcome_bonus", extra_welcome,
                    xp_cursor, xp_cursor + extra_welcome, 1, 1,
                )
            )
            try:
                await tg_app.bot.send_message(
                    chat_id,
                    f"🎁 <b>Welcome Bonus!</b>\n\n"
                    f"✨ <b>+{extra_welcome} Forest Energy</b> has been added to your path!\n\n"
                    f"A friend invited you to the clearing — the forest rewards bonds. 🌿\n\n"
                    f"<i>The trees remember every step you've taken...</i> 🍃",
                    parse_mode="HTML",
                )
            except Exception:
                pass
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

async def _log_wheel_spin(
    chat_id: int,
    first_name: str,
    rarity: str,
    xp_earned: int,
    got_bonus_slot: bool = False,
    got_fresh_cookie: bool = False,
    cookie_service: str | None = None,
):
    """Log every wheel spin + update user summary stats (reliable version)"""
    # 1. Save detailed record
    await _sb_post("wheel_spins", {
        "chat_id": chat_id,
        "first_name": first_name,
        "rarity": rarity,
        "xp_earned": xp_earned,
        "got_bonus_slot": got_bonus_slot,
        "got_fresh_cookie": got_fresh_cookie,
        "cookie_service": cookie_service,
    })

    # 2. Update summary stats reliably
    profile = await get_user_profile(chat_id)
    if profile:
        new_spins = (profile.get("total_wheel_spins") or 0) + 1
        new_xp = (profile.get("wheel_xp_earned") or 0) + xp_earned
        new_legendary = (profile.get("legendary_spins") or 0) + (1 if rarity == "Legendary" else 0)

        await _sb_patch(f"user_profiles?chat_id=eq.{chat_id}", {
            "total_wheel_spins": new_spins,
            "wheel_xp_earned": new_xp,
            "legendary_spins": new_legendary,
        })

# ══════════════════════════════════════════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════
def kb_start():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")
    ]])

def kb_resources():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account")],
        [InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")],
    ])

def kb_main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌟 Wheel of Whispers", callback_data="show_wheel_menu")],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [
            InlineKeyboardButton("👤 My Profile", callback_data="show_profile_page"),
            InlineKeyboardButton("⚙️ Settings", callback_data="show_settings_page"),
        ],
        [
            InlineKeyboardButton("❓ Guidance", callback_data="help"),
            InlineKeyboardButton("ℹ️ Lore", callback_data="about"),
        ],
        [InlineKeyboardButton("📦 Resources", callback_data="show_resources")],
        [InlineKeyboardButton("🕊️ Messenger", url="https://t.me/caydigitals")],
    ])

def kb_first_time_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❓ Start Here → Guidance", callback_data="help")],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌟 Wheel of Whispers", callback_data="show_wheel_menu")],
        [
            InlineKeyboardButton("👤 My Profile", callback_data="show_profile_page"),
            InlineKeyboardButton("⚙️ Settings", callback_data="show_settings_page"),
        ],
        [InlineKeyboardButton("📦 Resources", callback_data="show_resources")],
        [InlineKeyboardButton("🕊️ Messenger", url="https://t.me/caydigitals")],
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

def kb_winoffice_guide():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Got it, show me the keys →",
            callback_data="winoffice_got_it"
        )
    ]])


def kb_back_to_wheel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back to Wheel of Whispers", callback_data="show_wheel_menu")],
    ])

# ══════════════════════════════════════════════════════════════════════════════
# KEYBOARDS (final version)
# ══════════════════════════════════════════════════════════════════════════════
async def kb_caretaker_dynamic() -> InlineKeyboardMarkup:
    """Always await this — it fetches the active event from Redis/Supabase."""
    event = await get_active_event()
    
    row1 = [
        InlineKeyboardButton("📜 Add Patch", callback_data="caretaker_addupdate"),
        InlineKeyboardButton("📬 View Feedbacks", callback_data="caretaker_viewfeedback"),
    ]
    
    if event:
        row2 = [
            InlineKeyboardButton("🎉 Create Event", callback_data="caretaker_addevent"),
            InlineKeyboardButton("🔴 End Event", callback_data="confirm_end_event"),
        ]
    else:
        row2 = [
            InlineKeyboardButton("🎉 Create Event", callback_data="caretaker_addevent"),
            InlineKeyboardButton("🌿 No Active Event", callback_data="noop"),   # cleaner text
        ]
    
    return InlineKeyboardMarkup([
        row1,
        row2,
        [
            InlineKeyboardButton("👁️ View Event", callback_data="caretaker_viewevent"),
            InlineKeyboardButton("🔄 Flush Cache", callback_data="caretaker_flushcache"),
        ],
        [
            InlineKeyboardButton("📤 Upload Keys", callback_data="caretaker_uploadkeys"),
             InlineKeyboardButton("📊 System Health", callback_data="caretaker_health"),
        ],
        [
            InlineKeyboardButton("📋 View Key Reports", callback_data="caretaker_viewreports"),
            InlineKeyboardButton("🛠️ Maintenance", callback_data="confirm_toggle_maintenance"),
        ],
        [
            InlineKeyboardButton("📝 Set Forest Info", callback_data="caretaker_setinfo"),
            InlineKeyboardButton("⚠️ Full Reset", callback_data="caretaker_resetfirst"),
        ],
        [
            InlineKeyboardButton("🎮 Search Steam", callback_data="caretaker_searchsteam"),
            InlineKeyboardButton("🎮 Upload Steam", callback_data="caretaker_uploadsteam"),
        ],
        [
            InlineKeyboardButton("📋 View & Edit Notion Steam Library", callback_data="view_notion_steam"),
        ],
        [
            InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu"),
        ],
    ])

# ══════════════════════════════════════════════════════════════════════════════
# UTILITY MESSAGES
# ══════════════════════════════════════════════════════════════════════════════
def _greeting(tz_str: str = "Asia/Manila", first_name: str = None) -> tuple[str, str, str]:
    """Returns (icon, greeting_text, gif_url) — fully personalized & immersive"""
    now = datetime.now(pytz.timezone(tz_str))
    hour = now.hour
    is_weekend = now.weekday() >= 5   # Saturday or Sunday

    # Personalize the name (safe fallback)
    name = html.escape(first_name.strip()) if first_name and first_name.strip() else "wanderer"

    # ── Midnight (most magical) ──
    if hour == 0:
        return "🌌", f"Midnight in the Enchanted Clearing, {name}", MIDNIGHT_GIF

    # ── Dawn (peaceful early hours) ──
    elif 1 <= hour <= 4:
        return "🌄", f"Dawn whispers through the trees, {name}", DAWN_GIF

    # ── Morning ──
    elif 5 <= hour < 12:
        text = f"Good morning, gentle {name}" if not is_weekend else f"Peaceful weekend morning, {name}"
        return "🌅", text, MORNING_GIF

    # ── Afternoon ──
    elif 12 <= hour < 18:
        text = f"Good afternoon, kind {name}" if not is_weekend else f"Relaxed weekend afternoon, {name}"
        return "🌤️", text, AFTERNOON_GIF

    # ── Late Evening (cozy & quiet) ──
    elif 22 <= hour <= 23:
        return "🌃", f"The forest grows quiet, {name}", LATENIGHT_GIF

    # ── Evening ──
    else:  # 6 PM – 9:59 PM
        text = f"Good evening, {name}" if not is_weekend else f"Cozy weekend evening, {name}"
        return "🌙", text, EVENING_GIF

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

async def send_loading(
    chat_id: int,
    caption: str = "🌫️ <i>The ancient mist begins to stir...</i>",
    remember: bool = False,
):
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=LOADING_GIF,
        caption=caption,
        parse_mode="HTML"
    )
    if remember:
        await _remember(chat_id, msg.message_id)
    return msg

async def _announce_daily_bonus(chat_id: int, bonus: int, label: str):
    # ── Respect user preference (Redis) ──
    if not await get_daily_bonus_notif(chat_id):
        return

    await asyncio.sleep(2.5)
    await send_temporary_message(
        chat_id,
        f"✨ <b>{label}</b>\n\n"
        f"<b>+{bonus} XP</b> for visiting the clearing today 🍃",
        duration=5,
    )

async def _remember(chat_id: int, message_id: int):
    try:
        await redis_client.lpush(f"mem:{chat_id}", message_id)
        await redis_client.expire(f"mem:{chat_id}", 86400)  # 24 hours TTL
    except Exception as e:
        print(f"⚠️ Redis remember failed: {e}")
        forest_memory.setdefault(chat_id, []).append(message_id)  # fallback to RAM


# ══════════════════════════════════════════════════════════════════════════════
# STREAK
# ══════════════════════════════════════════════════════════════════════════════
async def calculate_streak(chat_id: int) -> int:
    # ── Check Redis cache first ──
    cached = await redis_client.get(f"streak:{chat_id}")
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
    await redis_client.setex(f"streak:{chat_id}", 300, streak)
    return streak


# ══════════════════════════════════════════════════════════════════════════════
# MESSAGES / SCREENS
# ══════════════════════════════════════════════════════════════════════════════
# ── IMPROVED: Send animation + auto-translated caption ──
async def send_animated_translated(
    chat_id: int,
    caption: str,
    animation_url: str = None,
    lang: str = None,
    reply_markup=None,
    **kwargs
):
    if lang is None:
        lang = await get_user_language(chat_id)
   
    translated_caption = await translate_text(caption, lang)

    if animation_url:
        msg = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=animation_url,
            caption=translated_caption or None,
            parse_mode="HTML",
            reply_markup=reply_markup,
            **kwargs
        )
    else:
        msg = await tg_app.bot.send_message(
            chat_id=chat_id,
            text=translated_caption or None,
            parse_mode="HTML",
            reply_markup=reply_markup,
            **kwargs
        )
    return msg

async def send_initial_welcome(chat_id: int, first_name: str):
# ✅ Now correctly passes the name to _greeting()
    icon, greeting, gif_url = _greeting(first_name=first_name)

    # ── NEW: New users go through onboarding first ──
    profile = await get_user_profile(chat_id)
    is_new_user = profile is None
    onboarding_done = await has_completed_onboarding(chat_id)

    if is_new_user and not onboarding_done:
        await hide_bot_commands(chat_id)
        await send_onboarding_step(chat_id, first_name, step=1)
        return
    
    # ── Existing users or those who already completed onboarding ──
    caption = (
        f"{icon} {greeting}!\n\n"
        "🌿 Welcome, dear wanderer, to Clyde's Enchanted Clearing.\n\n"
        "Beneath the whispering ancient trees, a world of gentle magic awaits.\n"
        "Hidden wonders and peaceful moments are ready to be discovered.\n\n"
        "Tap the button below to step into the heart of the forest. 🍃✨"
    )
    
    msg = await send_animated_translated(
        chat_id=chat_id,
        animation_url=gif_url,
        caption=caption,
        reply_markup=kb_start(),
    )
    await _remember(chat_id, msg.message_id)

async def send_full_menu(chat_id: int, first_name: str, is_first_time: bool = False):
    icon, greeting, gif_url = _greeting(first_name=first_name)

    profile = await get_user_profile(chat_id)
    level      = profile.get("level", 1) if profile else 1
    title      = get_level_title(level)
    level_info = f"🏷️ {title} • ⭐ Level {level}"
    if profile:
        await redis_client.delete(f"streak:{chat_id}")
    streak = await calculate_streak(chat_id) if profile else 0
    if streak >= 2:
        streak_txt = f"🔥 <b>{streak}-day streak!</b> The forest fire burns bright!"
    else:
        streak_txt = "🌱 <b>First steps in the forest!</b>"

    # ── NEW: Referral hint (only shown to existing users) ──
    referral_hint = "\n🌲 Invite friends → +25 XP each!" if not is_first_time else ""

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
            f"{icon} {greeting}!\n\n"
            "🌿 <b>Welcome to the Enchanted Clearing</b>\n\n"
            f"{level_info} • {streak_txt}{referral_hint}\n"
            f"{event_banner}"
            "Beneath the whispering ancient trees, many paths lie before you...\n\n"
            "🌱 <b>New wanderer?</b> We recommend starting with <b>Guidance</b> first.\n\n"
            "<i>Every view gives +8 XP • Every reveal gives +14 XP</i>\n\n"
            "<i>May your steps be guided by gentle forest magic.</i> 🍃✨"
        )
        keyboard = kb_first_time_menu()
    else:
        caption = (
            f"{icon} {greeting}!\n\n"
            "🌿 <b>Welcome back to the Enchanted Clearing</b>\n\n"
            f"{level_info} • {streak_txt}\n"
            f"{event_banner}"
            "The clearing welcomes you back, wanderer.\n\n"
            "<i>Every view gives +8 XP • Every reveal gives +14 XP</i>\n\n"
            "<i>May the forest welcome you once more.</i> 🍃✨"
        )
        keyboard = kb_main_menu()

    msg = await send_animated_translated(
        chat_id=chat_id,
        animation_url=gif_url,
        caption=caption,
        reply_markup=keyboard,
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

#STEAM CLAIM
async def show_steam_accounts(
    chat_id: int, first_name: str, level: int, query, page: int = 0
):
    tier = get_steam_tier(level)
    ITEMS_PER_PAGE = 5

    manila = pytz.timezone("Asia/Manila")
    now = datetime.now(manila)
    today_start = now.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(pytz.utc).isoformat()
    today_end = now.replace(
        hour=23, minute=59, second=59, microsecond=0
    ).astimezone(pytz.utc).isoformat()
    now_iso = now.astimezone(pytz.utc).isoformat()

    # ── Lv1-6: Website only ──
    if tier == "public":
        hours, mins = get_time_until_drop()
        if is_public_drop_time():
            drop_note = "✅ Today's account is now live on the website!"
        elif is_sunday_noon_manila():
            drop_note = "✅ Sunday bonus account is now live on the website!"
        else:
            drop_note = f"⏰ Next drop in <b>{hours}h {mins}m</b>"

        await query.message.edit_caption(
            caption=(
                "🎮 <b>Steam Accounts</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "🌍 Steam accounts are posted on the website.\n\n"
                f"🕗 <b>Daily drop:</b> 8:00 PM Manila time\n"
                f"🌟 <b>Sunday bonus:</b> 12:00 PM Manila time\n\n"
                f"{drop_note}\n\n"
                "💡 <i>Reach Level 7 for Early Preview access inside the bot!</i>"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🌐 Visit Website", url="https://clydehub.notion.site/Clyde-s-Resource-Hub-ae102294d90682dbaeed81459b131eed")],
                [InlineKeyboardButton("📋 My Claims", callback_data="my_steam_claims")],
                [InlineKeyboardButton("⬅️ Back to Inventory", callback_data="check_vamt")]
            ])
        )
        return

    # ── Check early access time ──
    if not is_early_access_time(level):
        hours, mins = get_time_until_early_access(level)

        tier_badges = {
            7: "⭐ Level 7 Early Access — opens at 4:00 PM",
            8: "⭐ Level 8 Early Access — opens at 12:00 PM",
            9: "🌟 Level 9 Early Access — opens at 8:00 AM",
        }

        await query.message.edit_caption(
            caption=(
                "🎮 <b>Steam Accounts</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                f"🏷️ {tier_badges.get(level, 'Early Access')}\n\n"
                f"⏰ Your early access opens in <b>{hours}h {mins}m</b>\n\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🌍 <b>Public drop:</b> 8:00 PM (website)\n"
                + (f"🌟 <b>Sunday bonus:</b> 12:00 PM (website)\n" if is_sunday_manila() else "")
                + "\n<i>Higher levels get earlier access. Keep leveling up!</i> 🍃"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Inventory", callback_data="check_vamt")]
            ])
        )
        return

    # ── FETCH ACCOUNTS based on level ──
    accounts = []

    if level >= 10:
        # ✅ CORRECT - only games with release_at set AND already past release time
        accounts = await _sb_get(
            "steamCredentials",
            **{
                "select": "*",
                "status": "eq.Available",
                "release_at": "not.is.null",   # scheduled only, no future block
                "order": "release_at.asc",
                "limit": 200,
            }
        ) or []

        # Extra safety filter in Python — exclude any NULL that slipped through
        accounts = [
            a for a in accounts
            if a.get("release_at") is not None
        ]

    elif level == 9:
        # Lv9: TODAY's daily (early, no 8PM restriction)
        daily_accounts = await _sb_get(
            "steamCredentials",
            **{
                "select": "*",
                "status": "eq.Available",
                "release_at": f"gte.{today_start}",
                "order": "release_at.asc",
                "limit": 5,
            }
        ) or []
        accounts += [
            a for a in daily_accounts
            if a.get("release_at")
            and a["release_at"] <= today_end
            and a.get("release_type") != "sunday_noon"
        ]

        # Lv9 Sunday bonus: today's sunday_noon (early, no noon restriction)
        if is_sunday_manila():
            sunday_accounts = await _sb_get(
                "steamCredentials",
                **{
                    "select": "*",
                    "status": "eq.Available",
                    "release_type": "eq.sunday_noon",
                    "release_at": f"gte.{today_start}",  # from today
                    "order": "release_at.asc",
                    "limit": 1,
                }
            ) or []
            # ✅ KEY FIX: only today's sunday game, not future
            accounts += [
                a for a in sunday_accounts
                if a.get("release_at") and a["release_at"] <= today_end
            ]

    else:
        # Lv7-8: TODAY's daily only (early access, no 8PM restriction)
        daily_accounts = await _sb_get(
            "steamCredentials",
            **{
                "select": "*",
                "status": "eq.Available",
                "release_at": f"gte.{today_start}",
                "order": "release_at.asc",
                "limit": 5,
            }
        ) or []
        accounts += [
            a for a in daily_accounts
            if a.get("release_at")
            and a["release_at"] <= today_end
            and a.get("release_type") != "sunday_noon"
        ]

    if not accounts:
        # Show helpful message based on why no accounts showing
        if level in (7, 8):
            no_account_msg = (
                "🎮 <b>Steam Accounts</b>\n\n"
                "🌫️ No account scheduled for today yet.\n\n"
                f"⏰ Check back after your early access time:\n"
                f"{'4:00 PM' if level == 7 else '12:00 PM'} Manila time 🍃"
            )
        elif level == 9:
            no_account_msg = (
                "🎮 <b>Steam Accounts</b>\n\n"
                "🌫️ No account scheduled for today yet.\n\n"
                "⏰ Check back after 08:00 AM Manila time 🍃"
                + ("\n🌟 Sunday bonus also coming at 08:00 AM!" if is_sunday_manila() else "")
            )
        else:
            no_account_msg = (
                "🎮 <b>Steam Accounts</b>\n\n"
                "🌫️ No accounts available right now.\n\n"
                "🌿 Check back later! 🍃"
            )

        await query.message.edit_caption(
            caption=no_account_msg,
            parse_mode="HTML",
            reply_markup=kb_back_inventory(),
        )
        return

    # ── Daily claim limits ──
    daily_limit = STEAM_DAILY_LIMITS.get(min(level, 10), 999) if level < 10 else 999
    claims_today = await get_steam_claims_today(chat_id) if level < 10 else 0
    claims_left = max(0, daily_limit - claims_today) if level < 10 else 999

    # ── Pagination ──
    start = page * ITEMS_PER_PAGE
    page_accounts = accounts[start:start + ITEMS_PER_PAGE]
    total_pages = max(1, (len(accounts) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

    tier_label = {
        7: "⭐ Level 7 — Early Access (4 PM)",
        8: "⭐ Level 8 — Early Access (12 PM)",
        9: "🌟 Level 9 — Early Access (8 AM) + Sunday Bonus",
        10: "👑 Legend Tier",
    }.get(min(level, 10), "⭐ Early Access")

    sunday_line = ""
    if level == 9 and is_sunday_manila():
        sunday_line = "\n🎉 <b>Sunday — You get both accounts today!</b>"

    report = (
        f"🎮 <b>Steam Accounts</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ {tier_label}{sunday_line}\n"
        f"📦 <b>{len(accounts)}</b> account(s) available\n"
    )

    if level < 10:
        status_emoji = "✅" if claims_left > 0 else "❌"
        report += (
            f"🎯 Claimed today: <b>{claims_today}</b> / <b>{daily_limit}</b>\n"
            f"{status_emoji} Claims left: <b>{claims_left}</b>\n"
        )

    report += f"\n📄 Page {page + 1} of {total_pages}\n\n"

    buttons = []
    for acc in page_accounts:
        game = html.escape(acc.get("game_name") or "Unknown Game")
        email = acc.get("email", "")
        release_type = acc.get("release_type", "daily")

        # Dynamic date from release_at
        release_date_str = ""
        if acc.get("release_at"):
            try:
                release_dt = datetime.fromisoformat(acc["release_at"].replace("Z", "+00:00")).astimezone(manila)
                release_date_str = release_dt.strftime("%b %d")
            except Exception:
                release_date_str = ""

        type_badge = f"🌟 {release_date_str}" if release_type == "sunday_noon" else f"📅 {release_date_str}"

        report += (
            f"🎮 <b>{game}</b> {type_badge}\n"
            f"└ 📧 <tg-spoiler>{html.escape(email)}</tg-spoiler>\n\n"
        )

        if claims_left > 0 or level >= 10:
            buttons.append([
                InlineKeyboardButton(
                    f"🔓 Claim {game[:25]}",
                    callback_data=f"claim_steam|{email}|{page}"
                )
            ])
        else:
            buttons.append([
                InlineKeyboardButton(
                    f"🔒 {game[:30]} — Limit Reached",
                    callback_data="steam_claimed_limit"
                )
            ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("↼ Previous", callback_data=f"steam_page_{page-1}"))
    if start + ITEMS_PER_PAGE < len(accounts):
        nav.append(InlineKeyboardButton("Next ⇀", callback_data=f"steam_page_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("📋 My Claims", callback_data="my_steam_claims")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Inventory", callback_data="check_vamt")])

    report += (
        "━━━━━━━━━━━━━━━━━━\n"
        "⚠️ Do not change password or add purchases.\n"
        "<i>Shared accounts — be respectful.</i> 🍃"
    )

    try:
        await query.message.edit_caption(
            caption=report,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    except Exception as e:
        if "not modified" in str(e).lower():
            pass
        else:
            raise

async def show_my_steam_claims(chat_id: int, first_name: str, query=None, page: int = 0):
    """📜 My Claims — Shows all Steam accounts the user has ever claimed"""
    ITEMS_PER_PAGE = 8

    claims = await _sb_get(
        "steam_claims",
        **{
            "chat_id": f"eq.{chat_id}",
            "select": "game_name,account_email,claimed_at",
            "order": "claimed_at.desc",
            "limit": 500,
        }
    ) or []

    if not claims:
        text = (
            "📜 <b>Your Claimed Treasures</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "🌫️ You haven't claimed any Steam accounts yet.\n\n"
            "Go explore the Steam section and bring some games home! 🎮🍃"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎮 Back to Steam Accounts", callback_data="vamt_filter_steam")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ])
        if query and query.message:
            await query.message.edit_caption(text, parse_mode="HTML", reply_markup=keyboard)
        else:
            await tg_app.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=keyboard)
        return

    total = len(claims)
    claims_today = await get_steam_claims_today(chat_id)
    level = (await get_user_profile(chat_id) or {}).get("level", 1)
    daily_limit = STEAM_DAILY_LIMITS.get(min(level, 10), 1)

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    start = page * ITEMS_PER_PAGE
    page_claims = claims[start:start + ITEMS_PER_PAGE]

    manila = pytz.timezone("Asia/Manila")
    lines = [
        "📜 <b>Your Claimed Treasures</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🌿 You have claimed <b>{total}</b> account(s) so far",
        f"🎯 Today: <b>{claims_today}</b> / <b>{daily_limit}</b> claimed",
        f"📄 Page {page + 1} of {total_pages}\n\n"
    ]

    for i, c in enumerate(page_claims, start + 1):
        game = html.escape(c.get("game_name", "Unknown Game"))
        email = html.escape(c.get("account_email", "—"))
        try:
            dt = datetime.fromisoformat(c["claimed_at"].replace("Z", "+00:00"))
            time_str = dt.astimezone(manila).strftime("%b %d, %Y • %I:%M %p")
        except:
            time_str = "—"

        lines.append(f"{i}. 🎮 <b>{game}</b>")
        lines.append(f"   └ 📧 <tg-spoiler>{email}</tg-spoiler>")
        lines.append(f"   └ 📅 {time_str}\n")

    text = "\n".join(lines)

    # Navigation buttons
    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("↼ Previous", callback_data=f"myclaims_page_{page-1}"))
    if start + ITEMS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("Next ⇀", callback_data=f"myclaims_page_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton("🎮 Back to Steam Accounts", callback_data="vamt_filter_steam"),
        InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
    ])

    markup = InlineKeyboardMarkup(buttons)

    if query and query.message:
        try:
            await query.message.edit_caption(caption=text, parse_mode="HTML", reply_markup=markup)
            return
        except Exception:
            pass

    await tg_app.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

# ══════════════════════════════════════════════════════════════════════════════
# INVENTORY — PAGINATED COOKIES
# ══════════════════════════════════════════════════════════════════════════════
async def show_paginated_cookie_list(
    service_type: str, chat_id: int, query, page: int = 0
):
        title = "Netflix" if service_type == "netflix" else "PrimeVideo"
        emoji = "🍿"    if service_type == "netflix" else "🎥"

        await asyncio.sleep(0.5)

        await query.message.edit_caption(
            caption=f"{emoji} <i>Opening the ancient scroll of {title} cookies...</i>",
            parse_mode="HTML",
        )
        await asyncio.sleep(1.5)
        await query.message.edit_caption(
            caption=f"🌿 <i>The forest spirits are gathering your {title} cookies...</i>",
            parse_mode="HTML",
        )
        await asyncio.sleep(1.5)

        profile   = await get_user_profile(chat_id)
        user_level = profile.get("level", 1) if profile else 1
        event      = await get_active_event() 
        max_items  = get_max_items(service_type, user_level, event)

        # Get remaining reveals
        rem = await get_remaining_reveals_and_views(chat_id)
        reveals_left = rem.get(service_type, 0)

        event_bonus_txt = ""
        if event:
            bonus_type = event.get("bonus_type", "").strip()
            if bonus_type == "netflix_double":
                event_bonus_txt = "🎉 <b>Event Bonus Active!</b> Netflix slots are <b>doubled</b> today!\n\n"
            elif bonus_type == "netflix_max":
                event_bonus_txt = "🎉 <b>Event Bonus Active!</b> Netflix slots are <b>maximized</b> today!\n\n"

        data = await get_vamt_data()
        if not data:
            await send_supabase_error(chat_id)
            try:
                await query.message.edit_caption(
                    "🌫️ <b>The forest is unreachable right now...</b>\n\nPlease try again shortly. 🍃",
                    parse_mode="HTML",
                    reply_markup=kb_back_inventory(),
                )
            except Exception:
                pass
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
            f"🌿 You have <b>{reveals_left}</b> reveals left today (Level {user_level})\n\n"
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

async def reveal_cookie(service_type: str, chat_id: int, first_name: str, query, idx: int, page: int):
    emoji = "🍿" if service_type == "netflix" else "🎥"
    loading = None

    try:
        # ── Spam guard ──
        spam_key = f"reveal_spam:{chat_id}:{service_type}"
        if await redis_client.exists(spam_key):
            await query.answer("🌿 Slow down, wanderer! One reveal at a time 🍃", show_alert=True)
            return

        # Set immediately — use SET NX so concurrent requests can't both win
        acquired = await redis_client.set(spam_key, 1, ex=8, nx=True)
        if not acquired:
            await query.answer("🌿 Slow down, wanderer! One reveal at a time 🍃", show_alert=True)
            return

        # Now safe to validate data and consume cap
        profile = await get_user_profile(chat_id)
        user_level = profile.get("level", 1) if profile else 1
        event = await get_active_event()
        max_items = get_max_items(service_type, user_level, event)

        data = await get_vamt_data()
        if not data:
            await query.answer("🌫️ The forest is unreachable right now. Please try again shortly.", show_alert=True)
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
            await show_paginated_cookie_list(service_type, chat_id, query, page)
            return

        item = filtered[idx - 1]

        if str(item.get("status", "")).lower() != "active" or int(item.get("remaining", 0)) <= 0:
            await query.answer("⚠️ This cookie has expired.", show_alert=True)
            await show_paginated_cookie_list(service_type, chat_id, query, page)
            return

        # ── Check bonus slots first ──
        bonus_key = f"daily_reveals_bonus:{chat_id}"
        bonus_slots = int(await redis_client.get(bonus_key) or 0)
        if bonus_slots > 0:
            await redis_client.decr(bonus_key)
        else:
            allowed, remaining = await try_consume_reveal_cap(chat_id, service_type)
            if not allowed:
                await query.message.edit_caption(
                    caption=(
                        f"{emoji} <b>Daily Reveal Limit Reached</b>\n\n"
                        f"🌿 You have already revealed your maximum cookies today.\n\n"
                        f"Come back tomorrow after midnight (Manila time) 🍃\n\n"
                        f"<i>Remaining reveals today: <b>{remaining}</b></i>"
                    ),
                    parse_mode="HTML",
                    reply_markup=kb_back_inventory(),
                )
                await query.answer("Daily limit reached", show_alert=False)
                return

        # ── Show loading (cap is now committed) ──
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

        # ── Deliver the cookie ──
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

        await redis_client.setex(f"reveal_key:{chat_id}:{service_type}:{idx}", 3600, cookie)

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

        doc_msg = await tg_app.bot.send_document(
            chat_id=chat_id,
            document=file_bytes,
            caption=caption,
            parse_mode="HTML",
            reply_markup=kb,
            filename=file_bytes.name
        )

        try:
            await loading.delete()
        except Exception:
            pass

        # Success only
        action_xp, _ = await add_xp(chat_id, first_name, action_name)
        if action_xp:
            asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        await redis_client.setex(
            f"reveal_msg:{chat_id}:{service_type}",
            3600,
            str(doc_msg.message_id)
        )

        asyncio.create_task(send_temporary_message(
            chat_id, f"✨ <i>{display_name} successfully delivered!</i>", duration=3
        ))

        try:
            await query.message.delete()
        except Exception:
            pass

    except Exception as e:
        print(f"🔴 CRITICAL error in reveal_cookie for {chat_id} ({service_type}): {e}")
        await query.answer("🌿 Something went wrong in the forest. Please try again.", show_alert=True)
        if loading:
            try:
                await loading.delete()
            except:
                pass

# ══════════════════════════════════════════════════════════════════════════════
# PROFILE / STATS / LEADERBOARD / HISTORY
# ══════════════════════════════════════════════════════════════════════════════
async def handle_profile_page(chat_id: int, first_name: str, query=None):
    """Fixed version — no more UnboundLocalError + shows custom GIF"""
    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_message(chat_id, "🌿 Please use /start first.")
        return

    level = profile.get("level", 1)
    xp = profile.get("xp", 0)
    xp_required_next = get_cumulative_xp_for_level(level + 1)
    streak = await calculate_streak(chat_id)

    # ── NEW: Daily limits progress
    rem = await get_remaining_reveals_and_views(chat_id)
    level_for_calc = profile.get("level", 1)

    daily_section = (
        "📊 <b>Today's Forest Limits</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"╭🍿 Netflix Reveals: <b>{rem['netflix']}</b> left\n"
        f"╰{create_daily_progress_bar(get_max_daily_reveals(level_for_calc, 'netflix') - rem['netflix'], get_max_daily_reveals(level_for_calc, 'netflix'))}\n\n"
        f"╭🎥 Prime Reveals: <b>{rem['prime']}</b> left\n"
        f"╰{create_daily_progress_bar(get_max_daily_reveals(level_for_calc, 'prime') - rem['prime'], get_max_daily_reveals(level_for_calc, 'prime'))}\n\n"
        f"╭🪟 Windows Keys: <b>{rem['windows']}</b> left\n"
        f"╰{create_daily_progress_bar(get_max_daily_views(level_for_calc, 'windows') - rem['windows'], get_max_daily_views(level_for_calc, 'windows'))}\n\n"
        f"╭📑 Office Keys: <b>{rem['office']}</b> left\n"
        f"╰{create_daily_progress_bar(get_max_daily_views(level_for_calc, 'office') - rem['office'], get_max_daily_views(level_for_calc, 'office'))}\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
    )

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
            dt = datetime.fromisoformat(profile["last_active"].replace("Z", "+00:00"))
            dt = dt.astimezone(pytz.timezone("Asia/Manila"))
            diff_m = int((datetime.now(pytz.timezone("Asia/Manila")) - dt).total_seconds() / 60)
            if diff_m < 2:
                last_active = "Just now"
            elif diff_m < 60:
                last_active = f"{diff_m} minutes ago"
            else:
                last_active = dt.strftime("%B %d, %Y • %I:%M %p")
        except Exception:
            pass

    streak_txt = f"🔥 {streak}-day streak!" if streak >= 2 else "🌱 Just getting started!"

    caption = (
        f"👤 <b>{html.escape(first_name)}'s Forest Profile</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"🏷️ <b>{get_level_title(level)}</b>\n"
        f"⭐ Level <b>{level}</b> • {streak_txt}\n\n"

        f"✨ <b>XP:</b> {xp:,} / {xp_required_next:,}\n"
        f"{create_progress_bar(xp, xp_required_next, 12)}\n"
        f"📈 To next level: <b>{max(0, xp_required_next - xp):,} XP</b>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"{daily_section}"
        "📊 <b>Activity</b>\n\n"
        f"• Total XP Earned: <b>{profile.get('total_xp_earned', xp):,}</b>\n"
        f"• Days Active: <b>{profile.get('days_active', 0)}</b>\n"
        f"• Friends Referred: <b>{profile.get('referral_count', 0)}</b>\n"
        f"• Profile Views: <b>{profile.get('profile_views', 0)}</b>\n"
        f"• Guidance Read: <b>{profile.get('guidance_reads', 0)}</b>\n"
        f"• Lore Read: <b>{profile.get('lore_reads', 0)}</b>\n"
        f"• Times Cleared: <b>{profile.get('times_cleared', 0)}</b>\n\n"

        "🎮 <b>Resource Usage</b>\n\n"
        f"• Windows Keys Viewed: <b>{profile.get('windows_views', 0)}</b>\n"
        f"• Office Keys Viewed: <b>{profile.get('office_views', 0)}</b>\n"
        f"• Netflix Viewed: <b>{profile.get('netflix_views', 0)}</b>\n"
        f"• Netflix Revealed: <b>{profile.get('netflix_reveals', 0)}</b>\n"
        f"• PrimeVideo Viewed: <b>{profile.get('prime_views', 0)}</b>\n"
        f"• PrimeVideo Revealed: <b>{profile.get('prime_reveals', 0)}</b>\n"
        f"• Steam Claimed: <b>{profile.get('steam_claims_count', 0)}</b>\n\n"

        "🎰 <b>Wheel of Whispers</b>\n\n"
        f"• Total Spins: <b>{profile.get('total_wheel_spins', 0)}</b>\n"
        f"• Legendary Spins: <b>{profile.get('legendary_spins', 0)}</b>\n"
        f"• Wheel XP Earned: <b>{profile.get('wheel_xp_earned', 0):,}</b>\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        f"🌱 Joined: <b>{joined_date}</b>\n"
        f"🌲 Last Active: <b>{last_active}</b>\n\n"
        "<i>The trees remember every step of your journey.</i> 🍃"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🖼️ Change Profile", callback_data="change_profile_logo")],
        [InlineKeyboardButton("📜 XP History", callback_data="history_page_0")],
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="leaderboard_from_profile")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")],
    ])

    if query and query.message:
        try:
            await query.message.delete()
        except Exception:
            pass

    loading = await send_loading(chat_id, "🌿 <i>Reading your forest soul...</i>")
    
    # Award XP
    action_xp, _ = await add_xp(chat_id, first_name, "profile")
    if action_xp:
        asyncio.create_task(send_xp_feedback(chat_id, action_xp))

    profile = await get_user_profile(chat_id)
    gif_id = profile.get("profile_gif_id") if profile else None

    if gif_id:
        msg = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=gif_id,
            caption=caption,
            parse_mode="HTML",
            reply_markup=keyboard
        )
    else:
        msg = await send_animated_translated(
            chat_id=chat_id,
            animation_url=MYID_GIF,
            caption=caption,
            reply_markup=keyboard,
        )

    try:
        await loading.delete()
    except Exception:
        pass

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

async def handle_wheel_leaderboard(chat_id: int):
    """🏆 Wheel of Whispers Leaderboard - Top Spinners"""
    top_data = await _sb_get(
        "user_profiles",
        **{
            "select": "chat_id,first_name,total_wheel_spins,wheel_xp_earned,legendary_spins,level",
            "total_wheel_spins": "gt.0",
            "order": "wheel_xp_earned.desc",
            "limit": 10
        }
    ) or []

    if not top_data:
        await tg_app.bot.send_message(
            chat_id,
            "🌿 <b>No one has spun the wheel yet...</b>\n\n"
            "Be the first to test your luck! ✨",
            parse_mode="HTML"
        )
        return

    text = (
        "🌟 <b>Wheel of Whispers Leaderboard</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "🏆 <b>Top Spinners of the Forest</b>\n\n"
    )

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for rank, user in enumerate(top_data, 1):
        # ✅ FIX 2: chat_id is now in select so this comparison works correctly
        name = "The Forest Warden" if str(user.get("chat_id", "")) == str(OWNER_ID) else html.escape(user.get("first_name", "Unknown"))
        medal = medals.get(rank, f"{rank}.")
        spins = user.get("total_wheel_spins", 0)
        legendaries = user.get("legendary_spins", 0)
        wheel_xp = user.get("wheel_xp_earned", 0)

        text += (
            f"{medal} <b>{name}</b>\n"
            f"   🌿 {spins} spins • 🔥 {legendaries} Legendaries\n"
            f"   ✨ {wheel_xp:,} Wheel XP\n\n"
        )

    # ✅ FIX 3: Accurate rank using count of users with MORE spins
    profile = await get_user_profile(chat_id)
    if profile and profile.get("total_wheel_spins", 0) > 0:
        user_spins = profile.get("total_wheel_spins", 0)

        rank_data = await _sb_get(
            "user_profiles",
            **{
                "select": "chat_id",
                "wheel_xp_earned": f"gt.{profile.get('wheel_xp_earned', 0)}",
            }
        ) or []
        user_rank = len(rank_data) + 1

        # Check if the current user is already visible in top 10
        top_ids = [str(u.get("chat_id", "")) for u in top_data]
        already_shown = str(chat_id) in top_ids

        text += "━━━━━━━━━━━━━━━━━━\n"

        if not already_shown:
            # Only show the "Your rank" footer if user isn't already in the top 10 list
            display_name = "The Forest Warden" if chat_id == OWNER_ID else "You"
            text += (
                f"📍 <b>{display_name} — Rank #{user_rank}</b>\n"
                f"   ✨ {profile.get('wheel_xp_earned', 0):,} Wheel XP • "
                f"🌿 {user_spins} spins • "
                f"🔥 {profile.get('legendary_spins', 0)} Legendaries\n"
            )
        else:
            text += f"📍 <b>You are ranked #{user_rank} in the forest!</b>\n"

    elif profile:
        # User exists but has never spun
        text += (
            "━━━━━━━━━━━━━━━━━━\n"
            "📍 <b>You haven't spun the wheel yet!</b>\n"
            "   Try your luck to join the leaderboard 🌿\n"
        )

    text += "\n<i>May your spins bring you great fortune, wanderer...</i> 🍃✨"

    await send_animated_translated(
        chat_id=chat_id,
        caption=text,
        animation_url=WHEEL_BOARD_GIF,
    )

# ══════════════════════════════════════════════════════════════════════════════
# FEEDBACK
# ══════════════════════════════════════════════════════════════════════════════
async def handle_feedback(chat_id: int, first_name: str, feedback_text: str):
    # ── Rate limit: 3 per day ──
    key = f"feedback_limit:{chat_id}"
    count = await redis_client.incr(key)
    await redis_client.expire(key, 86400)  # 24 hours

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

    text = "🌿 <b>Patch Notes — Recent Updates</b>\n"
    text += "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"

    for i, u in enumerate(data, 1):
        # ── Entry header ──
        if len(data) > 1:
            text += f"✨ <b>{i}. {u.get('date', '')}</b>\n"
        else:
            text += f"✨ <b>{u.get('date', '')}</b>\n"

        text += f"<b>📌 {u.get('title', '')}</b>\n"
        text += "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"

        # ── Content: render each line individually ──
        content_lines = u.get('content', '').strip().split('\n')
        for line in content_lines:
            line = line.strip()
            if not line:
                text += "\n"
            elif line.startswith("━"):
                # Dividers inside content → keep as-is
                text += f"{line}\n"
            elif line.startswith("🔹") or line.startswith("•") or line.startswith("-"):
                # Bullet lines → indent slightly
                text += f"  {line}\n"
            elif line.startswith("⚠️") or line.startswith("Note:"):
                # Warnings → italic
                text += f"<i>  {line}</i>\n"
            elif any(line.startswith(e) for e in ["🌍", "🎮", "🌿", "✨", "🎉", "🔧", "🌟"]):
                # Section headers inside content → bold
                text += f"\n<b>{line}</b>\n"
            else:
                text += f"{line}\n"

        text += "\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"

    text += "<i>🍃 May these updates bring more magic to your journey.</i> ✨"

    await tg_app.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

# ══════════════════════════════════════════════════════════════════════════════
# EVENTS
# ══════════════════════════════════════════════════════════════════════════════
async def get_active_event() -> dict | None:
    cached = await redis_client.get("active_event")
    if cached:
        result = json.loads(cached)
    else:
        data = await _sb_get("events", **{"is_active": "eq.true", "order": "created_at.desc", "limit": 1})
        result = data[0] if data else None
        if result:
            await redis_client.setex("active_event", 300, json.dumps(result))

    if result:
        # ── Auto-expire check ──
        try:
            manila = pytz.timezone("Asia/Manila")
            event_dt = datetime.strptime(result.get("event_date", "").strip(), "%B %d, %Y")
            event_dt = manila.localize(event_dt)
            expires_at = event_dt + timedelta(days=1)
            if datetime.now(manila) >= expires_at:
                await _sb_patch("events?is_active=eq.true", {"is_active": False})
                await redis_client.delete("active_event")
                return None
        except Exception:
            pass  # unparseable date — just show event normally

    return result

async def handle_add_event(chat_id: int, title: str, description: str, event_date: str, bonus_type: str = ""):
    if chat_id != OWNER_ID:
        return

    # Deactivate previous — use checked version so we know if anything was active
    await _sb_patch_check("events?is_active=eq.true", {"is_active": False})

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
        await redis_client.delete("active_event")
    else:
        await tg_app.bot.send_message(chat_id, "❌ Failed to save event.")
    
async def handle_end_event(chat_id: int):
    if chat_id != OWNER_ID:
        return

    event = await get_active_event()
    if not event:
        await tg_app.bot.send_message(
            chat_id,
            "🌿 <b>No active event to end.</b>\n\nThe forest is already peaceful. 🍃",
            parse_mode="HTML",
        )
        return

    ok, rows_updated = await _sb_patch_check("events?is_active=eq.true", {"is_active": False})
    if ok and rows_updated:
        await tg_app.bot.send_message(
            chat_id,
            "✅ <b>Event ended successfully!</b>\n\n"
            "🌿 The forest has returned to its peaceful state.\n"
            "<i>No active events running.</i> 🍃",
            parse_mode="HTML",
        )
        await redis_client.delete("active_event")
    elif ok and not rows_updated:
        await tg_app.bot.send_message(
            chat_id,
            "⚠️ <b>No active event found to end.</b>\n\n"
            "It may have already expired or been ended. 🍃",
            parse_mode="HTML",
        )
    else:
        await tg_app.bot.send_message(chat_id, "❌ Failed to end event.")

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
        "All owner commands are now available as buttons below.\n"
        "Choose your action:"
    )
    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=CARETAKER_GIF,
        caption=text,
        parse_mode="HTML",
        reply_markup=await kb_caretaker_dynamic(),
    )
# ──────────────────────────────────────────────
# INVITE HANDLER (used by both /invite and menu button)
# ──────────────────────────────────────────────
async def handle_invite(chat_id: int, first_name: str):
    """🌲 Full Referral System — now available to everyone"""
    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_message(chat_id, "🌿 Start your journey first with /start!")
        return

    link = await get_referral_link(chat_id)
    count = profile.get("referral_count", 0)

    caption = (
        f"🌲 <b>Your Personal Invite Link</b>\n\n"
        f"✨ Invite a friend → both get rewards!\n\n"
        f"🌿 You earn <b>+{REFERRAL_XP} XP</b> per successful referral\n"
        f"🌱 Your friend gets <b>+{NEW_USER_WELCOME_BONUS_IF_REFERRED} XP</b> welcome bonus\n\n"
        f"🔗 <code>{link}</code>\n\n"
        f"📊 You have invited <b>{count}</b> wanderers so far\n\n"
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

    await send_animated_translated(
        chat_id=chat_id,
        caption=caption,
        animation_url=INVITE_GIF,
        reply_markup=keyboard,
    )
    return

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
        message_ids = await redis_client.lrange(f"mem:{chat_id}", 0, -1)
        await redis_client.delete(f"mem:{chat_id}")
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
        await send_xp_feedback(chat_id, action_xp,duration=1) 
    await send_full_menu(chat_id, first_name, is_first_time=False)

async def handle_status(chat_id: int):
    async def check_redis():
        try:
            info = await asyncio.wait_for(redis_client.info("memory"), timeout=3.0)
            used = round(info["used_memory"] / 1024 / 1024, 2)
            key_count = await asyncio.wait_for(redis_client.dbsize(), timeout=3.0)
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
            return f"✅ OK"
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
        f"🔑 <b>Key/Cookie:</b>\n<tg-spoiler>{html.escape(str(key_id))}</tg-spoiler>\n\n"
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

async def handle_steam_feedback(
    chat_id: int,
    first_name: str,
    account_email: str,
    game_name: str,
    is_working: bool,
    query
):
    status = "working" if is_working else "not_working"
    emoji  = "✅" if is_working else "❌"
    label  = "Working" if is_working else "Not Working"

    # Save to key_reports
    await _sb_post("key_reports", {
        "chat_id":      chat_id,
        "first_name":   first_name,
        "key_id":       account_email,
        "service_type": "steam",
        "status":       status,
    })

    # ── Only notify owner — NO auto status change yet ──
    owner_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔁 Restore to Available",
                callback_data=f"owner_restore|{account_email}|{game_name[:25]}"
            ),
            InlineKeyboardButton(
                "🗑 Mark Unavailable",
                callback_data=f"owner_keep|{account_email}|{game_name[:25]}"
            ),
        ]
    ]) if not is_working else None

    await tg_app.bot.send_message(
        OWNER_ID,
        f"🎮 <b>Steam Account Feedback</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 <b>User:</b> {html.escape(str(first_name))} "
        f"(<code>{chat_id}</code>)\n"
        f"🎮 <b>Game:</b> {html.escape(game_name)}\n"
        f"📧 <b>Email:</b> <code>{html.escape(account_email)}</code>\n\n"
        f"Status: {emoji} <b>{label}</b>\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        + ("⚠️ <i>Account status unchanged — tap below to decide.</i>"
           if not is_working else
           "✅ <i>User confirmed this account is working.</i>"),
        parse_mode="HTML",
        reply_markup=owner_kb
    )

    # ── Show undo button to user (only for not working) ──
    undo_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "↩️ Undo — I made a mistake!",
                callback_data=f"stfb_undo|{account_email}|{game_name[:30]}"
            )
        ]
    ]) if not is_working else None

    await query.answer(
        "✅ Thanks! Feedback sent." if is_working else
        "❌ Reported! You have 30 seconds to undo.",
        show_alert=True,
    )

    # ── Edit claim message to show feedback result ──
    try:
        current_caption = query.message.caption or query.message.text or ""
        new_caption = (
            f"{current_caption}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} <b>You reported this as: {label}</b>\n"
            + ("<i>Made a mistake? Tap Undo within 30 seconds.</i>"
               if not is_working else
               "<i>Thank you for your feedback! 🍃</i>")
        )
        await query.message.edit_caption(
            caption=new_caption,
            parse_mode="HTML",
            reply_markup=undo_kb
        )
    except Exception:
        pass

    # ── Auto-remove undo button after 30 seconds ──
    if not is_working:
        async def remove_undo():
            await asyncio.sleep(30)
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
        asyncio.create_task(remove_undo())

async def handle_settings_page(chat_id: int, first_name: str, query=None):
    lang = await get_user_language(chat_id)
    flag, lang_name = SUPPORTED_LANGUAGES.get(lang, ("🇬🇧", "English"))

    # Fetch all prefs in parallel
    profile = await get_user_profile(chat_id)
    daily_on = await get_daily_bonus_notif(chat_id)

    # Read service prefs from profile (default True if missing)
    netflix_on = profile.get("notif_netflix", True) if profile else True
    prime_on   = profile.get("notif_prime",   True) if profile else True
    windows_on = profile.get("notif_windows", True) if profile else True
    steam_on   = profile.get("notif_steam",  False) if profile else False

    def _icon(state: bool, on="🔔", off="🔕") -> str:
        return on if state else off

    caption = (
        f"⚙️ <b>Settings</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🌍 <b>Language:</b> {flag} {lang_name}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔔 <b>Notifications</b>\n\n"
        f"{_icon(daily_on)} <b>Daily Bonus Alert:</b> {'ON' if daily_on else 'OFF'}\n"
        f"<i>Popup when your daily login XP is added.</i>\n\n"
        f"{_icon(netflix_on, '🍿', '🔇')} <b>Netflix Alerts:</b> {'ON' if netflix_on else 'OFF'}\n"
        f"{_icon(prime_on,   '🎥', '🔇')} <b>Prime Alerts:</b> {'ON' if prime_on else 'OFF'}\n"
        f"{_icon(windows_on, '🪟', '🔇')} <b>Windows/Office Alerts:</b> {'ON' if windows_on else 'OFF'}\n"
        f"{_icon(steam_on,   '🎮', '🔇')} <b>Steam Alerts:</b> {'ON' if steam_on else 'OFF'}\n\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "<i>Tap any button to toggle it on or off.</i> 🍃"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"🌍 Change Language ({flag} {lang_name})",
            callback_data="set_language"
        )],
        [InlineKeyboardButton(
            f"{_icon(daily_on)} Daily Bonus Alert: {'ON' if daily_on else 'OFF'}",
            callback_data="toggle_notif|daily_bonus"
        )],
        [
            InlineKeyboardButton(
                f"{_icon(netflix_on, '🍿', '🔇')} Netflix: {'ON' if netflix_on else 'OFF'}",
                callback_data="toggle_notif|netflix"
            ),
            InlineKeyboardButton(
                f"{_icon(prime_on, '🎥', '🔇')} Prime: {'ON' if prime_on else 'OFF'}",
                callback_data="toggle_notif|prime"
            ),
        ],
        [
            InlineKeyboardButton(
                f"{_icon(windows_on, '🪟', '🔇')} Windows: {'ON' if windows_on else 'OFF'}",
                callback_data="toggle_notif|windows"
            ),
            InlineKeyboardButton(
                f"{_icon(steam_on, '🎮', '🔇')} Steam: {'ON' if steam_on else 'OFF'}",
                callback_data="toggle_notif|steam"
            ),
        ],
        [InlineKeyboardButton(
            "🌲 Invite Friends & Earn 25 XP",
            callback_data="invite_friends"
        )],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")],
    ])

    if query and query.message:
        try:
            await query.message.delete()
        except Exception:
            pass

    await send_animated_translated(
        chat_id=chat_id,
        animation_url=MYID_GIF,
        caption=caption,
        reply_markup=keyboard,
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
            f"└ 🔑 <tg-spoiler>{str(r.get('key_id', ''))[:40]}</tg-spoiler>\n"
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

    # ← NEW: Check if there's actually an active event first
    event = await get_active_event()

    if not event:
        await tg_app.bot.send_message(
            chat_id,
            "🌿 <b>No active event to end.</b>\n\n"
            "The forest is already in its peaceful state. 🍃",
            parse_mode="HTML",
        )
        return

    # Only show confirmation if there IS an active event
    confirm_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✔️ Yes, End Event Now", callback_data="yes_end_event")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data="cancel_end_event")],
    ])

    await tg_app.bot.send_message(
        chat_id,
        "⚠️ <b>End Current Event?</b>\n\n"
        "This will immediately remove the event banner for <b>all users</b>.\n"
        "The forest will return to its peaceful state.\n\n"
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

async def show_winoffice_keys(chat_id: int, category: str, profile: dict, query):
    pending_cat = category
    cat_label = "Windows" if pending_cat in ("win", "windows") else "Office"
    cat_emoji = "🪟" if pending_cat in ("win", "windows") else "📑"
    cat_gif   = WINOS_GIF if pending_cat in ("win", "windows") else OFFICE_GIF

    try:
        if query and query.message:
            try:
                await query.message.delete()
            except Exception:
                pass

        # Send a FRESH animation (never edit — previous msg is deleted)
        loading = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=cat_gif,
            caption=f"{cat_emoji} <i>Opening the {cat_label} key scroll...</i>",
            parse_mode="HTML",
        )

        # New safety check
        if not loading or not hasattr(loading, "message_id"):
            await send_supabase_error(chat_id, query)
            return

        await asyncio.sleep(1.5)
        try:
            await loading.edit_caption(
                caption=f"🌿 <i>The ancient {cat_label} scrolls are being unsealed...</i>",
                parse_mode="HTML",
            )
        except Exception:
            pass

        await asyncio.sleep(1.5)

        user_level = profile.get("level", 1)
        internal_cat = normalize_view_category(category)

        # Get remaining views (read only)
        rem = await get_remaining_reveals_and_views(chat_id)
        views_left = rem.get(internal_cat, 0)

        vamt = await get_vamt_data()
        if not vamt:
            await send_supabase_error(chat_id)
            try:
                await loading.edit_caption(
                    caption="🌫️ <b>The forest is unreachable right now...</b>\n\nPlease try again shortly. 🍃",
                    parse_mode="HTML",
                    reply_markup=kb_back_inventory(),
                )
            except Exception:
                pass
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
            await loading.edit_caption(
                caption=f"🍃 No {cat_label} keys available right now. Check back later!",
                parse_mode="HTML",
                reply_markup=kb_back_inventory(),
            )
            return

        max_items = get_max_items(pending_cat, user_level)
        filtered.sort(key=lambda x: (str(x.get("service_type", "")), str(x.get("key_id", ""))))
        display_items = filtered[:max_items]

        report = f"{cat_emoji} <b>{cat_label} Activation Keys</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        report += f"🌿 You have <b>{views_left}</b> views left today (Level {user_level})\n\n"
        report += f"📋 <b>{len(display_items)} key(s) available for your level</b>\n\n"

        for item in display_items:
            stock = str(item.get("remaining", 0))
            report += (
                f"✨ <b>{item.get('service_type', 'Unknown')}</b>\n"
                f"└ 🔑 Key: <tg-spoiler>{item.get('key_id', 'HIDDEN')}</tg-spoiler>\n"
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
            await redis_client.setex(f"winkey:{token}", 3600, f"{raw_key}||{svc}")
            buttons.append([
                InlineKeyboardButton(f"✅ {short}", callback_data=f"wkfb_ok|{token}"),
                InlineKeyboardButton(f"❌", callback_data=f"wkfb_bad|{token}"),
            ])

        buttons.append([InlineKeyboardButton(
            "❓ What is VAMT / Remaining?", callback_data=f"explain_vamt|{internal_cat}"
        )])
        buttons.append([InlineKeyboardButton(
            "⬅️ Back to Inventory", callback_data="check_vamt"
        )])

        await loading.edit_caption(
            caption=report,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    except Exception as e:
        print(f"🔴 Error in show_winoffice_keys for {chat_id}: {e}")
        await send_supabase_error(chat_id, query)

# ══════════════════════════════════════════════════════════════════════════════
# CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════════════════════
async def handle_searchsteam_command(chat_id: int, raw_text: str, page: int = 0, query=None):
    """Single search with pagination | Bulk search = Supabase only"""
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can use this.")
        return

    body = raw_text.replace("/searchsteam", "").strip()
    if not body:
        await send_animated_translated(
            chat_id,
            "🔍 <b>Steam Account Search</b>\n\n"
            "• Single: username or email\n"
            "• Bulk: multiple lines (Supabase only)\n\n"
            "Example:\n<code>/searchsteam clydeforest</code>",
            animation_url=STEAM_GIF,
        )
        return

    lines = [line.strip() for line in body.split("\n") if line.strip()]

    # ====================== BULK SEARCH ======================
    if len(lines) > 1:
        loading_msg = await tg_app.bot.send_message(
            chat_id,
            f"📋 <b>Processing {len(lines)} accounts...</b>\n\n<i>Searching Supabase...</i>",
            parse_mode="HTML"
        )

        all_accounts = await _sb_get(
            "steamCredentials",
            **{"select": "email,password,game_name,status", "limit": 2000}
        ) or []

        account_map = {str(acc.get("email", "")).lower().strip(): acc for acc in all_accounts if acc.get("email")}

        found_results = []
        not_found_results = []

        for term in lines:
            acc = account_map.get(term.lower().strip())
            if acc:
                password = html.escape(acc.get("password", "HIDDEN"))
                found_results.append(
                    f"✅ <code>{html.escape(term)}</code>\n"
                    f"🎮 {acc.get('game_name', '—')} | {acc.get('status', 'Available')}\n"
                    f"🔑 <tg-spoiler>{password}</tg-spoiler>"
                )
            else:
                not_found_results.append(f"❌ <code>{html.escape(term)}</code> — Not found")

        total     = len(lines)
        succeeded = len(found_results)
        failed    = len(not_found_results)

        summary_header = (
            f"📊 <b>Bulk Search Results</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✅ Found: <b>{succeeded}</b>  |  ❌ Not Found: <b>{failed}</b>  |  📋 Total: <b>{total}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
        )

        grouped_lines = []
        if found_results:
            grouped_lines.append("🟢 <b>Found Accounts</b>")
            grouped_lines.extend(found_results)
        if not_found_results:
            if found_results:
                grouped_lines.append("")
            grouped_lines.append("🔴 <b>Not Found</b>")
            grouped_lines.extend(not_found_results)

        final_text = summary_header + "\n\n".join(grouped_lines)

        # Delete the loading message before sending results
        try:
            await loading_msg.delete()
        except Exception:
            pass

        await send_animated_translated(chat_id, final_text, animation_url=STEAM_RESULT_GIF)
        return

    # ====================== SINGLE SEARCH ======================
    term = lines[0]

    STEAM_API_KEY = os.getenv("STEAM_API_KEY")
    if not STEAM_API_KEY:
        await tg_app.bot.send_message(chat_id, "❌ STEAM_API_KEY not set.")
        return

    # Fetch from Supabase
    all_accounts = await _sb_get(
        "steamCredentials",
        **{"select": "email,password,game_name,status,steam_id", "limit": 2000}
    ) or []

    supabase_acc = next((acc for acc in all_accounts 
                        if str(acc.get("email", "")).lower().strip() == term.lower().strip()), None)

    supabase_text = ""
    if supabase_acc:
        password = html.escape(supabase_acc.get("password", "HIDDEN"))
        supabase_text = (
            f"✅ <b>Found in Supabase</b>\n"
            f"🎮 Game: {supabase_acc.get('game_name', '—')}\n"
            f"Status: <b>{supabase_acc.get('status', 'Available')}</b>\n"
            f"🔑 Password: <tg-spoiler>{password}</tg-spoiler>\n\n"
        )
    else:
        supabase_text = "❌ Not found in Supabase.\n\n"

    # Get steam_id
    steamid = None
    if supabase_acc and supabase_acc.get("steam_id"):
        sid = str(supabase_acc.get("steam_id")).strip()
        if sid.isdigit() and len(sid) == 17:
            steamid = sid

    if not steamid:
        await send_animated_translated(
            chat_id,
            f"🔍 <b>Result for:</b> <code>{html.escape(term)}</code>\n\n"
            f"{supabase_text}"
            f"🌐 Live data unavailable — No steam_id stored.",
            animation_url=STEAM_RESULT_GIF,
        )
        return

    # Fetch live data
    live_status = "❌ Could not fetch status"
    games = []
    try:
        sum_url = f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={STEAM_API_KEY}&steamids={steamid}"
        sum_r = await http.get(sum_url, timeout=10.0)
        players = sum_r.json().get("response", {}).get("players", [])
        if players:
            p = players[0]
            state_map = {0:"Offline",1:"Online",2:"Busy",3:"Away",4:"Snooze",5:"Looking to Trade",6:"Looking to Play"}
            status = state_map.get(p.get("personastate", 0), "Unknown")
            game = p.get("gameextrainfo")
            live_status = f"🎮 <b>Currently playing</b>: {game}\nStatus: {status}" if game else f"Status: {status}"

        games_url = f"https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={STEAM_API_KEY}&steamid={steamid}&include_appinfo=1&include_played_free_games=1"
        games_r = await http.get(games_url, timeout=15.0)
        games_data = games_r.json().get("response", {}).get("games", [])
        if games_data:
            games_data.sort(key=lambda x: x.get("playtime_forever", 0), reverse=True)
            games = games_data
    except Exception as e:
        print(f"Live fetch error: {e}")

    # Show the requested page (and edit if from callback)
    await show_games_page(chat_id, term, supabase_text, live_status, games, page=page, query=query)

# ──────────────────────────────────────────────
# New Helper Function for Pagination
# ──────────────────────────────────────────────
async def show_games_page(chat_id: int, term: str, supabase_text: str, live_status: str, all_games: list, page: int = 0, query=None):
    """Show games with pagination - edits message if called from callback"""
    GAMES_PER_PAGE = 10
    total_games = len(all_games)
    total_pages = (total_games + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE if total_games > 0 else 1

    start = page * GAMES_PER_PAGE
    end = min(start + GAMES_PER_PAGE, total_games)
    page_games = all_games[start:end]

    text = f"🔍 <b>Steam Search Result for:</b> <code>{html.escape(term)}</code>\n\n"
    text += supabase_text
    text += f"🌐 <b>Live Steam Data</b>\n{live_status}\n\n"

    if total_games == 0:
        text += "⚠️ No games visible (profile is likely private)."
    else:
        text += f"🎮 <b>{total_games:,} games owned</b> — Page {page+1} of {total_pages}\n\n"
        for g in page_games:
            name = g.get("name", "Unknown Game")
            hours = round(g.get("playtime_forever", 0) / 60, 1)
            text += f"• {name} — <b>{hours:,} hrs</b>\n"

    # Navigation buttons
    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("↼ Previous", callback_data=f"games_page|{term}|{page-1}"))
    if end < total_games:
        nav.append(InlineKeyboardButton("Next ⇀", callback_data=f"games_page|{term}|{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("⬅️ Back to Caretaker Menu", callback_data="caretaker_searchsteam")])

    markup = InlineKeyboardMarkup(buttons)

    # === KEY CHANGE: Edit if from callback, else send new message ===
    try:
        if query and query.message:
            await query.message.edit_text(
                text=text,
                parse_mode="HTML",
                reply_markup=markup
            )
            return
    except Exception:
        pass  # fallback if edit fails

    # Fallback: send new message
    await send_animated_translated(
        chat_id=chat_id,
        caption=text,
        animation_url=STEAM_RESULT_GIF,
        reply_markup=markup
    )

# ──────────────────────────────────────────────
# LANGUAGE SELECTOR (now deletes old menu first)
# ──────────────────────────────────────────────
async def handle_set_language(chat_id: int, query=None):
    """Show language selector — deletes old animated main menu first"""
    lang = await get_user_language(chat_id)
    current_flag, current_name = SUPPORTED_LANGUAGES.get(lang, ("🇬🇧", "English"))
   
    text = f"🌍 <b>Current language:</b> {current_flag} {current_name}\n\nChoose your preferred language:"
    
    buttons = [
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_set|en")],
        [InlineKeyboardButton("🇵🇭 Tagalog", callback_data="lang_set|tl")],
        [InlineKeyboardButton("🇵🇭 Bisaya", callback_data="lang_set|ceb")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
    ]
    markup = InlineKeyboardMarkup(buttons)

    # 🔥 CRITICAL FIX: Delete the old animated menu
    if query and query.message:
        try:
            await query.message.delete()
        except Exception:
            pass

    # Send clean language selector
    await send_animated_translated(
        chat_id=chat_id,
        caption=text,
        animation_url=None,
        reply_markup=markup
    )

    # Beta notice (disappears automatically)
    asyncio.create_task(send_temporary_message(chat_id, "🌱 This language feature is still in beta.", duration=3))

# ── Auto-translate helpers (use these from now on) ──
async def send_translated(chat_id: int, text: str, lang: str = None, **kwargs):
    if lang is None:
        lang = await get_user_language(chat_id)
    translated = await translate_text(text, lang)
    return await tg_app.bot.send_message(chat_id=chat_id, text=translated, **kwargs)

async def edit_translated(query, text: str, lang: str = None, **kwargs):
    if lang is None:
        lang = await get_user_language(query.message.chat_id)
    translated = await translate_text(text, lang)
    return await query.message.edit_caption(caption=translated, **kwargs)

async def handle_uploadsteam_command(chat_id: int, raw_text: str):
    if chat_id != OWNER_ID:
        await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can upload Steam accounts.")
        return

    body = raw_text.replace("/uploadsteam", "").strip()
    if not body:
        await send_animated_translated(
            chat_id,
            "🎮 <b>Steam Account Uploader</b>\n\n"
            "<b>Single account:</b>\n"
            "<code>/uploadsteam\n"
            "email\n"
            "password\n"
            "Game Name (optional)\n"
            "SteamID64 (optional)\n"
            "https://banner-url.jpg (optional)</code>\n\n"
            "<b>Bulk (separate accounts with a blank line):</b>\n"
            "<code>/uploadsteam\n"
            "email1\n"
            "password1\n"
            "Game Name\n"
            "76561198XXXXXXXXX\n"
            "https://banner.jpg\n\n"
            "email2\n"
            "password2\n\n"
            "email3\n"
            "password3\n"
            "Only Game Name</code>\n\n"
            "⚠️ <i>Minimum required: email + password</i>\n"
            "💡 <i>SteamID = 17 digits | Banner = must start with https://</i>\n"
            "<i>Tip: Use /searchsteam first to check for duplicates.</i>",
            animation_url=STEAM_GIF
        )
        return

    # Split blocks by blank lines
    blocks = [b.strip() for b in body.split("\n\n") if b.strip()]

    imported = 0
    skipped = 0
    results = []

    for i, block in enumerate(blocks, start=1):
        lines = [l.strip() for l in block.split("\n") if l.strip()]

        # Must have at least email + password
        if len(lines) < 2:
            skipped += 1
            results.append(f"❌ Block {i} — Too few fields (need at least email + password)")
            continue

        email     = lines[0]
        password  = lines[1]
        game_name = lines[2] if len(lines) >= 3 else None

        # ✅ FIX: Detect steam_id and image_url by CONTENT not POSITION
        # This allows skipping SteamID without breaking image URL
        steam_id  = None
        image_url = None
        steam_id_warning = ""

        for field in lines[3:]:
            field = field.strip()
            if not field:
                continue
            if field.isdigit() and len(field) == 17:
                steam_id = field  # ✅ Valid SteamID64
            elif field.startswith("http"):
                image_url = field  # ✅ Valid image URL
            else:
                # ⚠️ Something provided but unrecognizable
                steam_id_warning = f" ⚠️ (unrecognized field ignored: {field[:20]})"

        # ✅ Build missing fields note
        missing = []
        if not game_name:
            missing.append("no game name")
        if not steam_id:
            missing.append("no SteamID")
        if not image_url:
            missing.append("no banner")

        missing_note = f" ⚠️ ({', '.join(missing)})" if missing else ""

        # ✅ NEW payload — always sets schedule on upload
        manila = pytz.timezone("Asia/Manila")
        tonight_8pm = datetime.now(manila).replace(
            hour=20, minute=0, second=0, microsecond=0
        ).astimezone(pytz.utc).isoformat()

        payload = {
            "email":        email,
            "password":     password,
            "game_name":    game_name,
            "steam_id":     steam_id,
            "image_url":    image_url,
            "status":       "Available",
            "release_type": "daily",      # ✅ added
            "release_at":   tonight_8pm,  # ✅ added
            "action":       None,
            "Posted":       None,
        }
        async with db_sem:
            try:
                r = await asyncio.wait_for(
                    http.post(
                        f"{SUPABASE_URL}/rest/v1/steamCredentials",
                        headers=_supabase_headers({
                            "Content-Type": "application/json",
                            "Prefer": "resolution=ignore-duplicates,return=minimal",
                        }),
                        json=payload,
                    ),
                    timeout=10.0,
                )
                if r.status_code in (200, 201):
                    imported += 1
                    label = f"{game_name or 'Unknown Game'}{steam_id_warning}{missing_note}"
                    results.append(f"✅ <code>{html.escape(email)}</code> — {label}")
                elif r.status_code == 409:
                    skipped += 1
                    results.append(f"⚠️ <code>{html.escape(email)}</code> — Duplicate (already exists)")
                else:
                    skipped += 1
                    results.append(f"⚠️ <code>{html.escape(email)}</code> — Rejected (status {r.status_code})")
            except asyncio.TimeoutError:
                skipped += 1
                results.append(f"❌ <code>{html.escape(email)}</code> — Timed out")
            except Exception as e:
                skipped += 1
                results.append(f"❌ <code>{html.escape(email)}</code> — Error: {str(e)[:50]}")

    # ✅ Summary
    summary = (
        f"🎮 <b>Upload Complete!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Imported: <b>{imported}</b>\n"
        f"⚠️ Skipped: <b>{skipped}</b>\n\n"
        f"<b>Results:</b>\n"
        + "\n".join(results[:20])
    )
    if len(results) > 20:
        summary += f"\n<i>...and {len(results) - 20} more</i>"

    if imported > 0:
        asyncio.create_task(broadcast_new_resources({"steam": imported}))

    await send_animated_translated(chat_id, summary, animation_url=STEAM_RESULT_GIF)
    
async def hide_bot_commands(chat_id: int):
    try:
        await tg_app.bot.set_my_commands(
            commands=[],
            scope=BotCommandScopeChat(chat_id=chat_id)
        )
        print(f"✅ Commands hidden for {chat_id}")
    except Exception as e:
        print(f"🔴 Could not hide commands for {chat_id}: {e}")

async def restore_bot_commands(chat_id: int):
    """Remove chat-specific override → falls back to BotFather commands"""
    try:
        await tg_app.bot.delete_my_commands(
            scope=BotCommandScopeChat(chat_id=chat_id)  # ← just delete the override
        )
        print(f"✅ Commands restored to BotFather defaults for {chat_id}")
    except Exception as e:
        print(f"🔴 Could not restore commands for {chat_id}: {e}")

async def handle_callback(update: Update):
    query     = update.callback_query
    if not query or not query.data:
        return
    chat_id   = update.effective_chat.id
    first_name = update.effective_user.first_name if update.effective_user else "Wanderer"
    data      = query.data

    FEEDBACK_PREFIXES = (
        "kfb_ok|",
        "kfb_bad|",
        "wkfb_ok|",
        "wkfb_bad|",
        "key_feedback_ok|",
        "key_feedback_bad|",
        "copy_ref_link|",
        "stfb_ok|", "stfb_bad|",
        "stfb_undo|",
        "owner_restore|",
        "owner_keep|",
        "steam_claimed_limit",
    )
    if not data.startswith(FEEDBACK_PREFIXES):
        await query.answer()

    # ── ONBOARDING GUARD FOR CALLBACKS ──
    ONBOARDING_ALLOWED = (
        "onboarding_step_", "onboarding_skip", "onboarding_complete", "show_main_menu"
    )
    onboarding_done = await has_completed_onboarding(chat_id)
    profile_check = await get_user_profile(chat_id)

    if not onboarding_done and profile_check is None:
        if not any(data.startswith(p) for p in ONBOARDING_ALLOWED):
            await query.answer(
                "🌿 Please complete the welcome tour first!",
                show_alert=True
            )
            return

# ── ONBOARDING TUTORIAL ──
    if data.startswith("onboarding_step_"):
        try:
            step = int(data.split("_")[2])
        except Exception:
            step = 1
        try:
            await query.message.delete()
        except Exception:
            pass
        await send_onboarding_step(chat_id, first_name, step=step)
        return

    elif data == "onboarding_skip":
        await mark_onboarding_complete(chat_id)
        await redis_client.delete(f"onboarding_step:{chat_id}")
        await restore_bot_commands(chat_id)
        try:
            await query.message.delete()
        except Exception:
            pass
        # Give a small consolation XP for at least starting
        await add_xp(chat_id, first_name, "onboarding_skip")
        await send_initial_welcome(chat_id, first_name)
        return

    elif data == "onboarding_complete":
        await mark_onboarding_complete(chat_id)
        await redis_client.delete(f"onboarding_step:{chat_id}")
        await restore_bot_commands(chat_id)
        try:
            await query.message.delete()
        except Exception:
            pass
        await add_xp(chat_id, first_name, "onboarding_complete")
        asyncio.create_task(send_temporary_message(
            chat_id,
            "🎉 <b>Tour Complete!</b>\n\n✨ <b>+15 XP</b> added to your forest energy! 🌱",
            duration=5
        ))
        await asyncio.sleep(0.5)
        await send_initial_welcome(chat_id, first_name)
        return

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
        await send_animated_translated(
            chat_id=chat_id,
            animation_url=INVENTORY_GIF,
            caption=(
                "📜 <b>Ancient Library — Resource Scrolls</b>\n\n"
                "Choose the type of resource you need today:\n\n"
                "<i>Viewing items earns XP and helps you level up.</i>"
            ),
            reply_markup=kb_inventory(),
        )

    elif data == "show_resources":
        # Immersive description + clean menu
        immersive_text = (
            "📦 <b>The Resource Grove</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Deep within the Enchanted Clearing, where golden sunlight filters through ancient leaves, "
            "lies a hidden grove tended by the forest spirits themselves.\n\n"
            "Here they have gathered the rarest treasures and wisest knowledge from distant realms — "
            "all for kind wanderers like you.\n\n"
            "<i>Choose your path below, and may the trees guide you to what you seek...</i> 🍃✨"
        )
        
        try:
            await send_animated_translated(
                chat_id=chat_id,
                caption=immersive_text,
                animation_url=RESOURCES_GIF,
                reply_markup=kb_resources()
            )
        except Exception:
            # Safe fallback if edit fails (e.g. animation message)
            try:
                await query.message.delete()
            except:
                pass
            await send_animated_translated(
                chat_id=chat_id,
                caption=immersive_text,
                animation_url=RESOURCES_GIF,
                reply_markup=kb_resources()
            )
        return

    elif data == "set_language":
        await handle_set_language(chat_id, query=query)
        return
    
    elif data.startswith("toggle_notif|"):
        notif_type = data.split("|")[1]

        SUPABASE_SERVICES = ("netflix", "prime", "windows", "steam")
        REDIS_SERVICES    = ("daily_bonus",)

        if notif_type not in (*SUPABASE_SERVICES, *REDIS_SERVICES):
            await query.answer("❌ Unknown setting.", show_alert=True)
            return

        if notif_type == "daily_bonus":
            new_state = await toggle_daily_bonus_notif(chat_id)
        else:
            new_state = await toggle_service_notif(chat_id, notif_type)

        label_map = {
            "daily_bonus": "Daily Bonus Alert",
            "netflix":     "Netflix Alerts",
            "prime":       "Prime Alerts",
            "windows":     "Windows/Office Alerts",
            "steam":       "Steam Alerts",
        }

        context_map = {
            "daily_bonus": (
                "You'll see a popup when your daily XP is added.",
                "No popup when daily XP is added. XP still counts!"
            ),
            "netflix": (
                "You'll be notified when new Netflix cookies drop.",
                "You won't receive Netflix upload notifications."
            ),
            "prime": (
                "You'll be notified when new Prime cookies drop.",
                "You won't receive Prime upload notifications."
            ),
            "windows": (
                "You'll be notified when new Windows/Office keys drop.",
                "You won't receive Windows/Office upload notifications."
            ),
            "steam": (
                "You'll be notified when Steam accounts are uploaded.",
                "You won't receive Steam upload notifications."
            ),
        }

        icon    = "🔔" if new_state else "🔕"
        status  = "ON"  if new_state else "OFF"
        label   = label_map.get(notif_type, notif_type)
        context_on, context_off = context_map.get(notif_type, ("", ""))
        context = context_on if new_state else context_off

        # ✅ Answer query first — stops spinner immediately
        await query.answer(f"{icon} {label} is now {status}", show_alert=False)

        # 3-second confirmation message
        asyncio.create_task(send_temporary_message(
            chat_id,
            f"{icon} <b>{label} is now {status}</b>\n\n"
            f"<i>{context}</i> 🍃",
            duration=3
        ))

        # ✅ Pass query so old settings page is deleted before new one appears
        await handle_settings_page(chat_id, first_name, query=query)
        return

    elif data.startswith("lang_set|"):
        lang_code = data.split("|")[1]
        await set_user_language(chat_id, lang_code)
        
        flag, name = SUPPORTED_LANGUAGES.get(lang_code, ("🇬🇧", "English"))
        
        await send_temporary_message(
            chat_id=chat_id,
            text=f"✅ Language successfully changed to {flag} {name}!",
            duration=3
        )
        
        await query.message.delete()
        
        await send_full_menu(chat_id, first_name)
        return
    
    elif data == "view_notion_steam":
        await view_notion_steam_library(chat_id)

    # ── Games Pagination ──
    elif data.startswith("games_page|"):
        try:
            _, term, page_str = data.split("|")
            page = int(page_str)

            # Pass the query so we can EDIT the current message instead of sending new one
            await handle_searchsteam_command(chat_id, f"/searchsteam {term}", page=page, query=query)
        except Exception as e:
            print(f"Pagination error: {e}")
            await query.answer("Error loading page", show_alert=True)

    elif data.startswith("notion_page_"):
        page = int(data.split("_")[2])
        await view_notion_steam_library(chat_id, page=page, query=query)

    # ── QUICK INLINE AVAILABILITY UPDATE
    elif data.startswith("quick_set|"):
        _, page_id, new_status = data.split("|")
        
        # Get game name for nice message (we'll extract it from query if possible)
        game_name = "Game"  # fallback
        try:
            # Try to read game name from current message text (best effort)
            if query and query.message and query.message.text:
                lines = query.message.text.split("\n")
                for line in lines:
                    if line.startswith("🎮"):
                        game_name = line.replace("🎮", "").strip()
                        break
        except:
            pass

        headers = {
            "Authorization": f"Bearer {os.getenv('NOTION_TOKEN')}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        payload = {
            "properties": {
                "Availability": {"multi_select": [{"name": new_status}]}
            }
        }
        
        try:
            r = await http.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=headers, json=payload)
            if r.status_code in (200, 204):
                # Beautiful disappearing message
                success_text = f"✅ {game_name} marked as {new_status} successfully!"
                asyncio.create_task(send_temporary_message(chat_id, success_text, duration=2))
                
                # Refresh library
                await view_notion_steam_library(chat_id, page=page if 'page' in locals() else 0, query=query)
            else:
                await query.answer("❌ Failed to update", show_alert=True)
        except Exception as e:
            await query.answer(f"❌ Error: {str(e)[:30]}", show_alert=True)

    # ── BACK TO CARETAKER MENU
    elif data == "caretaker_menu":
        await handle_caretaker(chat_id, first_name)

    # ── WHEEL OF WHISPERS ──
    elif data == "show_wheel_menu":
        await query.answer()
        try:
            await query.message.delete()
        except:
            pass

        caption = (
            "🌟 <b>Wheel of Whispers</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "The ancient wooden wheel, glowing with runes and floating leaves, awaits your touch.\n\n"
            "You may spin <b>once per day</b>.\n"
            "Each spin brings a blessing from the forest spirits.\n\n"
            "<i>Would you like to test your luck?</i> ✨"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌟 Spin Now", callback_data="spin_now")],
            [InlineKeyboardButton("🏆 Wheel Leaderboard", callback_data="wheel_leaderboard")],
            [InlineKeyboardButton("ℹ️ About the Wheel", callback_data="about_wheel")],
            [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
        ])
        await send_animated_translated(
            chat_id=chat_id,
            animation_url=WHEEL_WHISPERS_GIF,
            caption=caption,
            reply_markup=keyboard
        )
        return

    # ── ACTUAL SPIN ──
    elif data == "spin_now":
        await query.answer()

        # ── Manila midnight TTL ──
        manila = pytz.timezone("Asia/Manila")
        now = datetime.now(manila)
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        ttl = int((midnight - now).total_seconds())

        # ── Check cooldown with time remaining ──
        if await redis_client.get(f"wheel_spin:{chat_id}"):
            ttl_remaining = await redis_client.ttl(f"wheel_spin:{chat_id}")
            hours = ttl_remaining // 3600
            mins = (ttl_remaining % 3600) // 60
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text=(
                    "🌿 <b>The Wheel of Whispers is resting...</b>\n\n"
                    f"You can spin again in <b>{hours}h {mins}m</b>\n"
                    "Resets at midnight Manila time ✨"
                ),
                parse_mode="HTML"
            )
            return

        reward_table = [
            {"rarity": "Common",    "xp": 13,  "text": "🌿 <b>Common Reward</b>\n\n+13 XP"},
            {"rarity": "Uncommon",  "xp": 30,  "text": "🍃 <b>Uncommon Reward</b>\n\n+30 XP"},
            {"rarity": "Rare",      "xp": 55,  "text": "🌟 <b>Rare Reward!</b>\n\n+55 XP\n+1 Extra Reveal Slot"},
            {"rarity": "Epic",      "xp": 75,  "text": "✨ <b>Epic Reward!</b>\n\n+75 XP"},
            {"rarity": "Legendary", "xp": 100, "text": "🌠 <b>Legendary Jackpot!</b>"},
            {"rarity": "Secret",    "xp": 45,  "text": "🪄 <b>Secret Reward!</b>\n\n+45 XP"},
        ]
        weights = [50, 28, 12, 6, 3, 1]

        selected = random.choices(reward_table, weights=weights, k=1)[0]
        rarity = selected["rarity"]
        reward_xp = selected["xp"]

        # ── Lock immediately after selection so exceptions can't allow re-spins ──
        await redis_client.setex(f"wheel_spin:{chat_id}", ttl, "1")

        # ── Rarity buildup messages ──
        RARITY_BUILDUP = {
            "Common":    "🌿 <b>The wheel slows gently...</b>\n\n<i>The forest offers a quiet blessing.</i>",
            "Uncommon":  "🍃 <b>The wheel hums with energy...</b>\n\n<i>Something stirs in the branches above.</i>",
            "Rare":      "🌟 <b>The runes flash brightly!</b>\n\n<i>The ancient spirits take notice...</i>",
            "Epic":      "✨ <b>The wheel blazes with golden light!</b>\n\n<i>The whole forest holds its breath...</i>",
            "Legendary": "🌠 <b>THE WHEEL ERUPTS IN STARLIGHT!</b>\n\n<i>Even the oldest trees have never seen this...</i>",
            "Secret":    "🪄 <b>Something unexpected flickers...</b>\n\n<i>A hidden spirit winks from the shadows...</i>",
        }

        # ── Rarity flavor text ──
        RARITY_FLAVOR = {
            "Common":    "The forest whispers a small blessing your way. 🌿",
            "Uncommon":  "The trees rustle with quiet approval. 🍃",
            "Rare":      "A golden leaf drifts down from the canopy above. 🌟",
            "Epic":      "The spirits dance around you in celebration! ✨",
            "Legendary": "The oldest tree in the clearing bows to you. 🌠",
            "Secret":    "A mischievous forest sprite vanishes with a grin. 🪄",
        }

        # ── Immersive spin animation ──
        loading = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=WHEEL_WHISPERS_GIF,
            caption="🌿 <b>You place your hand on the ancient wheel...</b>",
            parse_mode="HTML"
        )
        await asyncio.sleep(1.2)
        await loading.edit_caption(
            "🍃 <b>The runes begin to glow softly...</b>",
            parse_mode="HTML"
        )
        await asyncio.sleep(1.0)
        await loading.edit_caption(
            "✨ <b>Leaves and fireflies swirl around you...</b>",
            parse_mode="HTML"
        )
        await asyncio.sleep(1.0)
        await loading.edit_caption(
            "🌟 <b>The wheel spins faster and faster...</b>",
            parse_mode="HTML"
        )
        await asyncio.sleep(0.8)

        # ── Rarity buildup ──
        await loading.edit_caption(RARITY_BUILDUP[rarity], parse_mode="HTML")
        await asyncio.sleep(1.5)

        # ── Award XP via add_xp with override ──
        if reward_xp > 0:
            action_xp, _ = await add_xp(chat_id, first_name, "wheel_spin", xp_override=reward_xp)
            if action_xp:
                asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        # ── Bonus reveal slot ──
        got_bonus_slot = rarity in ["Rare", "Epic", "Legendary"]
        if got_bonus_slot:
            await redis_client.incr(f"daily_reveals_bonus:{chat_id}")
            await redis_client.expire(f"daily_reveals_bonus:{chat_id}", ttl)

        # ── Legendary: deliver fresh cookie ──
        got_fresh_cookie = False
        cookie_service = None
        if rarity == "Legendary":
            vamt_data = await get_vamt_data()
            if vamt_data:
                fresh = [
                    item for item in vamt_data
                    if item.get("service_type") in ["netflix", "prime"]
                    and int(item.get("remaining", 0)) > 0
                ]
                if fresh:
                    item = fresh[-1]
                    service_type = item["service_type"]
                    cookie = str(item.get("key_id", "")).strip()
                    display = str(item.get("display_name", f"{service_type.title()} Cookie"))

                    file_content = (
                        f"🌠 Legendary Wheel Reward — Fresh {service_type.title()} Cookie 🌠\n"
                        "════════════════════════════════\n"
                        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                        f"{cookie}\n"
                        "════════════════════════════════"
                    )
                    file_bytes = BytesIO(file_content.encode("utf-8"))
                    file_bytes.name = f"{display.replace(' ', '_')}.txt"

                    await tg_app.bot.send_document(
                        chat_id=chat_id,
                        document=file_bytes,
                        caption=(
                            f"🌠 <b>Legendary Jackpot!</b>\n\n"
                            f"Here is your fresh {service_type.title()} cookie!\n\n"
                            f"Enjoy, wanderer 🍃"
                        ),
                        parse_mode="HTML"
                    )
                    got_fresh_cookie = True
                    cookie_service = service_type
                else:
                    # Fallback — no cookies available, compensate with bonus XP
                    await tg_app.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "🌠 <b>Legendary Jackpot!</b>\n\n"
                            "The forest searched but the cookie spirits are resting...\n"
                            "You have been blessed with <b>+50 bonus XP</b> instead! 🍃"
                        ),
                        parse_mode="HTML"
                    )
                    # Use a different action key so cooldown doesn't block it
                    action_xp, _ = await add_xp(chat_id, first_name, "legendary_fallback", xp_override=50)
                    if action_xp:
                        asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        # ── Log to wheel_spins table ──
        asyncio.create_task(
            _log_wheel_spin(
                chat_id=chat_id,
                first_name=first_name,
                rarity=rarity,
                xp_earned=reward_xp,
                got_bonus_slot=got_bonus_slot,
                got_fresh_cookie=got_fresh_cookie,
                cookie_service=cookie_service,
            )
        )

        # ── Final result ──
        final_text = selected["text"] + f"\n\n{RARITY_FLAVOR[rarity]}"
        await loading.edit_caption(caption=final_text, parse_mode="HTML")
        return

    # ── ABOUT WHEEL OF WHISPERS ──
    elif data == "about_wheel":
        await query.answer()

        try:
            await query.message.delete()
        except:
            pass

        text = (
            "🌟 <b>About the Wheel of Whispers</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Every wanderer gets <b>1 free spin per day</b>.\n\n"
            "🎁 <b>Rewards:</b>\n"
            "• Common (50%)   → +13 XP\n"
            "• Uncommon (28%) → +30 XP\n"
            "• Rare (12%)     → +55 XP + 1 Extra Reveal Slot\n"
            "• Epic (6%)      → +75 XP\n"
            "• Legendary (3%) → Fresh Netflix or Prime Cookie\n"
            "• Secret (1%)    → Special surprise +45 XP\n\n"
            "Extra Reveal Slots can be used when revealing Netflix or Prime cookies.\n\n"
            "The wheel resets every midnight (Manila time).\n\n"
            "<i>May the forest bless your spins, wanderer.</i> ✨"
        )
        await send_animated_translated(
            chat_id=chat_id,
            animation_url=NEW_UPLOAD_GIF,
            caption=text,
            reply_markup=kb_back_to_wheel()
            )
        return
    
    # ── WHEEL LEADERBOARD ──
    elif data == "wheel_leaderboard":
        await query.answer()
        await handle_wheel_leaderboard(chat_id)
        return

    # ── NOOP (disabled button)
    elif data == "noop":
        await query.answer("No active event right now 🌿", show_alert=True)
        return

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

    # ── VAMT HELP POPUP (shortened - max 200 chars)
    elif data.startswith("explain_vamt") or data == "winoffice_help":
        parts = data.split("|")
        pending_cat = parts[1] if len(parts) > 1 else (await redis_client.get(f"winoffice_pending_cat:{chat_id}") or "win")
        cat_label = "Windows" if pending_cat in ("win", "windows") else "Office"
        cat_emoji = "🪟" if pending_cat in ("win", "windows") else "📑"

        help_text = (
            f"{cat_emoji} <b>What is VAMT / Remaining? ({cat_label} Keys)</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "✅ These are official <b>Microsoft Volume Activation</b> keys.\n"
            "One key can activate <b>multiple PCs</b> at once.\n\n"
            "📦 <b>Remaining</b> = How many more devices this key can still activate.\n\n"
            "• <b>Remaining: 10</b> → Works on up to 10 more computers\n"
            "• <b>Remaining: 0</b> → Key is fully used up\n\n"
            "⚡ <b>Tip:</b> Always use keys with the highest Remaining first — they disappear fast!\n\n"
            "<i>Test quickly after viewing. These are shared keys.</i> 🍃"
        )

        buttons = [
            [InlineKeyboardButton("⬅️ Back to Keys", callback_data=f"back_to_winoffice_keys|{pending_cat}")],
            [InlineKeyboardButton("⬅️ Back to Inventory", callback_data="check_vamt")]
        ]

        await query.message.edit_caption(
            caption=help_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    elif data.startswith("back_to_winoffice_keys|"):
        try:
            _, category = data.split("|")
        except:
            category = "win"
        
        # Re-show the original key list
        await show_winoffice_keys(chat_id, category, profile, query)
        return

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
    
    elif data == "show_profile_page":
        await handle_profile_page(chat_id, first_name, query)
        return
    
    elif data == "change_profile_logo":
        can_change, hours_left = await can_change_profile_gif(chat_id)
        if not can_change:
            days_left = hours_left // 24
            hours_remaining = hours_left % 24

            time_text = ""
            if days_left > 0:
                time_text = f"{days_left}d {hours_remaining}h"
            else:
                time_text = f"{hours_left}h"

            await query.answer("🌿 You've already changed your logo this week!", show_alert=False)
            await tg_app.bot.send_message(
                chat_id,
                f"🌿 <b>Profile Logo Cooldown</b>\n\n"
                f"You can only change your profile logo <b>once per week</b>.\n\n"
                f"⏳ Come back in <b>{time_text}</b> to update it again.\n\n"
                f"<i>The forest remembers your emblem, wanderer. 🍃</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back to Profile", callback_data="show_profile_page")]
                ])
            )
            return

        await redis_client.setex(f"waiting_for_logo:{chat_id}", 600, "1")
        await query.answer("✅ Send your new GIF now!", show_alert=False)

        await tg_app.bot.send_message(
            chat_id,
            "🌿 <b>Upload Your New Profile Logo</b>\n\n"
            "Send me a <b>GIF</b> (animation or .gif file) within <b>10 minutes</b>.\n"
            "Maximum size: <b>10 MB</b>\n\n"
            "<i>This will become your personal emblem in the forest. ✨</i>",
            parse_mode="HTML"
        )
        
    elif data == "show_settings_page":
        await handle_settings_page(chat_id, first_name, query)
        return

    elif data == "leaderboard_from_profile":
        await handle_leaderboard(chat_id)
        return

    elif data == "steam_claimed_limit":
        daily_limit = STEAM_DAILY_LIMITS.get(min(profile.get("level", 1), 10), 1)
        await query.answer(
            f"❌ You've already claimed your {daily_limit} account(s) for today!\n"
            "Come back tomorrow 🍃",
            show_alert=True
        )
        return
    
    # Add these new elif blocks anywhere in handle_callback:
    elif data.startswith("steam_page_"):
        try:
            page = int(data.split("_")[2])
            level = profile.get("level", 1)
            await show_steam_accounts(chat_id, first_name, level, query, page=page)
        except Exception as e:
            print(f"Steam page error: {e}")
        return
    
    # ── MY STEAM CLAIMS ──
    elif data == "my_steam_claims":
        await show_my_steam_claims(chat_id, first_name, query, page=0)
        return

    elif data.startswith("myclaims_page_"):
        try:
            page = int(data.split("_")[2])
            await show_my_steam_claims(chat_id, first_name, query, page=page)
        except Exception as e:
            print(f"My claims page error: {e}")
            await query.answer("Error loading page", show_alert=True)
        return

    # ── STEAM FEEDBACK: Working ──
    elif data.startswith("stfb_ok|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, account_email, game_name = parts

            # Spam guard
            fb_key = f"steam_fb:{chat_id}:{account_email}"
            if not await redis_client.set(fb_key, 1, ex=86400, nx=True):
                await query.answer(
                    "🌿 You already submitted feedback for this account!",
                    show_alert=True
                )
                return

            await handle_steam_feedback(
                chat_id, first_name,
                account_email, game_name,
                is_working=True,
                query=query
            )

    # ── STEAM FEEDBACK: Not Working ──
    elif data.startswith("stfb_bad|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, account_email, game_name = parts

            # Spam guard
            fb_key = f"steam_fb:{chat_id}:{account_email}"
            if not await redis_client.set(fb_key, 1, ex=86400, nx=True):
                await query.answer(
                    "🌿 You already submitted feedback for this account!",
                    show_alert=True
                )
                return

            await handle_steam_feedback(
                chat_id, first_name,
                account_email, game_name,
                is_working=False,
                query=query
            )

    # ── STEAM FEEDBACK: User Undo ──
    elif data.startswith("stfb_undo|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, account_email, game_name = parts

            # One undo only
            undo_key = f"steam_undo:{chat_id}:{account_email}"
            if not await redis_client.set(undo_key, 1, ex=60, nx=True):
                await query.answer(
                    "⚠️ You've already used your undo!",
                    show_alert=True
                )
                return

            # Reset feedback spam guard so they can resubmit
            await redis_client.delete(f"steam_fb:{chat_id}:{account_email}")

            # Notify owner of undo
            await tg_app.bot.send_message(
                OWNER_ID,
                f"↩️ <b>Steam Feedback Undone</b>\n\n"
                f"👤 {html.escape(first_name)} (<code>{chat_id}</code>)\n"
                f"🎮 {html.escape(game_name)}\n"
                f"📧 <code>{html.escape(account_email)}</code>\n\n"
                f"<i>User undid their ❌ report — account status unchanged.</i>",
                parse_mode="HTML"
            )

            await query.answer("↩️ Undone! No changes made.", show_alert=True)

            # Restore original feedback buttons
            try:
                feedback_kb = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "✅ Working",
                            callback_data=f"stfb_ok|{account_email}|{game_name}"
                        ),
                        InlineKeyboardButton(
                            "❌ Not Working",
                            callback_data=f"stfb_bad|{account_email}|{game_name}"
                        ),
                    ]
                ])
                # Strip the "You reported this as" line from caption
                current = query.message.caption or query.message.text or ""
                clean_caption = current.split("\n\n━━━")[0]
                await query.message.edit_caption(
                    caption=clean_caption,
                    parse_mode="HTML",
                    reply_markup=feedback_kb
                )
            except Exception:
                pass

    # ── OWNER: Restore account to Available ──
    elif data.startswith("owner_restore|"):
        if chat_id != OWNER_ID:
            await query.answer(
                "🌿 Only the Caretaker can do this.",
                show_alert=True
            )
            return

        parts = data.split("|")
        if len(parts) == 3:
            _, account_email, game_name = parts

            await _sb_patch(
                f"steamCredentials?email=eq.{account_email}",
                {"status": "Available"}
            )

            try:
                await query.message.edit_text(
                    query.message.text +
                    "\n\n✅ <b>Restored to Available by Caretaker.</b>",
                    parse_mode="HTML",
                    reply_markup=None
                )
            except Exception:
                pass

            await query.answer(
                "✅ Account restored to Available!",
                show_alert=True
            )

    # ── OWNER: Confirm mark as Unavailable ──
    elif data.startswith("owner_keep|"):
        if chat_id != OWNER_ID:
            await query.answer(
                "🌿 Only the Caretaker can do this.",
                show_alert=True
            )
            return

        parts = data.split("|")
        if len(parts) == 3:
            _, account_email, game_name = parts

            # ✅ Only NOW mark as unavailable
            await _sb_patch(
                f"steamCredentials?email=eq.{account_email}",
                {"status": "Unavailable"}
            )

            try:
                await query.message.edit_text(
                    query.message.text +
                    "\n\n🗑 <b>Marked Unavailable by Caretaker.</b>",
                    parse_mode="HTML",
                    reply_markup=None
                )
            except Exception:
                pass

            await query.answer(
                "🗑 Account marked Unavailable.",
                show_alert=False
            )

    elif data.startswith("claim_steam|"):
        parts = data.split("|")
        if len(parts) < 3:
            return

        account_email = parts[1]
        page = int(parts[2])
        level = profile.get("level", 1)
        tier = get_steam_tier(level)

        # Spam guard
        spam_key = f"steam_claim_spam:{chat_id}"
        if not await redis_client.set(spam_key, 1, ex=10, nx=True):
            await query.answer("🌿 One at a time, wanderer!", show_alert=True)
            return

        # ── Daily claim limit check ──
        if tier != "legend":
            daily_limit = STEAM_DAILY_LIMITS.get(min(level, 10), 1)
            claims_today = await get_steam_claims_today(chat_id)

            if claims_today >= daily_limit:
                await query.answer(
                    f"❌ You've already claimed your {daily_limit} "
                    f"account(s) for today!\nCome back tomorrow 🍃",
                    show_alert=True
                )
                return

        # Fetch account
        acc_data = await _sb_get(
            "steamCredentials",
            **{
                "email": f"eq.{account_email}",
                "status": "eq.Available",
                "select": "*",
            }
        ) or []

        if not acc_data:
            await query.answer(
                "❌ Account already claimed by someone else!",
                show_alert=True
            )
            await show_steam_accounts(chat_id, first_name, level, query, page=page)
            return

        acc = acc_data[0]
        game_name = acc.get("game_name") or "Steam Account"
        release_type = acc.get("release_type", "daily")

        # ── Tier access check ──
        if tier == "public":
            await query.answer(
                "❌ Reach Level 7 for bot access!", show_alert=True
            )
            return

        if release_type == "sunday_noon" and tier != "early_sunday" and tier != "legend":
            await query.answer(
                "❌ Sunday noon accounts require Level 9!",
                show_alert=True
            )
            return

        # ── Atomic claim ──
        success = await claim_steam_account(
            chat_id, first_name, account_email, game_name
        )
        if not success:
            await send_temporary_message(
                chat_id,
                "🌿 <b>You have already claimed this Steam account!</b>\n\n",
                duration=3
            )
    
            await show_steam_accounts(chat_id, first_name, level, query, page=page)
            return
        
        # ── Deliver ──
        password = acc.get("password", "")
        steam_id = acc.get("steam_id", "")
        type_badge = (
            "🌟 Sunday Bonus Account"
            if release_type == "sunday_noon"
            else "📅 Daily Account"
        )

        claims_after = claims_today + 1 if tier != "legend" else 0
        claims_left = (
            max(0, daily_limit - claims_after)
            if tier != "legend"
            else 999
        )
        claims_left_text = (
            f"\n📊 Claims left today: <b>{claims_left}</b>"
            if tier != "legend"
            else ""
        )

        caption = (
            f"🎮 <b>{html.escape(game_name)} — Claimed!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"🏷️ {type_badge}\n\n"
            f"📧 Login: <tg-spoiler>{html.escape(account_email)}</tg-spoiler>\n"
            f"🔑 Password: <tg-spoiler>{html.escape(password)}</tg-spoiler>\n"
            + (f"🆔 Steam ID: <code>{steam_id}</code>\n" if steam_id else "")
            + claims_left_text
            + "\n\n⚠️ <b>Rules:</b>\n"
            "• Do not change the password\n"
            "• Do not make any purchases\n"
            "• Do not log out other sessions\n\n"
            "<i>Enjoy your game, wanderer! 🍃</i>"
        )

        image_url = acc.get("image_url", "").strip() if acc.get("image_url") else ""

        # ── Feedback buttons ──
        feedback_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Working",
                    callback_data=f"stfb_ok|{account_email}|{game_name[:30]}"
                ),
                InlineKeyboardButton(
                    "❌ Not Working",
                    callback_data=f"stfb_bad|{account_email}|{game_name[:30]}"
                ),
            ]
        ])

        if image_url:
            try:
                await tg_app.bot.send_photo(
                    chat_id=chat_id,
                    photo=image_url,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=feedback_kb
                )
            except Exception:
                print(f"⚠️ Photo failed for {account_email}: {e} | URL: {image_url[:60]}")
                await tg_app.bot.send_message(
                    chat_id, caption,
                    parse_mode="HTML",
                    reply_markup=feedback_kb
                )
        else:
            await tg_app.bot.send_message(
                chat_id, caption,
                parse_mode="HTML",
                reply_markup=feedback_kb
            )

        asyncio.create_task(send_temporary_message(
            chat_id,
            f"✨ <i>{html.escape(game_name)} successfully claimed!</i>",
            duration=3
        ))

        # ── Award XP for successful Steam claim ──
        if tier == "legend":
            steam_claim_xp = 15
        elif release_type == "sunday_noon":
            steam_claim_xp = 30
        else:
            steam_claim_xp = 20

        action_xp, _ = await add_xp(chat_id, first_name, "steam_claim", xp_override=steam_claim_xp)
        if action_xp:
            asyncio.create_task(send_xp_feedback(chat_id, action_xp))

        # Refresh the list
        await show_steam_accounts(chat_id, first_name, level, query, page=page)
        return

    # ── INVENTORY FILTERS ──
    elif data.startswith("vamt_filter_"):
        category = data.replace("vamt_filter_", "").lower()
        action_map = {
            "win": "view_windows", "windows": "view_windows",
            "office": "view_office", "netflix": "view_netflix", "prime": "view_prime",
        }

        # Only award XP immediately for netflix/prime/steam — win/office handles it later
        if category not in ("win", "windows", "office"):
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
            await show_steam_accounts(chat_id, first_name, level, query, page=0)
            return


        # Cookie types
        if category in ("netflix", "prime"):
            await show_paginated_cookie_list(category, chat_id, query, page=0)
            return
        
        # ── NEW: First-time guide for Windows / Office ──────────────────
        if category in ("win", "windows", "office"):
                seen = await has_seen_winoffice_guide(chat_id)

                if not seen:
                    # First time → show guide ONLY (no cap deduction yet)
                    await redis_client.setex(f"winoffice_pending_cat:{chat_id}", 3600, category)
                    cat_label = "Windows" if category in ("win", "windows") else "Office"
                    cat_emoji = "🪟" if category in ("win", "windows") else "📑"
            
                    await query.message.edit_caption(
                        caption=(
                            f"{cat_emoji} <b>Before you open the {cat_label} scrolls...</b>\n\n"
                            "━━━━━━━━━━━━━━━━━━\n\n"
                            "🔵 <b>What is a VAMT Key?</b>\n"
                            "It is a <b>Volume Activation</b> key. These are official Microsoft keys designed to activate multiple PCs.\n\n"
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
                
                # Normal flow (guide already seen) → now consume cap
                allowed, remaining = await try_consume_view_cap(chat_id, category)
                if not allowed:
                    cat_label = "Windows" if category in ("win", "windows") else "Office"
                    cat_emoji = "🪟" if category in ("win", "windows") else "📑"
                    max_daily = get_max_daily_views(profile.get("level", 1), category)
                    
                    await query.message.edit_caption(
                        caption=(
                            f"{cat_emoji} <b>{cat_label} Daily Limit Reached</b>\n\n"
                            f"🌿 You have already viewed your maximum <b>{max_daily}</b> keys today.\n\n"
                            f"Come back tomorrow after midnight (Manila time) for fresh keys 🍃\n\n"
                            f"<i>Remaining views today: <b>{remaining}</b></i>"
                        ),
                        parse_mode="HTML",
                        reply_markup=kb_back_inventory(),
                    )
                    await query.answer("Daily view limit reached", show_alert=False)
                    return
                
                # Award XP now that we're actually showing the keys
                action_xp, _ = await add_xp(chat_id, first_name,
                    "view_windows" if category in ("win","windows") else "view_office")
                if action_xp:
                    asyncio.create_task(send_xp_feedback(chat_id, action_xp))

                await show_winoffice_keys(chat_id, category, profile, query)
                return
        

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
            real_key = await redis_client.get(f"reveal_key:{chat_id}:{service_type}:{idx_str}")
            key_id = real_key if real_key else idx_str
            await handle_key_feedback(chat_id, first_name, key_id, service_type, is_working, query)

    # Windows/Office per-item feedback (uses token + Redis)
    elif data.startswith("wkfb_ok|") or data.startswith("wkfb_bad|"):
        parts = data.split("|", 1)
        if len(parts) == 2:
            _, token = parts
            is_working = data.startswith("wkfb_ok|")
            stored = await redis_client.get(f"winkey:{token}")
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
                f"🪟 <b>Windows & Office Keys</b>\n"
                f"• {_guidance_tier_text('windows')}\n\n"
                f"🍿 <b>Netflix Premium Cookies</b>\n"
                f"• {_guidance_tier_text('netflix')}\n\n"
                f"🎥 <b>PrimeVideo Premium Cookies</b>\n"
                f"• {_guidance_tier_text('prime')}\n\n"
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
            msg = await send_animated_translated(
                chat_id=chat_id,
                animation_url=GUIDANCE_GIF,
                caption=text,
                reply_markup=keyboard,
            )
            await _remember(chat_id, msg.message_id)
            if not profile.get("has_seen_menu", False):
                asyncio.create_task(update_has_seen_menu(chat_id))
        else:
            try:
                await query.message.edit_caption(caption=text, parse_mode="HTML", reply_markup=keyboard)
            except Exception:
                msg = await send_animated_translated(
                    chat_id=chat_id,
                    animation_url=GUIDANCE_GIF,
                    caption=text,
                    reply_markup=keyboard,
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
        stored_msg_id = await redis_client.get(f"reveal_msg:{chat_id}:{service_type}")
        if stored_msg_id:
            try:
                await tg_app.bot.delete_message(chat_id, int(stored_msg_id))
            except Exception:
                pass
            await redis_client.delete(f"reveal_msg:{chat_id}:{service_type}")

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
        final = await send_animated_translated(
            chat_id=chat_id,
            animation_url=ABOUT_GIF,
            caption=lore,
            reply_markup=kb_back(),
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
                    "📜 <b>Add New Patch Note</b>\n━━━━━━━━━━━━━━━━━━\n"
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
        elif data == "caretaker_uploadkeys":
            await handle_uploadkeys_command(chat_id)
        elif data == "caretaker_health":
            await handle_status(chat_id)
        elif data == "caretaker_viewreports":
            await handle_view_reports(chat_id)
        elif data == "caretaker_setinfo":
            await tg_app.bot.send_message(
                chat_id,
                "📝 <b>Set Forest Info</b>\n\n"
                "Reply with the command in this format:\n\n"
                "<code>/setforestinfo 1.4.5 April 14, 2026 · 02:58 PM</code>",
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
        elif data == "caretaker_uploadsteam":
            await send_animated_translated(
                chat_id,
                "🎮 <b>Steam Account Uploader</b>\n\n"
                "<b>Single account:</b>\n"
                "<code>/uploadsteam\n"
                "email\n"
                "password\n"
                "Game Name <b>(optional)</b>\n"
                "SteamID64 <b>(optional)</b>\n"
                "Banner (optional)</code>\n\n"
                "<b>Bulk (separate accounts with a blank line):</b>\n"
                "<code>/uploadsteam\n"
                "email1\n"
                "password1\n"
                "Game Name\n"
                "76561198XXXXXXXXX\n"
                "Banner.jpg\n\n"
                "email2\n"
                "password2\n\n"
                "email3\n"
                "password3\n"
                "Only Game Name</code>\n\n"
                "⚠️ <i>Minimum required: email + password</i>\n"
                "<i>Tip: Use /searchsteam first to check for duplicates.</i>",
                animation_url=STEAM_RESULT_GIF,
            )
        elif data == "caretaker_searchsteam":
            await handle_searchsteam_command(chat_id, "/searchsteam")

    elif data == "winoffice_got_it":
        await mark_winoffice_guide_seen(chat_id)
        pending_cat = await redis_client.get(f"winoffice_pending_cat:{chat_id}") or "win"
        await redis_client.delete(f"winoffice_pending_cat:{chat_id}")
        await show_winoffice_keys(chat_id, pending_cat, profile, query)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN UPDATE PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

async def process_update(update_data: dict):
    update = Update.de_json(update_data, tg_app.bot)

    # ── Improved Cold Start + Auto-delete after 5 seconds
    if update.message:
        chat_id = update.effective_chat.id
        cold_key = "cold_start_sent"
        if not await redis_client.get(cold_key):
            await redis_client.setex(cold_key, 3600, "1")   # 1 hour cooldown

            try:
                msg = await tg_app.bot.send_message(
                    chat_id=chat_id,
                    text="Yawnnn~ 😴💤\nThe forest just woke me up!\nReady for you now, wanderer ✨🍃"
                )
                # Auto-delete after 5 seconds
                await asyncio.sleep(5)
                await tg_app.bot.delete_message(chat_id=chat_id, message_id=msg.message_id)
            except:
                pass

    # ── ONBOARDING GUARD ──
    if update.message and update.message.text:
        raw_check = update.message.text.strip()
        check_chat_id = update.effective_chat.id
        
        # Only /start is allowed during onboarding
        if not raw_check.lower().startswith("/start"):
            onboarding_done = await has_completed_onboarding(check_chat_id)
            if not onboarding_done:
                profile_check = await get_user_profile(check_chat_id)
                if not profile_check:  # truly new user
                    await tg_app.bot.send_message(
                        check_chat_id,
                        "🌿 <b>Please complete the welcome tour first!</b>\n\n"
                        "Use /start to begin your journey into the Enchanted Clearing. 🍃",
                        parse_mode="HTML"
                    )
                    return

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

    if not text.startswith("/clear"):
        await _remember(chat_id, msg_id)

    # Registration guard (except /start)
    if not text.startswith("/start"):
        profile = await get_user_profile(chat_id)
        if not profile:
            await send_animated_translated(
                chat_id=chat_id,
                animation_url=HELLO_GIF,
                caption=(
                    "<b>🌲 You stand at the edge of a mysterious forest.</b>\n\n"
                    "The ancient trees watch you with quiet curiosity.\n\n"
                    "To step into the Enchanted Clearing..."
                ),
                reply_markup=kb_start(),
            )
            return

    asyncio.create_task(update_last_active(chat_id))

    # ── Command dispatch ──
    if text.startswith("/start"):
        # ── Fetch profile first so we know if user is new or existing
        profile = await get_user_profile(chat_id)
        is_new_user = profile is None

        referral_stored = True  # default
        if " " in raw:
            payload = raw.split(maxsplit=1)[1]
            if payload.startswith("ref_"):
                try:
                    referrer_id = int(payload[4:])
                    referral_stored = await store_pending_referral(chat_id, referrer_id)

                    if not referral_stored:
                        # Self-referral
                        asyncio.create_task(
                            send_temporary_message(
                                chat_id,
                                "🌿 Nice try, wanderer!\n\n"
                                "You can't refer yourself in the forest 🍃\n"
                                "The trees are watching 👀",
                                duration=8
                            )
                        )
                    elif is_new_user:
                        # New user who was referred
                        asyncio.create_task(
                            send_temporary_message(
                                chat_id,
                                "✨ <b>Someone invited you!</b>\n\nYou'll receive <b>+40 XP</b> on your first daily bonus 🌱",
                                duration=8
                            )
                        )
                    else:
                        # ← NEW: Already claimed referral
                        asyncio.create_task(
                            send_temporary_message(
                                chat_id,
                                "🌿 <b>You've already claimed a referral!</b>\n\n"
                                "The forest remembers your previous invitation.\n"
                                "No extra bonus this time, but you're always welcome back! 🍃",
                                duration=6
                            )
                        )
                except:
                    pass

        await send_initial_welcome(chat_id, first_name)

    elif text.startswith("/start"):
        # ── SOFT RATE LIMIT ON /START
        start_key = f"start_cd:{chat_id}"
        if await redis_client.exists(start_key):
            await tg_app.bot.send_message(
                chat_id,
                "🌿 <b>Slow down, wanderer!</b>\n\n"
                "The ancient trees are still greeting you...\n"
                "Please wait a moment before trying again 🍃",
                parse_mode="HTML"
            )
            return
        await redis_client.setex(start_key, 10, 1)

    elif text.startswith("/setlogo"):
        can_change, hours_left = await can_change_profile_gif(chat_id)
        
        if not can_change:
            await tg_app.bot.send_message(
                chat_id,
                f"🌿 <b>You can only change your profile logo once per week.</b>\n\n"
                f"Come back in <b>{hours_left}</b> hours! ✨",
                parse_mode="HTML"
            )
            return
        
    elif text.startswith("/resetprofilegif"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can use this.")
            return

        parts = text.split()
        target_id = int(parts[1]) if len(parts) > 1 else chat_id

        deleted = await redis_client.delete(f"profile_gif_cooldown:{target_id}")

        await tg_app.bot.send_message(
            chat_id,
            f"✅ <b>Profile GIF cooldown reset for <code>{target_id}</code></b>\n\n"
            f"They can now change their profile logo again. 🌿\n"
            f"Deleted {deleted} cooldown key.",
            parse_mode="HTML"
        )

        await redis_client.setex(f"waiting_for_logo:{chat_id}", 600, "1")  # 10 min to upload
        await tg_app.bot.send_message(
            chat_id,
            "🌿 <b>Upload Your Profile Logo</b>\n\n"
            "Send me a <b>GIF</b> (as animation or document).\n"
            "Maximum 10 MB.\n\n"
            "<i>This will become your personal emblem in the forest.</i> ✨",
            parse_mode="HTML"
        )

    elif text.startswith("/uploadkeys"):
        await handle_uploadkeys_command(chat_id)

    elif text.startswith("/resetsteamclaim"):
        if chat_id != OWNER_ID:
            return
        parts = text.split()
        target_id = int(parts[1]) if len(parts) > 1 else chat_id

        manila = pytz.timezone("Asia/Manila")
        today_start = datetime.now(manila).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).astimezone(pytz.utc).isoformat()

        ok = await _sb_delete(f"steam_claims?chat_id=eq.{target_id}&claimed_at=gte.{today_start}")
        await tg_app.bot.send_message(
            chat_id,
            f"✅ Steam claim limit reset for <code>{target_id}</code>",
            parse_mode="HTML"
        )

    elif text.startswith("/resetonboarding"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can use this.")
            return
        parts = text.split()
        target_id = int(parts[1]) if len(parts) > 1 else chat_id
        deleted1 = await redis_client.delete(f"onboarding_done:{target_id}")
        deleted2 = await redis_client.delete(f"onboarding_step:{target_id}")
        await _sb_patch(        # ← add this
            f"user_profiles?chat_id=eq.{target_id}",
            {"onboarding_completed": False}
        )
        await tg_app.bot.send_message(
            chat_id,
            f"✅ Onboarding reset for <code>{target_id}</code>\n"
            f"Redis: {deleted1 + deleted2} key(s) deleted\n"
            f"Supabase: onboarding_completed → false\n\n"
            f"They'll see the tutorial again on next /start.",
            parse_mode="HTML"
        )

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

    elif text.startswith("/searchsteam"):
        await handle_searchsteam_command(chat_id, raw)

    elif text.startswith("/uploadsteam"):
        await handle_uploadsteam_command(chat_id, raw)

    elif text.startswith("/profile"):
        await handle_profile_page(chat_id, first_name)

    elif text.startswith("/viewreports"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can view reports.")
            return
        await handle_view_reports(chat_id)

    elif text.startswith("/caretaker"):
        await handle_caretaker(chat_id, first_name)

    elif text.startswith("/mystats"):
        await handle_profile_page(chat_id, first_name) 

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
            deleted1 = await redis_client.delete(f"seen_winoffice_guide:{target_id}")
            deleted2 = await redis_client.delete(f"winoffice_pending_cat:{target_id}")
            await tg_app.bot.send_message(
                chat_id,
                f"✅ Guide reset for <code>{target_id}</code>\n\n"
                f"Deleted: {deleted1 + deleted2} key(s)",
                parse_mode="HTML"
            )
        except Exception as e:
            await tg_app.bot.send_message(chat_id, f"❌ Error: {e}")

    # ── RESET WHEEL (Safe - only removes daily cooldown) ──
    elif text.startswith("/resetwheel"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can reset the wheel.")
            return
        
        parts = text.split()
        target_id = int(parts[1]) if len(parts) > 1 else chat_id
        
        deleted = await redis_client.delete(f"wheel_spin:{target_id}")
        
        await tg_app.bot.send_message(
            chat_id,
            f"✅ <b>Wheel spin reset for user {target_id}</b>\n\n"
            f"The Wheel of Whispers is now ready again! ✨\n"
            f"Deleted {deleted} cooldown key.",
            parse_mode="HTML"
        )

    # ── TEST DAILY (Full test reset) ──
    elif text.startswith("/testdaily"):
        if chat_id != OWNER_ID:
            await tg_app.bot.send_message(chat_id, "🌿 Only the Forest Caretaker can use this.")
            return
        
        parts = text.split()
        if len(parts) < 2:
            await tg_app.bot.send_message(
                chat_id,
                "🔧 <b>Usage:</b> <code>/testdaily &lt;user_id&gt;</code>\n\n"
                "Example:\n<code>/testdaily 7399488750</code>",
                parse_mode="HTML"
            )
            return

        try:
            target_id = int(parts[1])

            # Reset everything needed for clean testing
            keys = [
                f"daily_bonus:{target_id}",
                f"pending_ref:{target_id}",
                f"ref_awarding:{target_id}",
            ]
            deleted = await redis_client.delete(*keys)

            await tg_app.bot.send_message(
                chat_id,
                f"✅ <b>Fully reset for user {target_id}</b>\n\n"
                f"• Daily bonus → now available again\n"
                f"• Pending referral → cleared\n"
                f"• Referral lock → cleared\n\n"
                f"Deleted {deleted} Redis key(s)\n\n"
                f"You can now test daily bonus or referral link cleanly.",
                parse_mode="HTML"
            )
        except ValueError:
            await tg_app.bot.send_message(chat_id, "❌ Invalid user ID. Must be a number.")
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

        # Support BOTH formats:
        # Format A (multi-line):
        #   /setforestinfo
        #   1.4.5
        #   April 15, 2026
        #   02:58 PM
        #
        # Format B (single-line):
        #   /setforestinfo 1.4.5 April 15, 2026 · 02:58 PM

        body = raw[len("/setforestinfo"):].strip()

        if "\n" in body:
            lines = [l.strip() for l in body.splitlines() if l.strip()]
            if len(lines) < 3:
                await tg_app.bot.send_message(
                    chat_id,
                    "📝 Multi-line usage:\n\n"
                    "<code>/setforestinfo\n1.4.5\nApril 15, 2026\n02:58 PM</code>",
                    parse_mode="HTML"
                )
                return
            version = lines[0]
            custom_datetime = f"{lines[1]} · {lines[2]}"
        elif body:
            # Single-line: version is first word, rest is datetime
            parts = body.split(" ", 1)
            if len(parts) < 2:
                await tg_app.bot.send_message(chat_id, "❌ Please include version and datetime.")
                return
            version = parts[0]
            custom_datetime = parts[1].strip()
        else:
            await tg_app.bot.send_message(
                chat_id,
                "📝 Usage:\n\n"
                "<code>/setforestinfo\n1.4.5\nApril 15, 2026\n02:58 PM</code>",
                parse_mode="HTML"
            )
            return

        await set_bot_info(version, custom_datetime, chat_id)

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

    elif text.startswith("/spin"):
        await handle_callback(Update.de_json({
            "callback_query": {
                "data": "spin_wheel",
                "from": update.effective_user.to_dict() if update.effective_user else {}
            }
        }, tg_app.bot))
        return
    
    elif text.startswith(("/updates", "/update")):
        await handle_updates(chat_id)

    elif text.startswith(("/lang", "/language")):
        await handle_set_language(chat_id)

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
