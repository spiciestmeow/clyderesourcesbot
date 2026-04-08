import os
import asyncio
import html
import httpx
import traceback
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ContextTypes
from datetime import datetime
from collections import Counter
import pytz
import time

# ==================== GLOBAL ERROR HANDLER ====================
async def global_error_handler(update: object, context):
    """Tracks ALL errors - prints to Vercel + sends to your private chat"""
    
    error = context.error
    error_type = type(error).__name__
    error_msg = str(error)
    tb_string = "".join(traceback.format_exception(None, error, error.__traceback__))

    # Print to Vercel Logs
    print(f"🔴 ERROR: {error_type} | {error_msg}")
    print("─" * 80)
    print(tb_string)
    print("─" * 80)

    # Send to your private chat
    OWNER_CHAT_ID = 7399488750
    notification = (
        f"🔴 <b>Forest Hub Error Detected</b>\n\n"
        f"<b>Type:</b> {error_type}\n"
        f"<b>Message:</b> {html.escape(error_msg)}\n\n"
        f"<b>Time:</b> {datetime.now(pytz.timezone('Asia/Manila')).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"<pre>{html.escape(tb_string[-3000:])}</pre>"
    )

    try:
        await context.bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=notification,
            parse_mode='HTML'
        )
    except Exception as send_err:
        print(f"Failed to send error notification: {send_err}")

forest_memory = {}
app = Flask(__name__)


# ==================== ANTI-XP ABUSE ====================
xp_cooldowns = {}
user_action_history = {}

COOLDOWN_SECONDS = {
    "view_windows": 12,
    "view_office": 12,
    "view_netflix": 12,
    "reveal_netflix": 18,
    "profile": 12,
    "clear": 25,
    "guidance": 20,
    "lore": 20,
    "general": 5,
}

MAX_ACTIONS_PER_MINUTE = 8

# ==================== CONFIG ====================
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ==================== GIFS ====================
WELCOME_GIF = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExeWZzOHRrYjRycTI4d2Z2eXR6bWNiMm1yYXVqbzVrb3NmczB2ZHdmayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/wsKqNQmHYZfs4/giphy.gif"
MENU_GIF = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExczJsZ25kM2N1N2twOHhmNWRsd3N6eWlyZ3N5M29pdmxsdDMzOHVscCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"
INVENTORY_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
ABOUT_GIF = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdTFqMHB0ODVxdmFoMHl3dzZyM2swanlicmRibGk1bjdpcjFsdnl1biZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/x5HlLDaLMZNVS/giphy.gif"
HELP_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNWxybTY5bXA0ejg1cGxxNTY3d3IyY3A4NGtkZ2gyOXkxcnlwZzN2NCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/J4FsxFgZgN2HS/giphy.gif"
LOADING_GIF = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
MYID_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
CLEAN_GIF = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
GUIDANCE_GIF = "https://64.media.tumblr.com/129ee065eff5fee81fab81c4f8e2ed4f/tumblr_oui1cvflgE1r9i2iuo1_r7_540.gif"
HELLO_GIF = "https://i.pinimg.com/originals/6a/a3/7f/6aa37fd0017bdb291ca8cbdd8b0ede52.gif"

# ==================== MAINTENANCE MODE ====================
MAINTENANCE_MODE = True
MAINTENANCE_MESSAGE = (
    "🌿 <b>The Forest Hub is currently under maintenance</b>\n\n"
    "We are making improvements and will be back shortly.\n\n"
    "<i>Thank you for your patience.</i> 🍃"
)

tg_app = Application.builder().token(TOKEN).build()

# Register global error handler - catches almost all errors
tg_app.add_error_handler(global_error_handler)

loop = asyncio.new_event_loop()
BOT_START_TIME = datetime.now(pytz.utc)
asyncio.set_event_loop(loop)

# ==================== DATABASE ====================
async def get_vamt_data():
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"🔴 Supabase Error: {e}")
            return None

# ==================== KEYBOARDS ====================
def get_start_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌿 Enter the Forest Hub", callback_data="show_main_menu")]])

def get_full_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account"),
         InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("❓ Guidance", callback_data="help"),
         InlineKeyboardButton("ℹ️ Lore", callback_data="about")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/caydigitals")]
    ])

def get_inventory_categories():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪟 Windows Keys", callback_data="vamt_filter_win"),
         InlineKeyboardButton("📑 Office Keys", callback_data="vamt_filter_office")],
        [InlineKeyboardButton("🍿 Netflix Premium Cookies", callback_data="vamt_filter_netflix")],
        [InlineKeyboardButton("🎥 PrimeVideo Premium Cookies", callback_data="vamt_filter_prime")],
        [InlineKeyboardButton("🎮 Steam Accounts", callback_data="vamt_filter_steam")],
        [InlineKeyboardButton("⬅️ Back to Hub", callback_data="main_menu")]
    ])

def get_back_to_inventory_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Scroll Selection", callback_data="check_vamt")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to the Hub", callback_data="main_menu")]])


def get_first_time_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❓ Start Here → Guidance", callback_data="help")],
        [InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account")],
        [InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("ℹ️ Lore", callback_data="about")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/caydigitals")]
    ])

# ==================== LEVELING SYSTEM HELPERS ====================
async def get_user_profile(chat_id):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_profiles?chat_id=eq.{chat_id}"
                "&select=*,has_seen_menu,created_at,total_xp_earned,"
                "windows_views,office_views,netflix_views,netflix_reveals,"
                "times_cleared,guidance_reads,lore_reads,profile_views",   # ← Added profile_views
                headers=headers
            )
            data = response.json()
            return data[0] if data else None
        except Exception as e:
            print(f"Error fetching profile: {e}")
            return None

async def update_has_seen_menu(chat_id):
    """Mark that the user has seen the main menu"""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    payload = {"has_seen_menu": True}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/user_profiles?chat_id=eq.{chat_id}",
                headers=headers,
                json=payload
            )
        except Exception as e:
            print(f"Failed to update has_seen_menu: {e}")
            
def get_cumulative_xp_for_level(target_level: int) -> int:
    """Returns total XP needed to reach this level (new balanced formula)"""
    if target_level <= 1:
        return 0
    # 200 base + 100 increasing per level (feels good for a Telegram bot)
    return sum(200 + (lvl * 100) for lvl in range(1, target_level))

async def add_xp(chat_id, first_name, action="general", query=None):
    """Add XP with cooldown + rate limit + true one-time rewards only"""

    current_time = time.time()

    if chat_id not in xp_cooldowns:
        xp_cooldowns[chat_id] = {}
    if chat_id not in user_action_history:
        user_action_history[chat_id] = []

    # Global Rate Limit
    user_action_history[chat_id] = [t for t in user_action_history[chat_id] if current_time - t < 60]

    # Cleanup old cooldown entries to prevent memory growth
    xp_cooldowns[chat_id] = {
        action: t for action, t in xp_cooldowns[chat_id].items()
        if current_time - t < 600  # remove entries older than 10 minutes
    }

    if len(user_action_history[chat_id]) >= MAX_ACTIONS_PER_MINUTE:
        if query:
            try:
                await query.answer("🌿 The forest is quite busy right now... Please slow down.", show_alert=True)
            except:
                pass
        return False

    # Action-specific Cooldown
    last_used = xp_cooldowns[chat_id].get(action, 0)
    if current_time - last_used < COOLDOWN_SECONDS.get(action, 8):
        if query:
            try:
                await query.answer("🌿 The forest spirits need a moment to rest... Try again soon!", show_alert=True)
            except:
                pass
        return False

    # Record cooldown
    xp_cooldowns[chat_id][action] = current_time
    user_action_history[chat_id].append(current_time)

    # ====================== XP AMOUNT LOGIC ======================
    profile = await get_user_profile(chat_id)

    xp_amount = 0   # Default is now 0 (no accidental XP)

    if action == "guidance":
        current_reads = profile.get('guidance_reads', 0) if profile else 0
        if current_reads == 0:           # First time only
            xp_amount = 8
    elif action == "lore":
        current_reads = profile.get('lore_reads', 0) if profile else 0
        if current_reads == 0:           # First time only
            xp_amount = 8
    elif action == "view_windows":
        xp_amount = 6
    elif action == "view_office":
        xp_amount = 6
    elif action == "view_netflix":
        xp_amount = 6
    elif action == "reveal_netflix":
        xp_amount = 10
    elif action == "profile":
        xp_amount = 5
    elif action == "clear":
        xp_amount = 5

    # ====================== Database Update ======================
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

    if profile:
        stats_update = {}

        if action == "view_windows":
            stats_update["windows_views"] = (profile.get('windows_views') or 0) + 1
        elif action == "view_office":
            stats_update["office_views"] = (profile.get('office_views') or 0) + 1
        elif action == "view_netflix":
            stats_update["netflix_views"] = (profile.get('netflix_views') or 0) + 1
        elif action == "reveal_netflix":
            stats_update["netflix_reveals"] = (profile.get('netflix_reveals') or 0) + 1
        elif action == "clear":
            stats_update["times_cleared"] = (profile.get('times_cleared') or 0) + 1
        elif action == "guidance":
            stats_update["guidance_reads"] = (profile.get('guidance_reads') or 0) + 1
        elif action == "lore":
            stats_update["lore_reads"] = (profile.get('lore_reads') or 0) + 1
        elif action == "profile":
            stats_update["profile_views"] = (profile.get('profile_views') or 0) + 1

        current_total = profile.get('total_xp_earned') or 0
        stats_update["total_xp_earned"] = current_total + xp_amount

        new_xp = (profile.get('xp') or 0) + xp_amount
        old_level = profile.get('level') or 1
        new_level = old_level

        while True:
            xp_required_for_next = get_cumulative_xp_for_level(new_level + 1)
            if new_xp < xp_required_for_next:
                break
            new_level += 1

        leveled_up = new_level > old_level

        payload = {
            "xp": new_xp,
            "level": new_level,
            "first_name": first_name,
            "last_active": datetime.now(pytz.utc).isoformat()
        }
        payload.update(stats_update)

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/user_profiles?chat_id=eq.{chat_id}",
                headers=headers,
                json=payload
            )

        # ====================== XP HISTORY LOGGING (NEW FEATURE) ======================
        # Only log when the user actually earned XP (this keeps history clean)
        if xp_amount > 0:
            await log_xp_history(
                chat_id=chat_id,
                first_name=first_name,
                action=action,
                xp_earned=xp_amount,
                previous_xp=profile.get('xp') or 0,
                new_xp=new_xp,
                previous_level=old_level,
                new_level=new_level
            )

        if leveled_up:
            await send_level_up_message(chat_id, first_name, old_level, new_level)

    else:
        # New user
        payload = {
            "chat_id": chat_id,
            "first_name": first_name,
            "xp": 0,
            "level": 1,
            "last_active": datetime.now(pytz.utc).isoformat(),
            "has_seen_menu": False,
            "created_at": "now()",
            "total_xp_earned": 0,
            "windows_views": 0,
            "office_views": 0,
            "netflix_views": 0,
            "netflix_reveals": 0,
            "times_cleared": 0,
            "guidance_reads": 0,
            "lore_reads": 0,
            "profile_views": 0
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/user_profiles",
                headers=headers,
                json=payload
            )

    return True

# ==================== UPDATE LAST ACTIVE LOGGING ====================
async def update_last_active(chat_id: int):
    """Automatically update last_active whenever user interacts with the bot"""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {"last_active": datetime.now(pytz.utc).isoformat()}

    async with httpx.AsyncClient(timeout=8.0) as client:
        try:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/user_profiles?chat_id=eq.{chat_id}",
                headers=headers,
                json=payload
            )
        except:
            pass   # silent fail

# ==================== XP HISTORY LOGGING ====================
async def log_xp_history(chat_id: int, first_name: str, action: str,
                         xp_earned: int, previous_xp: int, new_xp: int,
                         previous_level: int, new_level: int):
    """Log every XP earning to xp_history table - completely separate from main flow"""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    leveled_up = new_level > previous_level

    payload = {
        "chat_id": chat_id,
        "first_name": first_name,
        "action": action,
        "xp_earned": xp_earned,
        "previous_xp": previous_xp,
        "new_xp": new_xp,
        "previous_level": previous_level,
        "new_level": new_level,
        "leveled_up": leveled_up
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        try:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/xp_history",
                headers=headers,
                json=payload
            )
        except Exception as e:
            print(f"⚠️ Failed to log XP history: {e}")

def get_level_title(level):
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
        10: "🌟 Eternal Guardian"
    }
    return titles.get(level, f"🌟 Legend {level}")

def create_progress_bar(current_xp: int, required_xp: int, length: int = 12) -> str:
    """Create a green forest-themed progress bar"""
    if required_xp <= 0:
        return "🟩" * length

    percentage = min(current_xp / required_xp, 1.0)
    filled = int(percentage * length)

    bar = "🟩" * filled + "⬜" * (length - filled)
    percent_text = f"{int(percentage * 100)}%"

    return f"[{bar}] {percent_text}"

# ==================== MESSAGES ====================
async def send_initial_welcome(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, {html.escape(str(first_name))}!\n\n"
        "🌿 Welcome to Clyde's Forest Hub.\n\n"
        "A calm place to find Windows keys, Office keys, Netflix cookies, PrimeVideo cookies, Steam accounts, and useful guides.\n\n"
        "<i>Tap the button below to enter the main menu.</i> 🍃"
    )

    msg = await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())
    if chat_id not in forest_memory: forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

async def send_level_up_message(chat_id, first_name, old_level, new_level):
    """Send a beautiful level up celebration message"""
    title = get_level_title(new_level)

    caption = (
        f"🌟 <b>Congratulations, {html.escape(first_name)}!</b>\n\n"
        f"You reached Level <b>{old_level}</b> → <b>{new_level}</b>\n\n"
        f"New title: <b>{title}</b>\n\n"
        "More resources are now unlocked.\n\n"
        "<i>Great work!</i> 🍃"
    )

    try:
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOADING_GIF,
            caption=caption,
            parse_mode='HTML'
        )
    except:
        pass

async def send_full_menu(chat_id, first_name, is_first_time=False):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    # Get user profile to show current level and title
    profile = await get_user_profile(chat_id)

    if profile:
        level = profile.get('level', 1)
        title = get_level_title(level)
        level_info = f"🏷️ {title}  •  ⭐ Level {level}"
    else:
        level_info = "🌱 New Wanderer  •  ⭐ Level 1"

    if is_first_time:
        caption = (
            f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
            "🌿 <b>Welcome to Clyde's Forest Hub</b>\n\n"
            f"{level_info}\n\n"
            "Browse the forest paths below.\n\n"
            "🌱 New here? Start with <b>❓ Guidance</b> — it will help you get started.\n\n"
            "<i>Enjoy your time in the hub.</i> 🍃"
        )
        keyboard = get_first_time_menu_keyboard()
    else:
        caption = (
                f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
                "🌿 <b>Welcome back to Clyde's Forest Hub</b>\n\n"
                f"{level_info}\n\n"
                "Choose what you'd like to do today.\n\n"
                "<i>Have a peaceful stay.</i> 🍃"
        )
        keyboard = get_full_menu_keyboard()

    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=MENU_GIF,
        caption=caption,
        parse_mode='HTML',
        reply_markup=keyboard
    )

    if chat_id not in forest_memory:
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

async def send_myid(chat_id):
    caption_text = (
        "🌿 <b>Your User ID</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>Your ID:</b> <code>{chat_id}</code>\n\n"
        "This is your unique user ID in the Forest Hub.\n"
        "You may need it for support or future features.\n\n"
        "<i>Keep it safe.</i> 🍃"
    )
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=MYID_GIF,
        caption=caption_text,
        parse_mode="HTML"
    )
    if chat_id not in forest_memory:
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

# ==================== HISTORY LOGS ====================
async def handle_history(chat_id: int, first_name: str, page: int = 0):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    limit = 8
    offset = page * limit

    profile = await get_user_profile(chat_id)
    current_level = profile.get('level', 1) if profile else 1
    total_xp = profile.get('total_xp_earned', 0) if profile else 0
    title = get_level_title(current_level)

    # ====================== Manila "Today" for XP Today ======================
    manila_tz = pytz.timezone('Asia/Manila')
    today_manila = datetime.now(manila_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_manila.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Default values
    xp_today = 0
    streak = 0
    top_action_name = "None yet"
    top_action_count = 0
    logs = []
    total_entries = 0

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            # Total entries for pagination
            count_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/xp_history?chat_id=eq.{chat_id}&select=id",
                headers=headers
            )
            total_entries = len(count_resp.json()) if count_resp.json() else 0

            # Paginated logs
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/xp_history"
                f"?chat_id=eq.{chat_id}&order=created_at.desc&limit={limit}&offset={offset}",
                headers=headers
            )
            logs = resp.json() or []

            # Most Used
            all_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/xp_history"
                f"?chat_id=eq.{chat_id}&select=action&limit=1000",
                headers=headers
            )
            all_logs = all_resp.json() or []

            action_count = Counter(
                log.get('action')
                for log in all_logs
                if log.get('action') and str(log.get('action')).strip()
            )
            if action_count:
                top_action = action_count.most_common(1)[0]
                top_action_name = top_action[0].replace('_', ' ').title()
                top_action_count = top_action[1]

            # SAFE TODAY'S XP
            today_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/xp_history"
                f"?chat_id=eq.{chat_id}&created_at=gte.{today_start_utc}&select=xp_earned",
                headers=headers
            )
            if today_resp.status_code == 200:
                today_logs = today_resp.json() or []
                xp_today = sum(item.get('xp_earned', 0) for item in today_logs)

            # ====================== REAL STREAK (Realtime) ======================
            streak = await calculate_streak(chat_id, client, headers)

        except Exception as e:
            print(f"History fetch error: {e}")
            # All defaults already set above

    # No logs yet
    if not logs and page == 0:
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text="🌱 No steps recorded yet.\nStart using the hub to earn XP!"
        )
        return

    # Streak message
    streak_text = f"You're on a {streak}-day XP streak! 🔥" if streak >= 2 else "Welcome to your journey! 🌱"

    lines = [
        f"🌟 <b>{html.escape(first_name)}'s XP Journey</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🏷️ <b>Current Title:</b> {title} • Level {current_level}",
        f"✨ <b>Total XP Earned:</b> {total_xp:,}",
        f"🌞 <b>XP Today:</b> {xp_today:,}",
        f"🔥 <b>Streak:</b> {streak_text}",
        f"🏆 <b>Most Used:</b> {top_action_name} ({top_action_count} times)",
        "━━━━━━━━━━━━━━━━━━"
    ]

    for log in logs:
        action_name = log.get('action', 'Unknown').replace('_', ' ').title()

        try:
            dt = datetime.fromisoformat(log['created_at'].replace('Z', '+00:00'))
            dt = dt.astimezone(pytz.timezone('Asia/Manila'))
            time_str = dt.strftime("%Y-%m-%d %H:%M")
        except:
            time_str = str(log.get('created_at', ''))[:16]

        main_line = f" {action_name} → +{log.get('xp_earned', 0)} XP"
        if log.get('leveled_up') and log.get('new_level', 1) > 1:
            main_line += f" → <b>Level {log.get('new_level')} 🎉</b>"

        lines.append(f"🕒 {time_str}")
        lines.append(main_line)
        lines.append(f" {log.get('previous_xp', 0)} → {log.get('new_xp', 0)} XP")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━")
    total_pages = (total_entries + limit - 1) // limit
    lines.append(f"🌱 Page {page + 1} of {total_pages} • Keep exploring to grow more.")

    text = "\n".join(lines)

    # Pagination
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"history_page_{page-1}"))
    if (page + 1) * limit < total_entries:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"history_page_{page+1}"))

    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

    await tg_app.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode='HTML',
        reply_markup=keyboard
    )

async def calculate_streak(chat_id: int, client, headers):
    """Calculate current consecutive days with XP"""
    try:
        # Get last 30 days of history to be safe
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/xp_history"
            f"?chat_id=eq.{chat_id}&select=created_at&order=created_at.desc&limit=100",
            headers=headers
        )
        logs = resp.json() or []

        if not logs:
            return 0

        manila_tz = pytz.timezone('Asia/Manila')
        today = datetime.now(manila_tz).date()

        streak = 0
        seen_dates = set()

        for log in logs:
            try:
                dt = datetime.fromisoformat(log['created_at'].replace('Z', '+00:00'))
                dt_manila = dt.astimezone(manila_tz)
                log_date = dt_manila.date()

                if log_date not in seen_dates:
                    seen_dates.add(log_date)
                    # Check if it's consecutive from today
                    if (today - log_date).days == streak:
                        streak += 1
                    else:
                        break  # streak broken
            except:
                continue

        return streak
    except Exception as e:
        print(f"Streak calculation error: {e}")
        return 0

# ==================== PROFILE COMMAND ======================
async def handle_profile(chat_id, first_name):
    await add_xp(chat_id, first_name, "profile")

    profile = await get_user_profile(chat_id)
    if not profile:
        return

    level = profile['level']
    xp = profile['xp']
    xp_required_next = get_cumulative_xp_for_level(level + 1)
    xp_to_next = max(0, xp_required_next - xp)

    # Create green progress bar
    progress_bar = create_progress_bar(xp, xp_required_next, length=10)

    caption = (
        f"🌿 <b>{html.escape(first_name)}'s Profile</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ <b>Title:</b> {get_level_title(level)}\n"
        f"⭐ <b>Level:</b> {level}\n\n"
        f"✨ <b>XP:</b> {xp:,} / {xp_required_next:,} XP\n"
        f"{progress_bar}\n\n"
        f"📈 To next level: <b>{xp_to_next:,}</b> XP\n\n"
        "<i>Keep using the hub to level up and unlock more resources.</i> 🍃"
    )

    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=MYID_GIF,
        caption=caption,
        parse_mode='HTML'
    )

    # Add to forest_memory so /clear can delete it
    if chat_id not in forest_memory:
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)


async def handle_stats(chat_id, first_name):
    """Updated Stats with individual inventory tracking"""

    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text="🌿 You haven't started your journey yet. Use /profile to begin!"
        )
        return

    level = profile.get('level', 1)
    xp = profile.get('xp', 0)
    xp_required_next = get_cumulative_xp_for_level(level + 1)

    progress_bar = create_progress_bar(xp, xp_required_next, length=10)

    # Date formatting
    joined_date = "Unknown"
    if profile.get('created_at'):
        try:
            dt = datetime.fromisoformat(profile['created_at'].replace('Z', '+00:00'))
            joined_date = dt.strftime("%B %d, %Y")
        except:
            joined_date = str(profile['created_at'])[:10]

    # Improved Last Active display (minimal change)
    last_active = "Just now"
    if profile.get('last_active'):
        try:
            dt = datetime.fromisoformat(profile['last_active'].replace('Z', '+00:00'))
            dt = dt.astimezone(pytz.timezone('Asia/Manila'))
            now_manila = datetime.now(pytz.timezone('Asia/Manila'))

            diff_minutes = int((now_manila - dt).total_seconds() / 60)

            if diff_minutes < 2:
                last_active = "Just now"
            elif diff_minutes < 60:
                last_active = f"{diff_minutes} minutes ago"
            else:
                last_active = dt.strftime("%B %d, %Y • %I:%M %p")
        except:
            last_active = "Just now"

    # New individual stats
    windows_views = profile.get('windows_views', 0)
    office_views = profile.get('office_views', 0)
    netflix_views = profile.get('netflix_views', 0)
    netflix_reveals = profile.get('netflix_reveals', 0)
    times_cleared = profile.get('times_cleared', 0)
    guidance_reads = profile.get('guidance_reads', 0)
    lore_reads = profile.get('lore_reads', 0)
    profile_views = profile.get('profile_views', 0)

    caption = (
        f"🌲 <b>{html.escape(first_name)}'s Statistics</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ <b>Title:</b> {get_level_title(level)}\n"
        f"⭐ <b>Level:</b> {level}\n\n"
        f"✨ <b>XP:</b> {xp:,} / {xp_required_next:,}\n"
        f"{progress_bar}\n\n"
        "📊 <b>Activity:</b>\n"
        f"• Total XP Earned: <b>{profile.get('total_xp_earned', xp):,}</b>\n"
        f"• Profile Views: <b>{profile_views}</b>\n"
        f"• Windows Keys Viewed: <b>{windows_views}</b>\n"
        f"• Office Keys Viewed: <b>{office_views}</b>\n"
        f"• Netflix Keys Viewed: <b>{netflix_views}</b>\n"
        f"• Netflix Cookies Revealed: <b>{netflix_reveals}</b>\n"
        f"• Chat Cleared: <b>{times_cleared}</b>\n"
        f"• Guidance Opened: <b>{guidance_reads}</b>\n"
        f"• Lore Read: <b>{lore_reads}</b>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🌱 Joined: {joined_date}\n"
        f"🌲 Last Active: {last_active}\n\n"
        "<i>Keep exploring the hub to grow your stats.</i> 🍃"
    )

    msg = await tg_app.bot.send_message(
        chat_id=chat_id,
        text=caption,
        parse_mode='HTML'
    )

    if chat_id not in forest_memory:
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

# ==================== LEADERBOARD COMMAND ======================
async def handle_leaderboard(chat_id):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    OWNER_CHAT_ID = 7399488750
    MIN_LEVEL_TO_UNLOCK = 3

    async with httpx.AsyncClient(timeout=12.0) as client:
        try:
            user_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_profiles"
                f"?chat_id=eq.{chat_id}&select=first_name,xp,level",
                headers=headers
            )
            user = user_resp.json()[0] if user_resp.json() else None

            if chat_id != OWNER_CHAT_ID:
                if not user or user.get('level', 1) < MIN_LEVEL_TO_UNLOCK:
                    current_level = user.get('level', 1) if user else 1
                    await tg_app.bot.send_message(
                        chat_id=chat_id,
                        text=f"🏆 <b>Top Users</b>\n\n"
                            f"You need to reach <b>Level {MIN_LEVEL_TO_UNLOCK}</b> to see the leaderboard.\n\n"
                            f"You are currently <b>Level {current_level}</b>.\n\n"
                            "Keep using the hub to level up.",
                        parse_mode='HTML'
                    )
                    return

            top_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_profiles"
                f"?select=first_name,xp,level,chat_id"
                f"&xp=gt.0"
                f"&order=xp.desc&limit=10",
                headers=headers
            )
            top_data = top_resp.json() or []

            if not top_data:
                await tg_app.bot.send_message(
                    chat_id=chat_id,
                    text="🏆 <b>Top Users</b>\n\n"
                        "No one has earned XP yet.\n"
                        "Be the first to start exploring the hub! 🌱",
                    parse_mode='HTML'
                )
                return

            text = "🏆 <b>Top Users</b>\n━━━━━━━━━━━━━━━━━━\n\n"

            for rank, u in enumerate(top_data, 1):
                name = html.escape(u.get('first_name', 'Unknown Wanderer'))
                xp = u.get('xp', 0)
                level = u.get('level', 1)
                title = get_level_title(level)
                medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"{rank}."

                if str(u.get('chat_id')) == str(OWNER_CHAT_ID):
                    name = "The Forest Warden"

                text += f"{medal} <b>{name}</b>\n"
                text += f"   {title} • Level {level}\n"
                text += f"   ✨ {xp:,} XP\n\n"

            # Clean rank display for owner
            if user and user.get('xp', 0) > 0:
                text += "━━━━━━━━━━━━━━━━━━\n"

                rank_resp = await client.get(
                    f"{SUPABASE_URL}/rest/v1/user_leaderboard"
                    f"?chat_id=eq.{chat_id}&select=rank",
                    headers=headers
                )
                rank_data = rank_resp.json()
                real_rank = rank_data[0].get('rank', 1) if rank_data and len(rank_data) > 0 else 1

                if str(chat_id) == str(OWNER_CHAT_ID):
                    text += f"📍 <b>You are The Forest Warden</b> • Currently ranked <b>#{real_rank}</b>\n"
                    text += f"   {get_level_title(user.get('level', 1))} • Level {user.get('level', 1)}\n"
                    text += f"   ✨ {user.get('xp', 0):,} XP\n"
                else:
                    text += f"📍 <b>You are currently ranked #{real_rank}</b>\n"
                    text += f"   {get_level_title(user.get('level', 1))} • Level {user.get('level', 1)}\n"
                    text += f"   ✨ {user.get('xp', 0):,} XP\n"

            text += "\n<i>Keep exploring the hub.</i> 🍃"

            await tg_app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode='HTML'
            )

        except Exception as e:
            print(f"🔴 Leaderboard error: {e}")
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text="🌫️ The ancient trees are having trouble reading the winds right now..."
            )

# ==================== FEEDBACK COMMAND ======================
async def handle_feedback(chat_id, first_name, feedback_text):
    # Get current time in Philippines timezone for display
    user_tz = pytz.timezone('Asia/Manila')
    timestamp = datetime.now(user_tz).strftime("%B %d, %Y • %I:%M %p")

    # === Save Feedback to Supabase ===
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    payload = {
        "chat_id": int(chat_id),
        "first_name": str(first_name),
        "feedback_text": feedback_text.strip()
    }

    saved = False
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(
                f"{SUPABASE_URL}/rest/v1/feedback",
                headers=headers,
                json=payload
            )

            if response.status_code in (200, 201):
                print(f"✅ Feedback saved to Supabase | User: {chat_id}")
                saved = True
            else:
                print(f"⚠️ Supabase insert failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"🔴 Supabase Error while saving feedback: {e}")

    # === Thank you message to user (timestamp at bottom) ===
    thank_you = (
        "🕊️ <b>Feedback Received</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Thanks, <b>{html.escape(str(first_name))}</b>!\n\n"
        "Your message has been sent to the owner.\n"
        "We really appreciate your feedback.\n\n"
        f"🕒 Sent: {timestamp}"
    )

    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=HELP_GIF,
        caption=thank_you,
        parse_mode='HTML'
    )

    # === Notification to owner (timestamp at bottom) ===
    status = "✅ Saved to database" if saved else "⚠️ Failed to save to database"

    owner_message = (
        f"🌿 <b>New Feedback Received from the Forest</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 From: <b>{first_name}</b>\n"
        f"🆔 User ID: <code>{chat_id}</code>\n\n"
        f"💬 <b>Message:</b>\n{feedback_text}\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🕒 <b>Received:</b> {timestamp}\n"
        f"💾 <b>Database:</b> {status}"
    )

    try:
        await tg_app.bot.send_message(
            # chat_id=1234567890,
            chat_id=7399488750,
            text=owner_message,
            parse_mode='HTML'
        )
    except Exception as e:
        print(f"Failed to send feedback to owner: {e}")

# ==================== VIEW FEEDBACK COMMAND (Owner Only) ======================
async def handle_view_feedback(chat_id, user_id):
    # Security: Only you (the owner) can use this command
    # if chat_id != 1234567890:   # Your owner chat_id
    if chat_id != 7399488750:   # Your owner chat_id

        await tg_app.bot.send_message(
            chat_id=chat_id,
            text="🌿 Sorry, only the caretaker of the forest can view the feedback scrolls."
        )
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/feedback?select=*&order=created_at.desc&limit=15",
                headers=headers
            )

            if response.status_code != 200:
                await tg_app.bot.send_message(chat_id=chat_id, text="❌ Failed to fetch feedback from the database.")
                return

            data = response.json()

            if not data:
                await tg_app.bot.send_message(
                    chat_id=chat_id,
                    text="🌿 The feedback scroll is currently empty. No messages yet."
                )
                return

            # Build beautiful message
            message = "🌿 <b>Recent Feedback from the Forest</b>\n━━━━━━━━━━━━━━━━━━\n\n"

            for idx, item in enumerate(data, 1):
                created_at = item.get('created_at', '')
                # Convert ISO timestamp to readable format (Philippines time)
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    dt = dt.astimezone(pytz.timezone('Asia/Manila'))
                    time_str = dt.strftime("%b %d, %Y • %I:%M %p")
                except:
                    time_str = created_at[:16]  # fallback

                first_name = html.escape(str(item.get('first_name') or 'Unknown'))
                feedback = html.escape(str(item.get('feedback_text') or '').strip())

                message += (
                    f"✨ <b>{idx}.</b> From <b>{first_name}</b>\n"
                    f"🆔 <code>{item.get('chat_id')}</code>\n"
                    f"🕒 {time_str}\n\n"
                    f"💬 {feedback}\n"
                    "━━━━━━━━━━━━━━━━━━\n\n"
                )

            # If too long, Telegram has limit (~4096 chars), but 15 feedbacks should be fine
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='HTML'
            )

        except Exception as e:
            print(f"🔴 Error fetching feedbacks: {e}")
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text="⚠️ Something went wrong while reading the feedback scrolls."
            )

# ==================== RESET FIRST-TIME EXPERIENCE (Owner Only) ======================
async def handle_reset_first_time(chat_id):
    if chat_id != 7399488750:
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text="🌿 Sorry, only the caretaker can reset the forest memory."
        )
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    payload = {"has_seen_menu": False}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/user_profiles?chat_id=eq.{chat_id}",
                headers=headers,
                json=payload
            )
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text="✨ <b>First-time experience has been reset.</b>\n\n"
                     "The next time you enter the Enchanted Clearing, the guided menu "
                     "with <b>『 Start Here → Guidance 』</b> will appear again.",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"Reset failed: {e}")
            await tg_app.bot.send_message(chat_id=chat_id, text="❌ Failed to reset first-time flag.")

    print(f"✅ First-time flag reset for owner {chat_id}")

# --- CLEAN CLEAR FUNCTION (No leftover "renewed" message) ---
async def handle_clear(chat_id, user_command_id, first_name):
    # Delete the user's /clear command
    try:
        await tg_app.bot.delete_message(chat_id=chat_id, message_id=user_command_id)
    except:
        pass

    # Clear ALL previous bot messages
    if chat_id in forest_memory:
        for msg_id in forest_memory.get(chat_id, []):
            try:
                await tg_app.bot.delete_message(chat_id, msg_id)
            except:
                pass  # Message already deleted or doesn't exist
        del forest_memory[chat_id]  # Remove entirely instead of leaving empty list

    # ====================== MAGICAL CLEARING ANIMATION ======================
    loading_msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=CLEAN_GIF,
        caption="🌿 Cleaning up old messages...",
        parse_mode="HTML"
    )

    await asyncio.sleep(1.5)
    await loading_msg.edit_caption("🌿 Loading the main menu...", parse_mode="HTML")
    await asyncio.sleep(1.0)

    # Delete the loading animation
    try:
        await tg_app.bot.delete_message(chat_id, loading_msg.message_id)
    except:
        pass

    # Directly show the main menu (no extra button or old message)
    await send_full_menu(chat_id, first_name, is_first_time=False)

    # Give XP for using /clear
    await add_xp(chat_id, first_name, "clear")

    print(f"🌿 Chat cleared for user {chat_id}")

# ==================== BOT INFO / STATUS COMMAND ======================
async def handle_info(chat_id):
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }

        async with httpx.AsyncClient(timeout=12.0) as client:
            # Total Wanderers
            total_res = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_profiles?select=chat_id",
                headers=headers
            )
            total_users = len(total_res.json()) if total_res.status_code == 200 else 0

            # Active Today - Instant
            manila_tz = pytz.timezone('Asia/Manila')
            today_start = datetime.now(manila_tz).replace(hour=0, minute=0, second=0, microsecond=0)
            today_utc = today_start.astimezone(pytz.utc).isoformat()

            active_res = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_profiles?select=chat_id",
                headers=headers,
                params={"last_active": f"gte.{today_utc}"}
            )
            active_today = len(active_res.json()) if active_res.status_code == 200 else 0

        version = "1.3.1"
        last_updated = "April 7, 2026"
        uptime_delta = datetime.now(pytz.utc) - BOT_START_TIME
        total_seconds = int(uptime_delta.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        uptime = f"{days}d {hours}h {minutes}m"

        text = (
            "🌿 <b>Forest Hub Status</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "The hub is running smoothly.\n\n"
            f"🕒 Uptime: <b>{uptime}</b>\n"
            f"🌱 Total Users: <b>{total_users:,}</b>\n"
            f"✨ Active Today: <b>{active_today:,}</b>\n\n"
            f"📜 Version: <b>{version}</b>\n"
            f"🌲 Last Updated: <b>{last_updated}</b>\n\n"
            "⚠️ For personal and educational use only.\n\n"
            "Made with care 🍃"
        )

        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode='HTML'
        )

    except Exception as e:
        print(f"Info command error: {str(e)}")
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text="🌿 Something went wrong while fetching the status."
        )

# ==================== CALLBACK ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    first_name = update.effective_user.first_name if update.effective_user else "Wanderer"

    # ====================== MAIN MENU ======================
    if query.data in ["show_main_menu", "main_menu"]:
        try:
            await query.message.delete()
        except:
            pass

        await asyncio.sleep(0.8)

        # Loading animation
        loading_msg = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOADING_GIF,
            caption="🌿 <i>Loading the menu...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.0)
        await loading_msg.edit_caption("🌿 Preparing options...", parse_mode='HTML')
        await asyncio.sleep(0.8)
        await loading_msg.edit_caption("🌿 Welcome back!", parse_mode='HTML')
        await asyncio.sleep(0.6)

        # Get or create profile
        profile = await get_user_profile(chat_id)
        if not profile:
            await add_xp(chat_id, first_name, "general")
            profile = await get_user_profile(chat_id)

            if profile:
                await update_last_active(chat_id)

        is_first_time = not bool(profile.get('has_seen_menu', False)) if profile else True

        try:
            await tg_app.bot.delete_message(loading_msg.chat_id, loading_msg.message_id)
        except:
            pass

        await send_full_menu(chat_id, first_name, is_first_time=is_first_time)
        return   # ← Stop here for main menu

    # ====================== ALL OTHER BUTTONS ======================
    # Enforce registration
    profile = await get_user_profile(chat_id)
    if not profile:
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=HELLO_GIF,
            caption="🌿 <b>Welcome to the Forest Hub</b>\n\n"
                    "To start using the resources, please tap the button below.",
            parse_mode='HTML',
            reply_markup=get_start_keyboard()
        )
        return

    await update_last_active(chat_id)

    # ====================== MAIN INVENTORY MENU ======================
    if query.data == "check_vamt":
        # Delete current message to avoid "no caption" error
        try:
            await query.message.delete()
        except:
            pass

        # Send fresh inventory menu with GIF
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=INVENTORY_GIF,   # Your original inventory GIF
            caption="📜 <b>Resource Inventory</b>\n\n"
                    "Choose the type of resource you need today:\n\n"
                    "<i>Viewing items will earn you XP and help you level up.</i>",
            parse_mode='HTML',
            reply_markup=get_inventory_categories()
        )


    # ====================== FILTERED INVENTORY ======================
    elif query.data.startswith("vamt_filter_"):
        category = query.data.replace("vamt_filter_", "").lower()

        # Give XP only for currently working categories
        if category in ["win", "windows"]:
            await add_xp(chat_id, first_name, "view_windows", query=query)
        elif category == "office":
            await add_xp(chat_id, first_name, "view_office", query=query)
        elif category == "netflix":
            await add_xp(chat_id, first_name, "view_netflix", query=query)
        # Prime and Steam get no XP for now

        await query.message.edit_caption(
            caption=f"✨ <i>Searching the glade for {category.upper()}...</i>",
            parse_mode='HTML'
        )

        # ====================== COMING SOON - PRIME & STEAM ======================
        if category in ["prime", "steam"]:
            if category == "prime":
                msg = (
                    "🎥 <b>PrimeVideo Premium Cookies</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n\n"
                    "PrimeVideo premium cookies are currently being prepared.\n\n"
                    "They will be available soon for higher-level users.\n\n"
                    "<i>Stay tuned — we’ll notify you when they are ready.</i>"
                )
            else:  # steam
                msg = (
                    "🎮 <b>Steam Accounts</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n\n"
                    "Steam accounts are planned to drop daily at 8:00 PM.\n\n"
                    "Higher-level users will get early access before the public drop.\n\n"
                    "<i>Coming soon. Keep leveling up to unlock early access.</i>"
                )

            await query.message.edit_caption(
                caption=msg,
                parse_mode='HTML',
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        # ====================== WORKING CATEGORIES (Windows, Office, Netflix) ======================
        profile = await get_user_profile(chat_id)
        user_level = profile.get('level', 1) if profile else 1

        data = await get_vamt_data()
        if not data:
            await query.message.edit_caption(
                caption="🌫️ <i>The mist is too thick... Database connection failed.</i>",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        filtered = []
        for item in data:
            s_type = str(item.get('service_type', '')).lower().strip()
            if category in ["win", "windows"] and any(x in s_type for x in ["windows", "win"]):
                filtered.append(item)
            elif category == "office" and "office" in s_type:
                filtered.append(item)
            elif category == "netflix" and "netflix" in s_type:
                filtered.append(item)

        if not filtered:
            await query.message.edit_caption(
                caption=f"No {category.upper()} items available right now.",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        # Old limit logic (kept unchanged)
        if user_level == 1:
            limit = 1
            limit_note = "🌱 As a new wanderer, you can only see 1 item for now..."
        elif user_level <= 3:
            limit = 2
            limit_note = f"🌿 At Level {user_level}, you can see up to 2 items."
        elif user_level <= 6:
            limit = 4 if user_level <= 5 else 5
            limit_note = f"🌿 At Level {user_level}, you can see up to {limit} items."
        else:
            limit = len(filtered)
            limit_note = "✨ You have full access to all scrolls in the forest."

        # Sort for consistency
        filtered.sort(key=lambda x: (str(x.get('service_type', '')), str(x.get('key_id', ''))))

        # Netflix handling
        if category == "netflix":
            report = (
                "<b>🍿 Secret Netflix Cookies of the Forest</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>{len(filtered)} Cookies Resting in the Glade</b>\n"
                f"{limit_note}\n\n"
                "<i>Select a cookie to reveal it.</i>\n\n"
            )
            buttons = []
            for display_idx, item in enumerate(filtered[:limit], 1):
                display_name = f"Netflix Cookie {display_idx}"
                status_text = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"
                report += f"✨ <b>{display_name}</b>\n"
                report += f" Status: {status_text}\n"
                report += f" Remaining: {item.get('remaining', 0)}\n\n"
                buttons.append([
                    InlineKeyboardButton(f"🔓 Reveal {display_name}", callback_data=f"reveal_nf|{display_idx}")
                ])
            buttons.append([InlineKeyboardButton("⬅️ Back to the Hub", callback_data="check_vamt")])
            kb = InlineKeyboardMarkup(buttons)
            await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)
            return

        # Windows & Office handling
        report = f"<b>📜 {category.upper()} Scrolls</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for item in filtered[:limit]:
            product = item.get('service_type', 'Unknown')
            key = item.get('key_id', 'HIDDEN')
            raw_val = int(item.get('remaining') or 0)
            stock_text = f"{raw_val}" if raw_val > 0 else "Out of stock"
            report += f"✨ <b>{product}</b>\n└ 🔑 <code>{key}</code>\n└ 📦 Stock: <b>{stock_text}</b>\n\n"

        if limit < len(filtered):
            report += f"━━━━━━━━━━━━━━━━━━\n<i>Level up to see more scrolls hidden in the shadows...</i>"

        kb = get_back_to_inventory_keyboard()
        await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)

    # ====================== HISTORY PAGINATION ======================
    elif query.data.startswith("history_page_"):
        try:
            page = int(query.data.split("_")[2])
            await query.message.delete()   # Delete old message
        except:
            page = 0

        await handle_history(chat_id, first_name, page=page)
        return

    # ====================== REVEAL NETFLIX ======================
    elif query.data.startswith("reveal_nf|"):
        try:
            idx = int(query.data.split("|", 1)[1])
        except:
            await query.answer("Invalid selection", show_alert=True)
            return

        # Loading
        await query.message.edit_caption(
            caption="🍿 <i>Searching for the cookie...</i>",
            parse_mode='HTML'
        )
        await asyncio.sleep(1.3)
        await query.message.edit_caption(
            caption="🌟 <i>Revealing the cookie...</i>\n\n",
            parse_mode='HTML'
        )
        await asyncio.sleep(1.5)

        # Get data (same as before)
        profile = await get_user_profile(chat_id)
        user_level = profile.get('level', 1)

        if user_level == 1: limit = 1
        elif user_level <= 3: limit = 2
        elif user_level <= 6: limit = 4 if user_level <= 5 else 5
        else: limit = 999

        data = await get_vamt_data()
        if not data:
            await query.answer("Database error", show_alert=True)
            return

        filtered = [item for item in data if "netflix" in str(item.get('service_type', '')).lower()]
        filtered.sort(key=lambda x: (str(x.get('display_name') or ''), str(x.get('last_updated') or '')))

        if idx < 1 or idx > len(filtered[:limit]):
            await query.answer("❌ Cookie not found", show_alert=True)
            return

        item = filtered[idx - 1]

        cookie = str(item.get('key_id', '')).strip()
        display_name = str(item.get('display_name') or '').strip() or f"Forest Cookie {idx}"
        status = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"

        await add_xp(chat_id, first_name, "reveal_netflix", query=query)

        try:
            await query.message.delete()
        except:
            pass

        # Use HTML instead of MarkdownV2 — much more forgiving with dynamic text
        caption = (
            f"📄 <b>Netflix_Cookie_{idx}.txt</b>\n\n"
            f"🍿 <b>{html.escape(display_name)} Revealed</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"🌿 Status: <b>{html.escape(status)}</b>\n"
            f"📦 Remaining: <b>{item.get('remaining', 0)}</b>\n\n"
            "📥 The forest has wrapped your cookie in an ancient scroll.\n"
            "<i>Tap the file below to receive its magic.</i> 🍃"
        )

        # Immersive file content (plain text is safest)
        from io import BytesIO
        file_content = f"""🌿🍃 Clyde's Forest Hub — Secret Netflix Cookie 🌿🍃

══════════════════════════════════════════════════════════════

🌳 Cookie Spirit Awakened
Name        : {display_name}
Status      : {status}
Remaining   : {item.get('remaining', 0)} uses
Revealed on : {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}

🌲 The ancient trees have entrusted you with this cookie.
Guard it well, wanderer, for its magic fades quickly.

══════════════════════════════════════════════════════════════

{cookie}

══════════════════════════════════════════════════════════════

🍃 May this cookie bring you peaceful streams and hidden stories.
Use it wisely and with gratitude.

— The Caretaker of the Forest Clear Hub 🌿
"""

        file_bytes = BytesIO(file_content.encode('utf-8'))
        file_bytes.name = f"Netflix_Cookie_{idx}.txt"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Netflix Cookies", callback_data="back_to_netflix_list")]
        ])

        # Send document with HTML (much safer)
        doc_msg = await tg_app.bot.send_document(
            chat_id=chat_id,
            document=file_bytes,
            caption=caption,
            parse_mode='HTML',
            reply_markup=kb,
            filename=file_bytes.name
        )

        # Track BOTH messages for /clear
        if chat_id not in forest_memory:
            forest_memory[chat_id] = []
        forest_memory[chat_id].append(doc_msg.message_id)

        # Success message
        await asyncio.sleep(0.8)
        success_msg = (
            "✨ <b>Cookie successfully delivered!</b>\n\n"
            "🌿 The ancient scroll has been handed to you.\n"
            "May its magic serve you well, wanderer."
        )
        success_sent = await tg_app.bot.send_message(
            chat_id=chat_id,
            text=success_msg,
            parse_mode='HTML'
        )
        forest_memory[chat_id].append(success_sent.message_id)

    # ====================== ABOUT (Lore) ======================
    elif query.data == "about":
        await add_xp(chat_id, first_name, "lore", query=query)

        try:
            await query.message.delete()
        except:
            pass

        loading_msg = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOADING_GIF,
            caption="🌌 <i>The oldest spirits of the forest begin to stir...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.2)
        await loading_msg.edit_caption("📜 <i>They gather beneath the ancient canopy to share forgotten tales...</i>", parse_mode='HTML')
        await asyncio.sleep(1.3)
        await loading_msg.edit_caption("✨ <i>The story of this sacred clearing gently unfolds...</i>", parse_mode='HTML')
        await asyncio.sleep(1.0)

        text = (
            "<b>🌿 About Clyde's Forest Hub</b>\n\n"
            "This peaceful sanctuary was created to make useful digital resources easy and stress-free to access all wrapped in a calm, Studio Ghibli-inspired forest theme.\n\n"
            "You can find:\n"
            "• Windows & Office activation keys\n"
            "• Netflix premium cookies\n"
            "• PrimeVideo premium cookies\n"
            "• Steam accounts\n"
            "• Learning guides\n\n"
            "A simple leveling system rewards you for using it.\n\n"
            "<i>Hope you find it helpful and relaxing.</i> 🍃"
        )

        final_msg = await tg_app.bot.send_animation(
            chat_id=chat_id,                                   # ← Fixed
            animation=ABOUT_GIF,
            caption=text,
            parse_mode='HTML',
            reply_markup=get_back_keyboard()
        )

        try:
            await tg_app.bot.delete_message(loading_msg.chat_id, loading_msg.message_id)
        except:
            pass

        if chat_id not in forest_memory:
            forest_memory[chat_id] = []
        forest_memory[chat_id].append(final_msg.message_id)


    # ====================== BACK TO NETFLIX LIST ======================
    elif query.data == "back_to_netflix_list":
        # Delete the .txt file message
        try:
            await query.message.delete()
        except:
            pass

        # Show fresh Netflix list WITH GIF
        profile = await get_user_profile(chat_id)
        user_level = profile.get('level', 1)

        data = await get_vamt_data()
        if not data:
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text="🌫️ The mist is too thick... Database connection failed.",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        filtered = [item for item in data if "netflix" in str(item.get('service_type', '')).lower()]
        filtered.sort(key=lambda x: (str(x.get('display_name') or ''), str(x.get('last_updated') or '')))

        if user_level == 1:
            limit = 1
            limit_note = "🌱 As a new wanderer, you can only see 1 item for now..."
        elif user_level <= 3:
            limit = 2
            limit_note = f"🌿 At Level {user_level}, you can see up to 2 items."
        elif user_level <= 6:
            limit = 4 if user_level <= 5 else 5
            limit_note = f"🌿 At Level {user_level}, you can see up to {limit} items."
        else:
            limit = len(filtered)
            limit_note = "✨ You have full access to all scrolls in the forest."

        report = (
            "<b>🍿 Secret Netflix Cookies of the Forest</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>{len(filtered)} Cookies Resting in the Glade</b>\n"
            f"{limit_note}\n\n"
            "<i>Select a cookie to reveal it.</i>\n\n"
        )

        buttons = []
        for display_idx, item in enumerate(filtered[:limit], 1):
            display_name = f"Netflix Cookie {display_idx}"
            status_text = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"
            report += f"✨ <b>{display_name}</b>\n   Status: {status_text}\n   Remaining: {item.get('remaining', 0)}\n\n"
            buttons.append([InlineKeyboardButton(f"🔓 Reveal {display_name}", callback_data=f"reveal_nf|{display_idx}")])

        buttons.append([InlineKeyboardButton("⬅️ Back to the Hub", callback_data="check_vamt")])
        kb = InlineKeyboardMarkup(buttons)

        # Send with GIF (this brings back the logo)
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=INVENTORY_GIF,           # ← Your logo GIF
            caption=report,
            parse_mode='HTML',
            reply_markup=kb
        )

    # ====================== HELP (Guidance) - 2 Pages ======================
    elif query.data == "help" or query.data.startswith("help_page_"):
        chat_id = update.effective_chat.id
        first_name = update.effective_user.first_name if update.effective_user else "Wanderer"

        # === Give XP only on the very first time opening Guidance ===
        if query.data == "help":
            await add_xp(chat_id, first_name, "guidance", query=query)

        try:
            await query.message.delete()
        except:
            pass

        page = 1
        if query.data.startswith("help_page_"):
            page = int(query.data.split("_")[2])

        # Loading animation
        loading_msg = await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOADING_GIF,
            caption="🪶 <i>The wind carries soft voices from the depths of the forest...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.2)
        await loading_msg.edit_caption("🌟 <i>The forest guides are preparing wisdom for you...</i>", parse_mode='HTML')
        await asyncio.sleep(1.0)

        if page == 1:
            text = (
                "<b>❓ Guidance - Page 1/2</b>\n\n"
                "🌿 <b>How to Use the Forest Hub</b>\n"
                "• Tap any button to explore\n"
                "• Type <b>/menu</b> to return to the main menu anytime\n"
                "• Type <b>/clear</b> to refresh the chat and remove old messages\n\n"

                "📜 <b>Available Commands</b>\n"
                "• /start — Restart the bot\n"
                "• /menu — Open the main menu\n"
                "• /profile — View your level and XP\n"
                "• /stats — Detailed statistics\n"
                "• /leaderboard — See top users\n"
                "• /myid — Show your user ID\n"
                "• /clear — Clean the chat\n"
                "• /feedback — Send message to the owner\n\n"

                "🌲 <b>Resources Available</b>\n"
                "• Spirit Treasures — Steam accounts\n"
                "• Ancient Scrolls — Learning guides\n"
                "• Forest Inventory — Windows, Office & Netflix keys\n"
                "• Whispering Forest — Main resource hub\n\n"

                "<b>New here?</b>\n"
                "You start at Level 1 with 0 XP.\n"
                "Use buttons and commands to earn XP and unlock more items.\n\n"

                "<i>Tap Next → to learn about the Leveling System</i>"
            )
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("Next →", callback_data="help_page_2")],
                [InlineKeyboardButton("⬅️ Back to Hub", callback_data="main_menu")]
            ])

        else:
            level_req_text = "\n".join(
                f"• Level {lvl} → {get_cumulative_xp_for_level(lvl):,} XP"
                for lvl in range(2, 11)
            )

            text = (
                "<b>❓ Guidance - Page 2/2</b>\n\n"
                "✨ <b>Leveling System</b>\n"
                "Earn XP by using the hub. Higher levels unlock more items in the inventory.\n\n"

                "<b>How to Gain XP:</b>\n"
                "• View Windows or Office keys → <b>+6 XP</b>\n"
                "• View Netflix keys → <b>+6 XP</b>\n"
                "• Reveal a Netflix cookie → <b>+10 XP</b>\n"
                "• Use /profile → <b>+5 XP</b>\n"
                "• Use /clear → <b>+5 XP</b>\n"
                "• Open Guidance or Lore → <b>+8 XP</b> (first time only)\n\n"

                "<b>Items Visible in Inventory:</b>\n\n"
                "<pre>"
                "Level     Win/Office     Netflix     Prime Video     Steam Accounts\n"
                "────────────────────────────────────────────────────────────────\n"
                " 1         1              1           1               Public Drop Only\n"
                " 2–3       2              2           1–2             Public Drop Only\n"
                " 4–5       3              2–3         2               Public Drop Only\n"
                " 6         5              4           3               Public Drop Only\n"
                " 7         Up to 6        Up to 5     Up to 3         Early Preview\n"
                " 8         Up to 8        Up to 7     Up to 4         Early Preview\n"
                " 9         Up to 10       Up to 8     Up to 5         Early Preview + Sunday Double\n"
                "10+        Up to All      Up to All   Up to All       🌟 Legend Tier\n"
                "</pre>\n\n"

                f"<b>XP Required:</b>\n{level_req_text}\n\n"

                "<i>The more you use the hub, the higher your level becomes.</i> 🍃"
            )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("← Previous", callback_data="help_page_1")],
            ])

        await loading_msg.edit_caption(
            caption=text,
            parse_mode='HTML',
            reply_markup=keyboard
        )

        # Save for /clear
        if chat_id not in forest_memory:
            forest_memory[chat_id] = []
        forest_memory[chat_id].append(loading_msg.message_id)

        # === Mark as seen ONLY when Guidance is actually opened (Softer Version) ===
        if query.data == "help":   # Only when first opening Guidance
            profile = await get_user_profile(chat_id)
            if profile and not profile.get('has_seen_menu', False):
                await update_has_seen_menu(chat_id)

# ==================== WEBHOOK ====================
async def start_tg_app():
    await tg_app.initialize()
    await tg_app.start()

loop.run_until_complete(start_tg_app())

@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🌿 Clyde's Enchanted Clearing is awake.", 200

    update_data = request.get_json(silent=True)
    if not update_data:
        return "No data", 400

@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🌿 Clyde's Enchanted Clearing is awake.", 200

    update_data = request.get_json(silent=True)
    if not update_data:
        return "No data", 400

    # ====================== PROCESS UPDATE ======================
    async def process_update():
        try:
            update = Update.de_json(update_data, tg_app.bot)

            # ==================== MAINTENANCE MODE ====================
            global MAINTENANCE_MODE
            OWNER_CHAT_ID = 7399488750

            if MAINTENANCE_MODE:
                chat_id = None
                if update.effective_chat:
                    chat_id = update.effective_chat.id
                elif update.callback_query and update.callback_query.message:
                    chat_id = update.callback_query.message.chat.id

                if chat_id and chat_id != OWNER_CHAT_ID:
                    try:
                        if update.message:
                            await tg_app.bot.send_message(chat_id=chat_id, text=MAINTENANCE_MESSAGE, parse_mode='HTML')
                        elif update.callback_query:
                            await update.callback_query.answer(
                                "🌿 The Enchanted Clearing is under maintenance.\nPlease come back later!", 
                                show_alert=True
                            )
                    except:
                        pass
                    return

            # ==================== NORMAL PROCESSING ====================
            if update.message and update.message.text:
                text = update.message.text.lower().strip()
                chat_id = update.effective_chat.id
                user_msg_id = update.message.message_id
                name = update.effective_user.first_name if update.effective_user else "Traveler"

                if chat_id not in forest_memory:
                    forest_memory[chat_id] = []
                forest_memory[chat_id].append(user_msg_id)

                # Registration check
                if not text.startswith("/start"):
                    profile = await get_user_profile(chat_id)
                    if not profile:
                        await tg_app.bot.send_animation(
                            chat_id=chat_id,
                            animation=HELLO_GIF,
                            caption="🌿 <b>Welcome to the Forest Hub</b>\n\n"
                                    "To start using the resources, please tap the button below.",
                            parse_mode='HTML',
                            reply_markup=get_start_keyboard()
                        )
                        return

                await update_last_active(chat_id)

                # Command handlers
                if text.startswith("/start"):
                    await send_initial_welcome(chat_id, name)
                elif text.startswith("/forest"):
                    await handle_info(chat_id)
                elif text.startswith("/testerror"):
                    1 / 0
                elif text.startswith("/history"):
                    await handle_history(chat_id, name)
                elif text.startswith("/leaderboard"):
                    await handle_leaderboard(chat_id)
                elif text.startswith("/profile"):
                    await handle_profile(chat_id, name)
                elif text.startswith("/mystats"):
                    await handle_stats(chat_id, name)
                elif text.startswith("/menu"):
                    profile = await get_user_profile(chat_id)
                    is_first = not bool(profile.get('has_seen_menu', False)) if profile else True
                    await send_full_menu(chat_id, name, is_first_time=is_first)
                elif text.startswith("/myid"):
                    await send_myid(chat_id)
                elif text.startswith("/clear"):
                    await handle_clear(chat_id, user_msg_id, name)
                elif text.startswith("/feedback"):
                    feedback_text = text.replace("/feedback", "").strip()
                    if feedback_text:
                        await handle_feedback(chat_id, name, feedback_text)
                    else:
                        await tg_app.bot.send_message(
                            chat_id=chat_id,
                            text="🌿 Please write your feedback after the /feedback command."
                        )
                elif text.startswith("/viewfeedback") or text.startswith("/feedbacks"):
                    await handle_view_feedback(
                        chat_id,
                        update.effective_user.id if update.effective_user else None
                    )
                elif text.startswith("/resetfirst") or text.startswith("/reset"):
                    await handle_reset_first_time(chat_id)

            elif update.callback_query:
                query = update.callback_query
                chat_id = update.effective_chat.id
                first_name = update.effective_user.first_name if update.effective_user else "Wanderer"

                if query.data in ["show_main_menu", "main_menu"]:
                    await handle_callback(update)
                    return

                profile = await get_user_profile(chat_id)
                if not profile:
                    await tg_app.bot.send_animation(
                        chat_id=chat_id,
                        animation=HELLO_GIF,
                        caption="🌿 <b>Welcome to the Forest Hub</b>\n\n"
                                "To start using the resources, please tap the button below.",
                        parse_mode='HTML',
                        reply_markup=get_start_keyboard()
                    )
                    return

                await handle_callback(update)

        except Exception as e:
            print(f"🔴 ERROR in process_update: {type(e).__name__} - {e}")
            print(traceback.format_exc())

    # ====================== FIXED: Always create a fresh loop (Vercel-safe) ======================
    try:
        # Create a brand new event loop for every request - this fixes the thread error
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop.run_until_complete(process_update())
        
        # Clean up
        loop.close()
        
    except Exception as e:
        print(f"🔴 CRITICAL WEBHOOK ERROR: {type(e).__name__} - {e}")
        print(traceback.format_exc())

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
