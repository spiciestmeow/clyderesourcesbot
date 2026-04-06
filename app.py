import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz
import time


forest_memory = {}
app = Flask(__name__)


# ==================== ANTI-XP ABUSE ====================
xp_cooldowns = {}         
user_action_history = {}

COOLDOWN_SECONDS = {
    "view_windows": 8,
    "view_office": 8,
    "view_netflix": 10,
    "reveal_netflix": 15,
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
WELCOME_GIF   = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExeWZzOHRrYjRycTI4d2Z2eXR6bWNiMm1yYXVqbzVrb3NmczB2ZHdmayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/wsKqNQmHYZfs4/giphy.gif"
MENU_GIF      = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExczJsZ25kM2N1N2twOHhmNWRsd3N6eWlyZ3N5M29pdmxsdDMzOHVscCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"
INVENTORY_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
ABOUT_GIF     = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdTFqMHB0ODVxdmFoMHl3dzZyM2swanlicmRibGk1bjdpcjFsdnl1biZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/x5HlLDaLMZNVS/giphy.gif"
HELP_GIF      = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNWxybTY5bXA0ejg1cGxxNTY3d3IyY3A4NGtkZ2gyOXkxcnlwZzN2NCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/J4FsxFgZgN2HS/giphy.gif"
LOADING_GIF   = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
MYID_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
CLEAN_GIF   = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
HELLO_GIF = "https://i.pinimg.com/originals/6a/a3/7f/6aa37fd0017bdb291ca8cbdd8b0ede52.gif"

# ==================== MAINTENANCE MODE ====================
MAINTENANCE_MODE = True
MAINTENANCE_MESSAGE = (
    "🌿 <b>The Enchanted Clearing is currently under maintenance</b>\n\n"
    "The ancient trees are resting and being prepared for new wonders...\n\n"
    "We will be back very soon with a smoother experience!\n\n"
    "<i>Thank you for your patience, kind wanderer.</i> 🍃✨"
)

tg_app = Application.builder().token(TOKEN).build()
loop = asyncio.new_event_loop()
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
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")]])

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
         InlineKeyboardButton("📑 Office Keys", callback_data="vamt_filter_office")
        ],
        [InlineKeyboardButton("🍿 Netflix Keys", callback_data="vamt_filter_netflix")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
    ])

def get_back_to_inventory_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Scroll Selection", callback_data="check_vamt")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to the Clearing", callback_data="main_menu")]])


def get_first_time_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❓ Start Here → Guidance", callback_data="help")],   # Highlighted
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
    """Add XP with cooldown + rate limit + detailed logging to xp_history table"""
    
    current_time = time.time()

    if chat_id not in xp_cooldowns:
        xp_cooldowns[chat_id] = {}
    if chat_id not in user_action_history:
        user_action_history[chat_id] = []

    # Anti-Abuse Checks
    user_action_history[chat_id] = [t for t in user_action_history[chat_id] if current_time - t < 60]
    if len(user_action_history[chat_id]) >= MAX_ACTIONS_PER_MINUTE:
        if query:
            try:
                await query.answer("🌿 The forest is quite busy right now... Please slow down.", show_alert=True)
            except:
                pass
        return False

    last_used = xp_cooldowns[chat_id].get(action, 0)
    if current_time - last_used < COOLDOWN_SECONDS.get(action, 8):
        if query:
            try:
                await query.answer("🌿 The forest spirits need a moment to rest... Try again soon!", show_alert=True)
            except:
                pass
        return False

    xp_cooldowns[chat_id][action] = current_time
    user_action_history[chat_id].append(current_time)

    # ====================== DETERMINE XP AMOUNT ======================
    profile = await get_user_profile(chat_id)
    
    xp_amount = 0

    if action == "guidance":
        current_reads = profile.get('guidance_reads', 0) if profile else 0
        if current_reads == 0:
            xp_amount = 8
    elif action == "lore":
        current_reads = profile.get('lore_reads', 0) if profile else 0
        if current_reads == 0:
            xp_amount = 8
    elif action in ["view_windows", "view_office", "view_netflix"]:
        xp_amount = 6
    elif action == "reveal_netflix":
        xp_amount = 10
    elif action == "profile":
        xp_amount = 5
    elif action == "clear":
        xp_amount = 5

    if xp_amount <= 0:
        return True

    # ====================== DATABASE OPERATIONS ======================
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

    leveled_up = False
    old_level = 1
    new_level = 1
    previous_xp = 0
    new_xp = 0

    if profile:
        old_level = profile.get('level') or 1
        previous_xp = profile.get('xp') or 0
        current_total = profile.get('total_xp_earned') or 0

        new_xp = previous_xp + xp_amount
        new_total = current_total + xp_amount
        new_level = old_level

        while True:
            xp_required_for_next = get_cumulative_xp_for_level(new_level + 1)
            if new_xp < xp_required_for_next:
                break
            new_level += 1

        leveled_up = new_level > old_level

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

        payload = {
            "xp": new_xp,
            "level": new_level,
            "first_name": first_name,
            "last_active": "now()",
            "total_xp_earned": new_total,
            **stats_update
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/user_profiles?chat_id=eq.{chat_id}",
                headers=headers,
                json=payload
            )

        # Log XP
        log_payload = {
            "chat_id": chat_id,
            "first_name": first_name,
            "action": action,
            "xp_earned": xp_amount,
            "previous_xp": previous_xp,
            "new_xp": new_xp,
            "previous_level": old_level,
            "new_level": new_level,
            "leveled_up": leveled_up
        }

        async with httpx.AsyncClient(timeout=8.0) as client:
            try:
                await client.post(f"{SUPABASE_URL}/rest/v1/xp_history", headers=headers, json=log_payload)
            except Exception as e:
                print(f"⚠️ Failed to log XP history for user {chat_id}: {e}")

        if leveled_up:
            await send_level_up_message(chat_id, first_name, old_level, new_level)

    else:
        # New user
        payload = {
            "chat_id": chat_id,
            "first_name": first_name,
            "xp": xp_amount,
            "level": 1,
            "last_active": "now()",
            "has_seen_menu": False,
            "created_at": "now()",
            "total_xp_earned": xp_amount,
            "windows_views": 0, "office_views": 0, "netflix_views": 0,
            "netflix_reveals": 0, "times_cleared": 0, "guidance_reads": 0,
            "lore_reads": 0, "profile_views": 0
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(f"{SUPABASE_URL}/rest/v1/user_profiles", headers=headers, json=payload)

        log_payload = {
            "chat_id": chat_id,
            "first_name": first_name,
            "action": action,
            "xp_earned": xp_amount,
            "previous_xp": 0,
            "new_xp": xp_amount,
            "previous_level": 1,
            "new_level": 1,
            "leveled_up": False
        }

        async with httpx.AsyncClient(timeout=8.0) as client:
            try:
                await client.post(f"{SUPABASE_URL}/rest/v1/xp_history", headers=headers, json=log_payload)
            except Exception as e:
                print(f"⚠️ Failed to log XP history for new user {chat_id}: {e}")

    return True

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
        f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
        "🌿 <b>Welcome, dear wanderer, to Clyde's Enchanted Clearing</b>\n\n"
        "The ancient wind carries soft whispers through the leaves...\n"
        "Hidden wonders and gentle magic await those with kind hearts.\n\n"
        "<i>Tap the button below to step into the heart of the forest.</i> 🍃✨"
    )

    msg = await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())
    if chat_id not in forest_memory: forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

async def send_level_up_message(chat_id, first_name, old_level, new_level):
    """Send a beautiful level up celebration message"""
    title = get_level_title(new_level)
    
    caption = (
        f"🌟 <b>Congratulations, {html.escape(first_name)}!</b>\n\n"
        f"You have grown stronger!\n\n"
        f"🏷️ New Title: <b>{title}</b>\n"
        f"⭐ Level: <b>{old_level}</b> → <b>{new_level}</b>\n\n"
        "The forest spirits celebrate your growth.\n"
        "More scrolls and wonders are now within your reach.\n\n"
        "<i>May your bond with the Enchanted Clearing continue to deepen.</i> 🍃✨"
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
            "🌿 <b>Welcome to the Enchanted Clearing</b>\n\n"
            f"{level_info}\n\n"
            "Beneath the whispering ancient trees, many paths lie before you...\n\n"
            "🌱 <b>New wanderer?</b> We recommend starting with <b>Guidance</b> first.\n\n"
            "<i>May your steps be guided by gentle forest magic.</i> 🍃✨"
        )
        keyboard = get_first_time_menu_keyboard()
    else:
        caption = (
            f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
            "🌿 <b>Welcome back to the Enchanted Clearing</b>\n\n"
            f"{level_info}\n\n"
            "The clearing welcomes you back, wanderer.\n\n"
            "<i>May the forest welcome you once more.</i> 🍃✨"
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
        "🌿 <b>Forest Spirit Identification</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "✨ <i>The ancient mist slowly parts before you...</i>\n\n"
        "Deep within the heart of the Enchanted Clearing,\n"
        "the oldest trees awaken to reveal your true essence.\n\n"
        "🪄 <b>The Forest Spirit Whispers:</b>\n"
        f"🌳 <b>Your Eternal ID:</b> <code>{chat_id}</code>\n\n"
        "This number is your unique bond with the forest —\n"
        "a mark carried by only you among all wanderers.\n\n"
        "<i>May this knowledge guide and protect you on your journey.</i>\n\n"
        "🍃 <b>The trees shall remember you always.</b>"
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
    progress_bar = create_progress_bar(xp, xp_required_next, length=12)

    caption = (
        f"🌿 <b>{html.escape(first_name)}'s Forest Profile</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ <b>Title:</b> {get_level_title(level)}\n"
        f"⭐ <b>Level:</b> {level}\n\n"
        f"✨ <b>Experience:</b> {xp:,} / {xp_required_next:,} XP\n"
        f"{progress_bar}\n\n"
        f"📈 <b>To Next Level:</b> {xp_to_next:,} XP\n\n"
        "<i>The more you explore the clearing, the stronger your bond with the forest grows.</i> 🍃"
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

    progress_bar = create_progress_bar(xp, xp_required_next, length=13)

    # Date formatting
    joined_date = "Unknown"
    if profile.get('created_at'):
        try:
            dt = datetime.fromisoformat(profile['created_at'].replace('Z', '+00:00'))
            joined_date = dt.strftime("%B %d, %Y")
        except:
            joined_date = str(profile['created_at'])[:10]

    last_active = "Just now"
    if profile.get('last_active'):
        try:
            dt = datetime.fromisoformat(profile['last_active'].replace('Z', '+00:00'))
            dt = dt.astimezone(pytz.timezone('Asia/Manila'))
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
        f"🌲 <b>{html.escape(first_name)}'s Forest Statistics</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ <b>Title:</b> {get_level_title(level)}\n"
        f"⭐ <b>Level:</b> {level}\n\n"
        f"✨ <b>Experience:</b> {xp:,} / {xp_required_next:,} XP\n"
        f"{progress_bar}\n\n"
        "📊 <b>Detailed Stats:</b>\n"
        f"• Total XP Earned: <b>{profile.get('total_xp_earned', xp):,}</b>\n"
        f"• Profile Views: <b>{profile_views}</b> times\n"
        f"• Windows Keys Viewed: <b>{windows_views}</b> times\n"
        f"• Office Keys Viewed: <b>{office_views}</b> times\n"
        f"• Netflix Keys Viewed: <b>{netflix_views}</b> times\n"
        f"• Netflix Cookies Revealed: <b>{netflix_reveals}</b> times\n"
        f"• Times Cleared the Forest: <b>{times_cleared}</b>\n"
        f"• Guidance Read: <b>{guidance_reads}</b> times\n"
        f"• Lore Read: <b>{lore_reads}</b> times\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🌱 <b>Joined:</b> {joined_date}\n"
        f"🌲 <b>Last Active:</b> {last_active}\n\n"
        "<i>The trees remember every step you've taken...</i> 🍃"
    )

    msg = await tg_app.bot.send_message(
        chat_id=chat_id,
        text=caption,
        parse_mode='HTML'
    )
    
    if chat_id not in forest_memory:
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

# ==================== XP HISTORY COMMAND ======================
async def handle_xp_history(chat_id, first_name):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{SUPABASE_URL}/rest/v1/xp_history?chat_id=eq.{chat_id}&order=created_at.desc&limit=20",
            headers=headers
        )
        data = response.json()

    if not data:
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text="🌿 No XP history yet. Start exploring the forest!"
        )
        return

    text = f"🌟 <b>{html.escape(first_name)}'s XP Journey</b>\n━━━━━━━━━━━━━━━━━━\n\n"

    for entry in data:
        ts = entry.get('created_at', '')[:16].replace('T', ' ')
        action = entry.get('action', 'unknown').replace('_', ' ').title()
        xp = entry.get('xp_earned', 0)
        level_info = ""
        if entry.get('leveled_up'):
            level_info = f" → Level {entry.get('new_level')} 🎉"

        text += f"🕒 {ts}\n"
        text += f"   {action} → <b>+{xp} XP</b>{level_info}\n"
        text += f"   {entry.get('previous_xp')} → {entry.get('new_xp')} XP\n\n"

    text += "<i>The trees remember every step of your growth...</i> 🍃"

    await tg_app.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
    
# ==================== LEADERBOARD COMMAND ======================
async def handle_leaderboard(chat_id):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_profiles?select=first_name,xp,level&order=xp.desc&limit=10",
                headers=headers
            )
            
            data = response.json()

            if not data:
                await tg_app.bot.send_message(
                    chat_id=chat_id,
                    text="🌿 The forest leaderboard is currently empty.\nBe the first to climb the ranks!"
                )
                return

            text = "🏆 <b>Top Wanderers of the Enchanted Clearing</b>\n━━━━━━━━━━━━━━━━━━\n\n"

            for rank, user in enumerate(data, 1):
                name = html.escape(user.get('first_name', 'Mysterious Wanderer'))
                xp = user.get('xp', 0)
                level = user.get('level', 1)
                title = get_level_title(level)

                medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"{rank}."
                
                text += f"{medal} <b>{name}</b>\n"
                text += f"   {title} • Level {level}\n"
                text += f"   ✨ {xp} XP\n\n"

            text += "<i>May the best wanderer continue to shine brightly.</i> 🍃✨"

            # Send the message and store its ID
            msg = await tg_app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode='HTML'
            )

            # IMPORTANT: Add to forest_memory so /clear can delete it
            if chat_id not in forest_memory:
                forest_memory[chat_id] = []
            forest_memory[chat_id].append(msg.message_id)

        except Exception as e:
            print(f"🔴 Leaderboard error: {e}")
            await tg_app.bot.send_message(
                chat_id=chat_id,
                text="🌫️ The ancient trees are having trouble reading the leaderboard right now..."
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
        "🕊️ <b>A Message Carried by the Wind</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Dear <b>{html.escape(str(first_name))}</b>,\n\n"
        "The gentle breeze has carried your words through the ancient trees...\n"
        "They have reached the caretaker of the Enchanted Clearing.\n\n"
        "Thank you for sharing your thoughts with this small, magical corner of the forest.\n\n"
        "<i>May your voice help the clearing bloom even brighter.</i> 🍃✨\n\n"
        f"🕒 <b>Sent:</b> {timestamp}"
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
            chat_id=7399488750,
            text=owner_message,
            parse_mode='HTML'
        )
    except Exception as e:
        print(f"Failed to send feedback to owner: {e}")

# ==================== VIEW FEEDBACK COMMAND (Owner Only) ======================
async def handle_view_feedback(chat_id, user_id):
    # Security: Only you (the owner) can use this command
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
        forest_memory[chat_id] = []

    # ====================== MAGICAL CLEARING ANIMATION ======================
    loading_msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=CLEAN_GIF,
        caption="🌫️ <b>The ancient mist begins to thicken...</b>",
        parse_mode="HTML"
    )

    await asyncio.sleep(1.8)
    await loading_msg.edit_caption(
        "🍃 <b>The wind spirit awakens...</b>\nWhispers of old paths are being carried away...", 
        parse_mode="HTML"
    )

    await asyncio.sleep(2.0)
    await loading_msg.edit_caption(
        "✨ <b>The forest is resetting...</b>\nAll footprints are gently erased by the glowing leaves.", 
        parse_mode="HTML"
    )

    await asyncio.sleep(1.2)

    # Delete the loading animation
    try:
        await tg_app.bot.delete_message(chat_id, loading_msg.message_id)
    except:
        pass

    # Directly show the main menu (no extra button or old message)
    await send_full_menu(chat_id, first_name, is_first_time=False)

    # Give XP for using /clear
    await add_xp(chat_id, first_name, "clear")

    print(f"🌿 Chat cleared magically for user {chat_id}")


    
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
            caption="🌫️ <i>The ancient mist begins to lift once more...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.3)
        await loading_msg.edit_caption("🌿 <i>The whispering trees lean in to welcome you home...</i>", parse_mode='HTML')
        await asyncio.sleep(1.3)
        await loading_msg.edit_caption("✨ <i>You stand again in the heart of the Enchanted Clearing...</i>", parse_mode='HTML')
        await asyncio.sleep(1.0)

        # Get or create profile
        profile = await get_user_profile(chat_id)
        if not profile:
            await add_xp(chat_id, first_name, "general")
            profile = await get_user_profile(chat_id)

        is_first_time = not bool(profile.get('has_seen_menu', False)) if profile else True

        # if is_first_time:
            # await update_has_seen_menu(chat_id)

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
            caption="🌿 <b>A gentle breeze rustles the leaves...</b>\n\n"
                "You stand at the edge of a mysterious forest...\n\n"
                "To step into the Enchanted Clearing, please press the button below.",
            parse_mode='HTML',
            reply_markup=get_start_keyboard()
        )
        return
        
    # Mark as seen
    # if not profile.get('has_seen_menu', False):
        # await update_has_seen_menu(chat_id)
    
    # ====================== INVENTORY & OTHER FEATURES ======================
    if query.data == "check_vamt":
        await query.message.edit_caption(
            caption="📜 <i>The doors of the Ancient Library creak open...</i>\n\n"
                    "Which scrolls call to your heart today, wanderer?\n\n"
                    "<i>The forest spirits await your choice.</i>", 
            parse_mode='HTML', 
            reply_markup=get_inventory_categories()
        )

    # ====================== FILTERED INVENTORY ======================
    elif query.data.startswith("vamt_filter_") or query.data.startswith("vamt_all_"):
        category = query.data.replace("vamt_filter_", "").replace("vamt_all_", "").lower()

        if category == "win":
            await add_xp(chat_id, first_name, "view_windows", query=query)
        elif category == "office":
            await add_xp(chat_id, first_name, "view_office", query=query)
        elif category == "netflix":
            await add_xp(chat_id, first_name, "view_netflix", query=query)

        # Get user level
        profile = await get_user_profile(chat_id)
        user_level = profile['level'] if profile else 1

        await query.message.edit_caption(
            caption=f"✨ <i>Searching the glade for {category.upper()}...</i>",
            parse_mode='HTML'
        )

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

            if category == "netflix" and "netflix" in s_type:
                filtered.append(item)
            elif category == "win" and any(x in s_type for x in ["windows", "win"]):
                filtered.append(item)
            elif category == "office" and "office" in s_type:
                filtered.append(item)

        if not filtered:
            await query.message.edit_caption(
                caption=f"🍃 <i>No {category.upper()} scrolls found in the clearing right now.</i>",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        # === Improved Level-based limit ===
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

        # ====================== NETFLIX ======================
        if category == "netflix":
            report = (
                "<b>🍿 Secret Netflix Cookies of the Forest</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>{len(filtered)} Cookies Resting in the Glade</b>\n"
                f"{limit_note}\n\n"
                "<i>Which one whispers to your spirit?</i>\n\n"
            )

            buttons = []
            # Force sequential numbering (Netflix Cookie 1, 2, 3...) for consistency
            for display_idx, item in enumerate(filtered[:limit], 1):
                display_name = f"Netflix Cookie {display_idx}"   # Force correct number

                status_text = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"

                report += f"✨ <b>{display_name}</b>\n"
                report += f"   Status: {status_text}\n"
                report += f"   Remaining: {item.get('remaining', 0)}\n\n"

                buttons.append([
                    InlineKeyboardButton(f"🔓 Reveal {display_name}", callback_data=f"reveal_nf|{display_idx}")
                ])

            buttons.append([InlineKeyboardButton("⬅️ Back to the Clearing", callback_data="check_vamt")])

            kb = InlineKeyboardMarkup(buttons)

            await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)
            return

        # ====================== WINDOWS & OFFICE ======================
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

    # ====================== REVEAL NETFLIX ======================
    elif query.data.startswith("reveal_nf|"):
        try:
            idx = int(query.data.split("|", 1)[1])
        except:
            await query.answer("Invalid selection", show_alert=True)
            return

        # === Double Loading Animation ===
        await query.message.edit_caption(
            caption="🍿 <i>Searching deep within the glowing glade...</i>",
            parse_mode='HTML'
        )
        await asyncio.sleep(1.3)

        await query.message.edit_caption(
            caption="🌟 <i>The hidden cookie spirit is slowly awakening...</i>\n\n"
                    "Please wait as the forest carefully reveals its secret...",
            parse_mode='HTML'
        )
        await asyncio.sleep(1.5)

        # === Get user level and rebuild the SAME limited list as the display ===
        profile = await get_user_profile(chat_id)
        user_level = profile['level'] if profile else 1

        # === Consistent limit logic with viewing section ===
        if user_level == 1:
            limit = 1
        elif user_level <= 3:
            limit = 2
        elif user_level <= 6:
            limit = 4 if user_level <= 5 else 5
        else:
            limit = 999

        data = await get_vamt_data()
        if not data:
            await query.answer("Database error", show_alert=True)
            return

        # Build the exact same filtered Netflix list
        filtered = [item for item in data if "netflix" in str(item.get('service_type', '')).lower()]
        filtered.sort(key=lambda x: (str(x.get('display_name') or ''), str(x.get('last_updated') or '')))

        # Safety check
        if idx < 1 or idx > len(filtered[:limit]):
            await query.answer(f"❌ Cookie not found", show_alert=True)
            return

        # Take the item from the LIMITED list (this fixes the inconsistency)
        item = filtered[idx - 1]

        cookie = str(item.get('key_id', '')).strip()
        display_name = str(item.get('display_name') or '').strip() or f"Forest Cookie {idx}"

        status = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"

        # Give XP
        await add_xp(chat_id, first_name, "reveal_netflix", query=query)

        report = (
            f"<b>🍿 {display_name} Revealed</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"🌿 Status: <b>{status}</b>\n"
            f"📦 Remaining: <b>{item.get('remaining', 0)}</b>\n\n"
            "<b>📋 The Hidden Cookie:</b>\n"
            f"<code>{html.escape(cookie[:800])}</code>\n\n"
            "<i>Long-press the code above to copy.\n"
            "Use it quickly before the magic fades.</i> 🍃"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Netflix Cookies", callback_data="back_to_netflix_list")]
        ])

        await query.message.edit_caption(
            caption=report, 
            parse_mode='HTML', 
            reply_markup=kb
        )

    # ====================== BACK TO NETFLIX LIST (No extra XP) ======================
    elif query.data == "back_to_netflix_list":
        # Redisplay Netflix list WITHOUT giving XP again
        category = "netflix"

        profile = await get_user_profile(chat_id)
        user_level = profile['level'] if profile else 1

        await query.message.edit_caption(
            caption="✨ <i>Loading Netflix cookies again...</i>",
            parse_mode='HTML'
        )

        data = await get_vamt_data()
        if not data:
            await query.message.edit_caption(
                caption="🌫️ <i>The mist is too thick... Database connection failed.</i>",
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
            "<i>Which one whispers to your spirit?</i>\n\n"
        )

        buttons = []
        for display_idx, item in enumerate(filtered[:limit], 1):
            display_name = f"Netflix Cookie {display_idx}"

            status_text = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"

            report += f"✨ <b>{display_name}</b>\n"
            report += f"   Status: {status_text}\n"
            report += f"   Remaining: {item.get('remaining', 0)}\n\n"

            buttons.append([
                InlineKeyboardButton(f"🔓 Reveal {display_name}", callback_data=f"reveal_nf|{display_idx}")
            ])

        buttons.append([InlineKeyboardButton("⬅️ Back to the Clearing", callback_data="check_vamt")])

        kb = InlineKeyboardMarkup(buttons)

        await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)
        return

    # ====================== ABOUT (Lore) ======================
    elif query.data == "about":
        await add_xp(chat_id, first_name, "lore", query=query)

        try: 
            await query.message.delete()
        except: 
            pass

        loading_msg = await tg_app.bot.send_animation(
            chat_id=chat_id,                                   # ← Fixed
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
            "<b>🌿 The Tale of Clyde's Enchanted Clearing</b>\n\n"
            "Long ago, in a hidden corner of the digital world, a gentle forest spirit named Clyde created this peaceful sanctuary.\n\n"
            "Inspired by the wonder of Studio Ghibli, this clearing was born as a place where wanderers can find rest, magic, and useful treasures — "
            "be it Steam accounts, learning guides, or activation keys.\n\n"
            "Here, kindness is the only key, and every visitor is welcomed with open arms by the whispering trees.\n\n"
            "<i>May this small enchanted corner bring you joy, wonder, and a little bit of magic in your journey.</i> 🍃✨"
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
                "🌿 <b>How to Navigate the Clearing</b>\n"
                "• Tap any button to explore the paths\n"
                "• Use <code>/menu</code> to return here anytime\n"
                "• Use <code>/clear</code> to renew your path\n\n"
                
                "📜 <b>Available Commands</b>\n"
                "• /start — Begin your journey anew\n"
                "• /menu — Return to the Enchanted Clearing\n"
                "• /profile — View your Forest Profile\n"
                "• /stats — View detailed Forest Statistics\n"
                "• /leaderboard — See Top Wanderers\n"
                "• /myid — Reveal your Eternal Forest ID\n"
                "• /clear — Cleanse and renew the clearing\n"
                "• /feedback — Send message to the caretaker\n\n"
                
                "🌲 <b>Treasures You Can Discover</b>\n"
                "• 🪄 Spirit Treasures — Steam accounts\n"
                "• 📜 Ancient Scrolls — Learning guides\n"
                "• 🌿 Forest Inventory — Windows, Office & Netflix keys\n"
                "• 🌲 The Whispering Forest — Main resource hub\n\n"
                
                "<b>Note for New Wanderers:</b>\n"
                "• You start at <b>Level 1 with 0 XP</b>\n"
                "• Your first actions will help you grow and unlock more items.\n\n"
                
                "<i>Tap Next → to learn about the Leveling System</i>"
            )
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("Next →", callback_data="help_page_2")],
                [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
            ])

        else:
            level_req_text = "\n".join(
                f"• Level {lvl} → {get_cumulative_xp_for_level(lvl):,} XP"
                for lvl in range(2, 11)
            )

            text = (
                "<b>❓ Guidance - Page 2/2</b>\n\n"
                "✨ <b>Forest Leveling System</b>\n"
                "Gain XP as you explore. Higher levels unlock more items.\n\n"
                
                "<b>How to Gain XP:</b>\n"
                "• View Win/Office Keys → <b>+6 XP</b>\n"
                "• View Netflix Keys → <b>+6 XP</b>\n"
                "• Reveal Netflix Cookie → <b>+10 XP</b>\n"
                "• /profile → <b>+5 XP</b>\n"
                "• /clear → <b>+5 XP</b>\n"
                "• Open Guidance → <b>+8 XP</b> (only first time)\n"
                "• Open Lore (About) → <b>+8 XP</b> (only first time)\n\n"
                
                "<b>Items Shown in Inventory:</b>\n"
                "• Level 1 → 1 item\n"
                "• Level 2–3 → 2 items\n"
                "• Level 4–5 → 4 items\n"
                "• Level 6 → 5 items\n"
                "• Level 7+ → All items\n\n"
                
                f"<b>Level Requirements:</b>\n"
                f"{level_req_text}\n\n"
                
                "<b>Note:</b>\n"
                "• New users start at Level 1 with 0 XP\n"
                "• You will see a celebration when you level up\n"
                "• Level 7 gives full access to all scrolls\n\n"
                
                "<i>The more you wander, the stronger your spirit grows.</i> 🍃✨"
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
    if request.method == 'GET': return "🌿 Clyde's Enchanted Clearing is awake.", 200
    update_data = request.get_json(silent=True)
    if not update_data: return "No data", 400

    async def process_update():
        update = Update.de_json(update_data, tg_app.bot)

        # ==================== MAINTENANCE MODE ====================
        MAINTENANCE_MODE = True
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
                        await update.callback_query.answer("🌿 The Enchanted Clearing is under maintenance.\nPlease come back later!", show_alert=True)
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

            # === STRICT REGISTRATION CHECK ===
            if not text.startswith("/start"):
                profile = await get_user_profile(chat_id)
                if not profile:
                    await tg_app.bot.send_animation(
                        chat_id=chat_id,
                        animation=HELLO_GIF,
                        caption="🌿 <b>A gentle breeze rustles the leaves...</b>\n\n"
                             "You stand at the edge of a mysterious forest.\n"
                             "The ancient trees seem to be watching you with quiet curiosity.\n\n"
                             "To step into the Enchanted Clearing and discover its hidden magic, "
                             "please press the button below.\n\n"
                             "<i>The forest is ready to welcome you...</i> 🍃✨",
                        parse_mode='HTML',
                        reply_markup=get_start_keyboard()
                    )
                    return

            # Command handlers
            if text.startswith("/start"): 
                await send_initial_welcome(chat_id, name)

            elif text.startswith("/history"):
                await handle_xp_history(chat_id, name)

            elif text.startswith("/leaderboard") or text.startswith("/top"):
                await handle_leaderboard(chat_id)

            elif text.startswith("/profile"):
                await handle_profile(chat_id, name)

            elif text.startswith("/stats"):
                await handle_stats(chat_id, name)

            elif text.startswith("/menu"): 
                profile = await get_user_profile(chat_id)
                is_first = not bool(profile.get('has_seen_menu', False)) if profile else True
                # Do NOT mark as seen here (soft version)
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
                        text="🌿 Please write your feedback after the /feedback command.\n\n"
                             "Example: `/feedback I really like the immersive captions!`"
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

            # === Allow Enter button for unregistered users ===
            if query.data in ["show_main_menu", "main_menu"]:
                await handle_callback(update)
                return

            # === Enforce registration for all other buttons ===
            profile = await get_user_profile(chat_id)
            if not profile:
                await tg_app.bot.send_animation(
                    chat_id=chat_id,
                    animation=HELLO_GIF,
                    caption="🌿 <b>A gentle breeze rustles the leaves...</b>\n\n"
                         "You stand at the edge of a mysterious forest.\n"
                         "The ancient trees seem to be watching you with quiet curiosity.\n\n"
                         "To step into the Enchanted Clearing and discover its hidden magic, "
                         "please press the button below.\n\n"
                         "<i>The forest is ready to welcome you...</i> 🍃✨",
                    parse_mode='HTML',
                    reply_markup=get_start_keyboard()
                )
                return

            # Registered user → normal handling
            await handle_callback(update)
    try: loop.run_until_complete(process_update())
    except Exception as e: print(f"🔴 Webhook Error: {e}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
