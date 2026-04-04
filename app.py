import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

app = Flask(__name__)

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
         InlineKeyboardButton("📑 Office Keys", callback_data="vamt_filter_office")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
    ])

def get_back_to_inventory_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Scroll Selection", callback_data="check_vamt")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to the Clearing", callback_data="main_menu")]])

# ==================== MESSAGES ====================
async def send_initial_welcome(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"
    caption = f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n🌿 <b>Welcome to Clyde's Enchanted Clearing</b>\n\n<i>May the forest spirits watch over you.</i> 🍃✨"
    await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())

async def send_full_menu(chat_id, first_name):
    caption = f"🌿 <b>You have entered the Enchanted Clearing</b>\n\nChoose your path beneath the whispering trees..."
    await tg_app.bot.send_animation(chat_id=chat_id, animation=MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())

# --- NEW STATIC ID FUNCTION ---
async def send_myid(chat_id):
    keyboard = [
        [InlineKeyboardButton("Your telegram user id", callback_data="static_id")],
        [InlineKeyboardButton("comming soon", callback_data="coming_soon")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    caption = "🌿 <b>Forest Spirit Identification</b>\n\nRegistration is currently undergoing a ritual of growth..."
    await tg_app.bot.send_animation(chat_id=chat_id, animation=HELP_GIF, caption=caption, parse_mode='HTML', reply_markup=reply_markup)

# ==================== CALLBACK ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    if query.data in ["show_main_menu", "main_menu"]:
        try: await query.message.delete()
        except: pass
        loading_msg = await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=LOADING_GIF, caption="🍃 <i>Guided by fireflies...</i>", parse_mode='HTML')
        await asyncio.sleep(1.0)
        try: await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except: pass
        await send_full_menu(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "check_vamt":
        await query.message.edit_caption(caption="🌿 <b>The Ancient Library</b>", reply_markup=get_inventory_categories())

    elif query.data == "about":
        text = "<b>🌿 About Clyde's Enchanted Clearing</b>\n\nInspired by the magic of Studio Ghibli. Digital treasures await."
        await query.message.edit_caption(caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

    elif query.data == "help":
        text = "<b>❓ Guidance</b>\n\n• Use <b>/menu</b> for the full list.\n• Use <b>/myid</b> to see your ID."
        await query.message.edit_caption(caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

# ==================== WEBHOOK ====================
async def start_tg_app():
    await tg_app.initialize()
    await tg_app.start()

loop.run_until_complete(start_tg_app())

@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🌿 Awake.", 200
    update_data = request.get_json(silent=True)
    if not update_data: return "No data", 400

    async def process_update():
        update = Update.de_json(update_data, tg_app.bot)
        if update.message and update.message.text:
            text = update.message.text.lower().strip()
            chat_id = update.effective_chat.id
            name = update.effective_user.first_name if update.effective_user else "Traveler"

            if text.startswith("/start"): await send_initial_welcome(chat_id, name)
            elif text.startswith("/menu"): await send_full_menu(chat_id, name)
            elif text.startswith("/myid"): await send_myid(chat_id) # ADDED THIS
                
        elif update.callback_query: await handle_callback(update)

    try: loop.run_until_complete(process_update())
    except Exception as e: print(f"🔴 Webhook Error: {e}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
