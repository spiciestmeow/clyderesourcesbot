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

# 1. Configuration (Vercel Environment Variables)
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

tg_app = Application.builder().token(TOKEN).build()

# --- Helper Functions ---

async def get_vamt_data():
    """Fetches key data from Supabase vamt_keys table"""
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        # Pulls service_type, key_id, and remaining
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

def mask_license_key(key_string):
    """Blurs the middle of the key for security"""
    if not key_string or key_string == "No Key Found":
        return "No Key Assigned"
    key_str = str(key_string).strip()
    if len(key_str) < 15:
        return key_str
    # Pattern: 6V38****-****-****-****-92FDB
    return f"{key_str[:4]}****-****-****-****-{key_str[-5:]}"

def get_main_menu_keyboard():
    """Original Clyde Resource Hub buttons with combined Contact & Advertise"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("📊 Check Key Status", callback_data="check_vamt")],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("📞 Contact & Advertise", url="https://t.me/caydigitals")]
    ])

async def send_welcome_message(chat_id, first_name):
    """Ghibli-themed welcome message with original content"""
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n"
        "<b>You've stumbled upon our hidden clearing. This space is built "
        "to help you find the resources you need, simply and peacefully.</b>\n\n"
        "<b>We're glad to have you! Explore the paths below to begin. 🍃</b>"
    )
    GIF_URL = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExanJlb3NqOHlwNDNmbmtlMnZtc2NramxmOXMydnU0a3B4amN3YnBiZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"

    await tg_app.bot.send_animation(
        chat_id=chat_id, animation=GIF_URL, caption=caption,
        parse_mode='HTML', reply_markup=get_main_menu_keyboard()
    )

async def handle_callback(update: Update):
    """Handles logic for Check Status and Back to Menu"""
    query = update.callback_query
    # Fix for double-click requirement
    await query.answer()

    if query.data == "main_menu":
        await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
        await query.message.delete()

    elif query.data == "check_vamt":
        try:
            await query.edit_message_caption(
                caption="🔎 <i>Consulting the hidden scrolls...</i>",
                parse_mode='HTML', reply_markup=query.message.reply_markup
            )

            data = await get_vamt_data()
            report = "<b>📊 Clyde's Resource Hub Inventory:</b>\n\n"
            
            for item in data:
                product = item.get('service_type', 'Unknown Product')
                count = item.get('remaining', 0)
                # Masking the key_id for security
                blurred_key = mask_license_key(item.get('key_id', 'No Key Found'))
                
                name_lower = str(product).lower()
                icon = "📑" if "office" in name_lower else "🪟" if "win" in name_lower else "📦"

                report += f"{icon} <b>{product}</b>\n"
                report += f"└ 🔑 Key: <code>{blurred_key}</code>\n"
                report += f"└ 📦 Stock: <b>{count}</b> left\n\n"
            
            report += f"<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🍃"
            back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]])
            await query.edit_message_caption(caption=report, parse_mode='HTML', reply_markup=back_kb)

        except Exception as e:
            await query.edit_message_caption(
                caption=f"⚠️ Hub Error: <code>{str(e)}</code>", 
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]])
            )

# --- Webhook Entry Point ---

@app.route('/', methods=['GET', 'POST'])
@app.route('/api/index', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🍃 Clyde Tech Hub is online.", 200
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        update_data = request.get_json(force=True)
        async def process():
            if not tg_app.bot_data: await tg_app.initialize()
            update = Update.de_json(update_data, tg_app.bot)
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            elif update.callback_query:
                await handle_callback(update)
        loop.run_until_complete(process())
        loop.close()
        return "OK", 200
    except:
        return "OK", 200
