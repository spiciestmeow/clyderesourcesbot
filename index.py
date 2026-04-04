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

# 1. Configuration (These MUST be set in Vercel Environment Variables)
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

tg_app = Application.builder().token(TOKEN).build()

async def get_vamt_data():
    """Fetches VAMT data from your Supabase 'vamt_keys' table"""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    async with httpx.AsyncClient() as client:
        # Targets the table you created in the screenshot
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*"
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        return None

async def send_welcome_message(chat_id, first_name):
    """The Ghibli-themed welcome message with the Check Status button"""
    keyboard = [
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [
            # This triggers the handle_callback function below
            InlineKeyboardButton("📊 Check Key Status", callback_data="check_vamt")
        ],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("🌿 Contact & Inquiries", url="https://t.me/caydigitals")]
    ]
    
    # Manila Time Greeting
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    safe_name = html.escape(first_name)
    caption = (
        f"{time_icon} {greeting}, <b>{safe_name}</b>!\n\n"
        "<b>You've stumbled upon our hidden clearing. This space is built "
        "to help you find the resources you need, simply and peacefully.</b>\n\n"
        "<b>We're glad to have you! Explore the paths below to begin. 🍃</b>"
    )

    GIF_URL = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExanJlb3NqOHlwNDNmbmtlMnZtc2NramxmOXMydnU0a3B4amN3YnBiZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"

    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=GIF_URL,
        caption=caption,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_callback(update: Update):
    """Updates the message with live VAMT data when the button is clicked"""
    query = update.callback_query
    await query.answer("Fetching from the hub... 🍃")

    if query.data == "check_vamt":
        data = await get_vamt_data()
        
        if not data:
            await query.edit_message_caption(
                caption="❌ No key data found. Is the Pusher running?",
                reply_markup=query.message.reply_markup
            )
            return

        # Build the stock report string
        report = "<b>📊 VAMT Remaining Count:</b>\n\n"
        for item in data:
            report += f"🔑 <code>{item['key_id']}</code>: <b>{item['remaining']}</b> left\n"
        
        report += f"\n<i>Updated: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🍃"

        # Swaps the original caption for the live data
        await query.edit_message_caption(
            caption=report,
            parse_mode='HTML',
            reply_markup=query.message.reply_markup
        )

@app.route('/api/index', methods=['POST'])
def webhook():
    """Vercel entry point - DO NOT REMOVE THIS"""
    try:
        data = request.get_json(force=True)
        
        async def handle_update():
            if not tg_app.bot_data:
                await tg_app.initialize()
            
            update = Update.de_json(data, tg_app.bot)
            
            # Route: Commands
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            
            # Route: Button Clicks
            elif update.callback_query:
                await handle_callback(update)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle_update())
        loop.close()
        
        return "OK", 200
    except Exception as e:
        print(f"Error: {e}")
        return str(e), 500

@app.route('/')
def index():
    return "🍃 Clyde Tech Hub is floating in the wind..."
