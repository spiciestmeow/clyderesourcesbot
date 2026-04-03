import os
import asyncio
import html
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

app = Flask(__name__)

# Load token
TOKEN = os.getenv("BOT_TOKEN")
tg_app = Application.builder().token(TOKEN).build()

async def send_welcome_message(chat_id, first_name):
    # MODERN LAYOUT: 1 Top, 2 Middle, 1 Bottom
    keyboard = [
        # Main Feature (Wide)
        [InlineKeyboardButton("🎮 Access Steam Accounts", url="https://clyderesourcehub.short.gy/steam-account")],
        
        # Tools & Hub (Split)
        [
            InlineKeyboardButton("🛠️ OS & Tech Tips", url="https://clyderesourcehub.short.gy/learn-and-guides"),
            InlineKeyboardButton("🍃 The Hub", url="https://clyderesourcehub.short.gy/")
        ],
        
        # Support/Contact (Wide)
        [InlineKeyboardButton("🌿 Contact Support", url="https://t.me/caydigitals")]
    ]
    
    # Time-based Logic
    user_tz = pytz.timezone('Asia/Manila')
    now = datetime.now(user_tz)
    current_hour = now.hour

    # Ghibli Time-Vibes
    if 5 <= current_hour < 12:
        greeting, icon = "Good morning", "🌅"
    elif 12 <= current_hour < 18:
        greeting, icon = "Good afternoon", "🌤️"
    else:
        greeting, icon = "Good evening", "🌙"

    safe_name = html.escape(first_name)
    
    # Modernized Content (Minimalist & Direct)
    caption = (
        f"{icon} {greeting}, <b>{safe_name}</b>!\n\n"
        "<b>Welcome to the Clearing.</b>\n"
        "We've gathered the finest digital tools and system wisdom for your journey. "
        "Whether you're installing a new OS or looking for a game, the path starts here.\n\n"
        f"<i>System Status: 🔋 Fully Charged • {now.strftime('%H:%M')} PHT</i>"
    )

    # Recommending a "Cleaner" Ghibli GIF (Jiro/Engineer vibe)
    GIF_URL = "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExNHJ4Znd4Znd4Znd4Znd4Znd4Znd4Znd4Znd4Znd4Znd4Znd4Znd4JmVwPXYxX2ludGVybmFsX2dpZl9ieV9pZCZjdD1n/3o7TKMGpxP5YI0QJxe/giphy.gif"

    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=GIF_URL,
        caption=caption,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.route('/api/index', methods=['POST'])
def webhook():
    try:
        data = request.get_json(force=True)
        async def handle():
            if not tg_app.bot_data: await tg_app.initialize()
            update = Update.de_json(data, tg_app.bot)
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle())
        loop.close()
        return "OK", 200
    except Exception as e:
        return str(e), 500

@app.route('/')
def index():
    return "🍃 System Online."
