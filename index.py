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
    """Sends the Ghibli-themed welcome message with the 1-2-1 modern layout"""
    
    # Modern 1-2-1 Layout
    keyboard = [
        [InlineKeyboardButton("🎮 Access Steam Accounts", url="https://clyderesourcehub.short.gy/steam-account")],
        [
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides"),
            InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")
        ],
        [InlineKeyboardButton("🌿 Contact & Inquiries", url="https://t.me/caydigitals")]
    ]
    
    user_tz = pytz.timezone('Asia/Manila')
    now = datetime.now(user_tz)
    current_hour = now.hour

    if 5 <= current_hour < 12:
        greeting, time_icon = "Good morning", "🌅"
    elif 12 <= current_hour < 18:
        greeting, time_icon = "Good afternoon", "🌤️"
    else:
        greeting, time_icon = "Good evening", "🌙"

    safe_name = html.escape(first_name)
    
    caption = (
        f"{time_icon} {greeting}, <b>{safe_name}</b>!\n\n"
        "<b>Welcome to the Clearing.</b>\n"
        "We've gathered the finest digital tools and system wisdom for your journey. "
        "Whether you're installing a new OS or looking for a game, the path starts here.\n\n"
        f"<i>System Status: 🔋 Fully Charged • {now.strftime('%H:%M')} PHT</i>"
    )

    # Updated to the Jiro/Engineer GIF for the Tech vibe
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
        user_tz = pytz.timezone('Asia/Manila')
        
        async def handle_update():
            if not tg_app.bot_data:
                await tg_app.initialize()
            
            update = Update.de_json(data, tg_app.bot)
            
            if update.message and update.message.text:
                text = update.message.text.lower()
                chat_id = update.effective_chat.id

                # 1. Start or Menu Commands
                if text.startswith("/start") or text.startswith("/menu"):
                    await send_welcome_message(chat_id, update.effective_user.first_name)
                
                # 2. Status Command (The part you're adding)
                elif text.startswith("/status"):
                    now = datetime.now(user_tz).strftime('%H:%M:%S')
                    status_text = (
                        "<b>🔋 System Pulse</b>\n\n"
                        f"<b>Time:</b> {now} PHT\n"
                        "<b>Status:</b> Operational 🍃\n"
                        "<b>Server:</b> Edge Node\n\n"
                        "<i>All paths are clear. Happy wandering.</i>"
                    )
                    await tg_app.bot.send_message(
                        chat_id=chat_id, 
                        text=status_text, 
                        parse_mode='HTML'
                    )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle_update())
        loop.close()
        
        return "OK", 200
    except Exception as e:
        return str(e), 500

@app.route('/')
def index():
    return "🍃 Clyde Tech Hub is floating in the wind..."
