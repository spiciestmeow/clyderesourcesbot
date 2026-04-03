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
    """Sends the Ghibli-themed welcome message with a focus on Tech/OS tips"""
    
    # 🛠️ for Tech/Installation, 📜 for the 'Guides' feel
    keyboard = [
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("🌿 Contact & Inquiries", url="https://t.me/caydigitals")]
    ]
    
    # Time-based Greeting (Manila Time)
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour

    if 5 <= current_hour < 12:
        greeting = "Good morning"
        time_icon = "🌅"
    elif 12 <= current_hour < 18:
        greeting = "Good afternoon"
        time_icon = "🌤️"
    else:
        greeting = "Good evening"
        time_icon = "🌙"

    safe_name = html.escape(first_name)
    
    # Content revised for Tech Tips & OS Installation
    caption = (
        f"{time_icon} {greeting}, <b>{safe_name}</b>!\n\n"
        "<b>You've stumbled upon our hidden clearing. This space is built "
        "to help you find the resources you need, simply and peacefully.</b>\n\n"
        "<b>We're glad to have you! Explore the paths below to begin. 🍃</b>"
    )

    GIF_URL = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExcDliYmZqdm9scXYzbm5xbHVqajg0d2pxd2x6eTY3dWFicDVhd3R0OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/x5HlLDaLMZNVS/giphy.gif"

    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=GIF_URL,
        caption=caption,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.route('/api/index', methods=['POST'])
def webhook():
    """Optimized Entry Point for Vercel"""
    try:
        data = request.get_json(force=True)
        
        async def handle_update():
            # Initialize bot components properly
            if not tg_app.bot_data:
                await tg_app.initialize()
            
            update = Update.de_json(data, tg_app.bot)
            
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(
                    update.effective_chat.id, 
                    update.effective_user.first_name
                )
        
        # Using a fresh loop for each request to prevent Vercel 500 'Loop Closed' errors
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











