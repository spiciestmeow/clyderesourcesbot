import os
import asyncio
import html
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

app = Flask(__name__)

# Load token - Ensure this is set in Vercel Dashboard!
TOKEN = os.getenv("BOT_TOKEN")
tg_app = Application.builder().token(TOKEN).build()

async def send_welcome_message(chat_id, first_name):
    keyboard = [
        [
            InlineKeyboardButton("Steam Accounts", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("Learn & Guides", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("Our Website", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("Contact & Advertise", url="https://t.me/caydigitals")]
    ]
    
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour

    if 5 <= current_hour < 12:
        greeting = "Good morning"
    elif 12 <= current_hour < 18:
        greeting = "Good afternoon"
    else:
        greeting = "Good evening"

    safe_name = html.escape(first_name)
    
    # Use HTML tags since you set parse_mode='HTML'
    caption = (
        f"👋 {greeting}, <b>{safe_name}</b>!\n\n"
        "<b>You've found the heart of our project. This channel is designed "
        "to help you navigate our ecosystem quickly and easily.</b>\n\n"
        "<b>We're glad to have you here! Check out the buttons below to get started. 🌿</b>"
    )

    GIF_URL = "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExZm9zemV0aTI3MTZzYTR6MmVoNDVuNWRjbzc5ZzB5eGZscDUzYjhzOSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/PgPVijEEPl6gw8WRRl/giphy.gif"

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
        
        async def process_update():
            # Initialize bot if not already done
            if not tg_app.bot_data: 
                await tg_app.initialize()
            
            update = Update.de_json(data, tg_app.bot)
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)

        # Better way to run async in Flask/Vercel
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_update())
        loop.close()
        
        return "OK", 200
    except Exception as e:
        print(f"Error: {e}")
        return str(e), 500

@app.route('/')
def index():
    return "Clyde Resource Bot is Online!"
