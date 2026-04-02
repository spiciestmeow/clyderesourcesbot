import os
import asyncio
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
    """Function to generate and send the dynamic Ghibli message"""
    
    # 1. Define the Keyboard Layout
    keyboard = [
        [
            InlineKeyboardButton("🛒 Steam Accounts 🛒", url="https://clydehub.notion.site/d1402294d90683468aa1814447299e13?v=0c802294d90683e69030086dfb718034"),
            InlineKeyboardButton("🔑 Learn & Guides 🔑", url="https://clydehub.notion.site/d1402294d90683468aa1814447299e13?v=21502294d9068345834508d28ffbe79e")
        ],
        [InlineKeyboardButton("🌐 Our Website 🌐", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("📩 Contact & Advertise 📩", url="https://t.me/caydigitals")]
    ]
    
    # 2. Handle Dynamic Time-based Greeting
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour

    if 5 <= current_hour < 12:
        greeting = "Good morning"
    elif 12 <= current_hour < 18:
        greeting = "Good afternoon"
    else:
        greeting = "Good evening"

    # 3. Create the Bold Caption
    caption = (
        f"👋 **{greeting}, {first_name}!**\n\n"
        "**You've found the heart of our project. This channel is designed "
        "to help you navigate our ecosystem quickly and easily.**\n\n"
        "**We're glad to have you here! Check out the buttons below to get started. 🌿**"
    )

    GIF_URL = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExMm1kbGExNW12Z3ZpcjRtZmcwcjAxNmJ3YnA5NmRzMjQwNno2NGo2dSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/tkaDAjbZUmoH1a1Z2R/giphy.gif"

    # 4. Send the message with Markdown enabled
    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=GIF_URL,
        caption=caption,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.route('/api/index', methods=['POST'])
def webhook():
    """Vercel Entry Point"""
    try:
        data = request.get_json(force=True)
        # Initialize the bot within the request context
        async def handle_update():
            async with tg_app:
                update = Update.de_json(data, tg_app.bot)
                if update.message and update.message.text in ["/start", "/menu"]:
                    await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
        
        asyncio.run(handle_update())
        return "OK", 200
    except Exception as e:
        print(f"Error: {e}")
        return "Internal Error", 500

@app.route('/')
def index():
    return "Clyde Resource Bot is Online!"
