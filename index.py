import os
import asyncio
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime

app = Flask(__name__)

# Load token
TOKEN = os.getenv("BOT_TOKEN")
tg_app = Application.builder().token(TOKEN).build()

async def send_welcome_message(chat_id, first_name):
    """Simple async function to send the response"""
    keyboard = [
        [InlineKeyboardButton("🛒      Steam Accounts      🛒", url="https://your-link.com")],
        [InlineKeyboardButton("🔑      Learn & Guides      🔑", url="https://your-link.com")],
        [InlineKeyboardButton("🔵      Official Discord      🔵", url="https://discord.gg/invite")],
        [InlineKeyboardButton("🌐      Our Website      🌐", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("🌟      Reviews & Feedback      🌟", url="https://your-link.com")],
        [InlineKeyboardButton("📨      Contact & Advertise      📨", url="https://your-link.com")]
    ]
    
    caption = f"👋 Hello, {first_name}!\n\nThis channel serves as the primary router for our project. ❤️"
    GIF_URL = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExMm1kbGExNW12Z3ZpcjRtZmcwcjAxNmJ3YnA5NmRzMjQwNno2NGo2dSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/tkaDAjbZUmoH1a1Z2R/giphy.gif"

    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=GIF_URL,
        caption=caption,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.route('/api/index', methods=['POST'])
def webhook():
    """The main entry point for Vercel"""
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, tg_app.bot)
        
        if update.message and update.message.text in ["/start", "/menu"]:
            # Create a new event loop for this specific request
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Start the bot context and send the message
            async def run_bot():
                async with tg_app:
                    await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            
            loop.run_until_complete(run_bot())
            loop.close()

        return "OK", 200
    except Exception as e:
        print(f"Error: {e}")
        return "Internal Error", 500

@app.route('/')
def index():
    return "Bot is online!"
