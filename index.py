import os
import asyncio
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime

app = Flask(__name__)

# Security: We get the token from Vercel's settings, not the code
TOKEN = os.getenv("BOT_TOKEN")
tg_app = Application.builder().token(TOKEN).build()

def get_greeting():
    hour = datetime.now().hour
    if 5 <= hour < 12: return "Good morning"
    elif 12 <= hour < 17: return "Good afternoon"
    else: return "Good evening"

async def process_update(update: Update):
    if update.message and update.message.text in ["/start", "/menu"]:
        greeting = get_greeting()
        username = update.effective_user.first_name or "there"
        
        # WIDE BUTTONS CONFIG
        keyboard = [
            [InlineKeyboardButton("🛒      Steam Accounts      🛒", url="https://your-link.com")],
            [InlineKeyboardButton("🔑      Learn & Guides      🔑", url="https://your-link.com")],
            [InlineKeyboardButton("🔵      Official Discord      🔵", url="https://discord.gg/invite")],
            [InlineKeyboardButton("🌐      Our Website      🌐", url="https://clyderesourcehub.short.gy/")],
            [InlineKeyboardButton("🌟      Reviews & Feedback      🌟", url="https://your-link.com")],
            [InlineKeyboardButton("📨      Contact & Advertise      📨", url="https://your-link.com")]
        ]

        caption = f"👋 {greeting}, {username}!\n\nThis channel serves as the primary router for our project. ❤️"
        GIF_URL = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExMm1kbGExNW12Z3ZpcjRtZmcwcjAxNmJ3YnA5NmRzMjQwNno2NGo2dSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/tkaDAjbZUmoH1a1Z2R/giphy.gif"

        await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=GIF_URL,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

@app.route('/api/index', methods=['POST'])
async def webhook_handler():
    async with tg_app:
        data = request.get_json(force=True)
        update = Update.de_json(data, tg_app.bot)
        await process_update(update)
    return "OK", 200

@app.route('/')
def index():
    return "Bot is Online"
