import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

# --- GLOBAL VARIABLES (Vercel looks here) ---
app = Flask(__name__)

TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize the TG app globally
tg_app = Application.builder().token(TOKEN).build()

LOGO_GIF = "https://media.giphy.com/media/cBKMTJGAE8y2Y/giphy.gif" 

# ... (Keep your get_vamt_data, get_main_menu_keyboard, and handle_callback functions here)

async def send_welcome_message(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n"
        "<b>You've wandered into our hidden clearing. The wind whispers of new "
        "treasures found deep within the digital thicket.</b>\n\n"
        "<i>May your path be clear and your scrolls be plenty.</i> 🍃"
    )

    try:
        await tg_app.bot.send_animation(
            chat_id=chat_id, 
            animation=LOGO_GIF, 
            caption=caption,
            parse_mode='HTML', 
            reply_markup=get_main_menu_keyboard(),
            connect_timeout=15, # Increased for Giphy stability
            read_timeout=15
        )
    except Exception as e:
        print(f"GIF Error: {e}")
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=f"<b>🍃 CLYDE'S RESOURCE HUB</b>\n\n{caption}",
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard()
        )

@app.route('/', methods=['GET', 'POST'])
@app.route('/api/index', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🍃 Online.", 200
    
    update_data = request.get_json(force=True)
    
    async def process():
        if not tg_app.bot_data: 
            await tg_app.initialize()
            await tg_app.start()

        update = Update.de_json(update_data, tg_app.bot)
        
        if update.message and update.message.text:
            text = update.message.text.lower()
            if text.startswith("/start") or text.startswith("/menu"):
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
        elif update.callback_query:
            await handle_callback(update)

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process())
        loop.close()
    except Exception as e:
        print(f"Webhook Error: {e}")
        
    return "OK", 200
