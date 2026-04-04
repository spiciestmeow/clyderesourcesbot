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

# 1. Configuration (Set these in Vercel Environment Variables)
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
    async with httpx.AsyncClient(timeout=15.0) as client:
        # Pulls data from your 'vamt_keys' table
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

async def send_welcome_message(chat_id, first_name):
    """The Ghibli-themed welcome message"""
    keyboard = [
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("📊 Check Key Status", callback_data="check_vamt")],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")]
    ]
    
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n"
        "<b>Welcome to the hidden clearing. Explore the paths below to begin. 🍃</b>"
    )

    GIF_URL = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExanJlb3NqOHlwNDNmbmtlMnZtc2NramxmOXMydnU0a3B4amN3YnBiZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"

    await tg_app.bot.send_animation(
        chat_id=chat_id, animation=GIF_URL, caption=caption,
        parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_callback(update: Update):
    """Updates message with categorized live data from your Supabase table"""
    query = update.callback_query
    
    # ANSWER IMMEDIATELY to prevent Telegram timeouts
    await query.answer("Fetching from the forest... 🍃")

    if query.data == "check_vamt":
        try:
            # Inform user of sync progress
            await query.edit_message_caption(
                caption="🔎 <i>Consulting the scrolls...</i>",
                parse_mode='HTML', reply_markup=query.message.reply_markup
            )

            data = await get_vamt_data()
            if not data:
                await query.edit_message_caption(caption="❌ Empty clearing. No data found.", reply_markup=query.message.reply_markup)
                return

            report = "<b>📊 Clyde Tech Hub Inventory:</b>\n\n"
            for item in data:
                # Using your specific column names: service_type and remaining
                product = item.get('service_type', 'Unknown Product')
                count = item.get('remaining', 0)
                
                name_lower = str(product).lower()
                icon = "📑" if "office" in name_lower else "🪟" if "win" in name_lower else "📦"
                cat = "Office" if "office" in name_lower else "Windows" if "win" in name_lower else "Software"

                report += f"{icon} <b>[{cat}]</b>\n└ <code>{product}</code>: <b>{count}</b> left\n\n"
            
            report += f"<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🍃"
            await query.edit_message_caption(caption=report, parse_mode='HTML', reply_markup=query.message.reply_markup)

        except Exception as e:
            # Provide error feedback in Telegram for easier debugging
            await query.edit_message_caption(
                caption=f"⚠️ <b>Hub Error:</b>\n<code>{str(e)}</code>", 
                parse_mode='HTML', reply_markup=query.message.reply_markup
            )

@app.route('/api/index', methods=['GET', 'POST'])
@app.route('/', methods=['GET', 'POST'])
def webhook():
    """Optimized Entry Point for Vercel"""
    # Fix for 'Method Not Allowed' browser error
    if request.method == 'GET':
        return "🍃 Clyde Tech Hub is online and floating in the wind..."
        
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        update_data = request.get_json(force=True)
        
        async def process():
            if not tg_app.bot_data:
                await tg_app.initialize()
            update = Update.de_json(update_data, tg_app.bot)
            
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            elif update.callback_query:
                await handle_callback(update)

        loop.run_until_complete(process())
        loop.close()
        return "OK", 200
    except Exception as e:
        print(f"Server Error: {e}")
        return "OK", 200 # Return 200 to keep Telegram quiet
