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

# 1. Configuration (Ensure these are in Vercel Environment Variables)
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
        # Pulls your table data and sorts by the product name
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        return None

async def send_welcome_message(chat_id, first_name):
    """The Ghibli-themed welcome message for Clyde Tech Hub"""
    keyboard = [
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("📊 Check Key Status", callback_data="check_vamt")],
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
    """Updates message with categorized live VAMT data - Safe Version"""
    query = update.callback_query
    await query.answer("Consulting the forest spirits... 🍃")

    if query.data == "check_vamt":
        try:
            data = await get_vamt_data()
            
            if not data:
                await query.edit_message_caption(
                    caption="❌ No key data found in the clearing. Is the Pusher running?",
                    reply_markup=query.message.reply_markup
                )
                return

            report = "<b>📊 Clyde Tech Hub Inventory:</b>\n\n"
            
            for item in data:
                # Based on your Supabase Table screenshot:
                # service_type = Product Name (Windows 10 Pro)
                # remaining = Stock Count
                product_name = item.get('service_type', 'Unknown Product')
                count = item.get('remaining', 0)
                
                # Logic to detect Service Type icon/category
                name_lower = str(product_name).lower()
                if "office" in name_lower:
                    category, icon = "Microsoft Office", "📑"
                elif "win" in name_lower:
                    category, icon = "Windows OS", "🪟"
                else:
                    category, icon = "Software", "📦"
                
                # Formats the list: Hides the actual 'key_id' for security
                report += f"{icon} <b>[{category}]</b>\n"
                report += f"└ <code>{product_name}</code>: <b>{count}</b> left\n\n"
            
            report += f"<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🍃"

            await query.edit_message_caption(
                caption=report,
                parse_mode='HTML',
                reply_markup=query.message.reply_markup
            )

        except Exception as e:
            # If the database call fails, this prevents a 500 error and tells you why
            print(f"Error in Callback: {e}")
            await query.edit_message_caption(
                caption=f"⚠️ <b>Internal Hub Error:</b>\n<code>{str(e)}</code>",
                parse_mode='HTML',
                reply_markup=query.message.reply_markup
            )

@app.route('/api/index', methods=['POST'])
def webhook():
    """Vercel entry point"""
    try:
        data = request.get_json(force=True)
        
        async def handle_update():
            if not tg_app.bot_data:
                await tg_app.initialize()
            
            update = Update.de_json(data, tg_app.bot)
            
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            elif update.callback_query:
                await handle_callback(update)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle_update())
        loop.close()
        
        return "OK", 200
    except Exception as e:
        print(f"Webhook Error: {e}")
        return str(e), 500

@app.route('/')
def index():
    return "🍃 Clyde Tech Hub is floating in the wind..."
