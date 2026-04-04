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

# 1. Configuration
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

tg_app = Application.builder().token(TOKEN).build()

# Static Image URL (more stable than GIFs for webhooks)
STABLE_IMG = "https://i.imgur.com/8N8yQ9T.jpeg" 

async def get_vamt_data():
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

def get_main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("📊 Check Key Status", callback_data="check_vamt")],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("📞 Contact & Advertise", url="https://t.me/clydedigital")]
    ])

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

    # Using send_photo for better stability across all Telegram clients
    await tg_app.bot.send_photo(
        chat_id=chat_id, photo=STABLE_IMG, caption=caption,
        parse_mode='HTML', reply_markup=get_main_menu_keyboard(),
        protect_content=True 
    )

async def handle_callback(update: Update):
    query = update.callback_query
    user_name = html.escape(update.effective_user.first_name)
    await query.answer()

    # Clear current message
    try:
        await query.message.delete()
    except:
        pass

    if query.data == "main_menu":
        await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "check_vamt":
        try:
            data = await get_vamt_data()
            report = f"📜 <b>Hello {user_name}, here is today's latest update:</b>\n\n"
            
            for item in data:
                product = item.get('service_type', 'Unknown Product')
                count = item.get('remaining', 0)
                actual_key = item.get('key_id', 'No Key Found')
                
                name_lower = str(product).lower()
                icon = "📑" if "office" in name_lower else "🪟" if "win" in name_lower else "📦"

                report += f"{icon} <b>{product}</b>\n"
                report += f"└ 🔑 Key: <code>{actual_key}</code>\n"
                report += f"└ 📦 Stock: <b>{count}</b> left\n\n"
            
            report += f"<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🌿"
            
            back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to Clearing", callback_data="main_menu")]])

            await tg_app.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=STABLE_IMG,
                caption=report,
                parse_mode='HTML',
                reply_markup=back_kb,
                protect_content=True
            )

        except Exception as e:
            # Fallback text if the image fails again
            await tg_app.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"📜 <b>The mist is too thick, {user_name}...</b>\n\n{report if 'report' in locals() else 'Scroll could not be read.'}\n\n⚠️ Error: <code>{str(e)}</code>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]])
            )

@app.route('/', methods=['GET', 'POST'])
@app.route('/api/index', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🍃 Clyde Tech Hub is online.", 200
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        update_data = request.get_json(force=True)
        async def process():
            if not tg_app.bot_data: await tg_app.initialize()
            update = Update.de_json(update_data, tg_app.bot)
            
            if update.message:
                # Delete user commands and previous bot messages (Clean Reset)
                msg_id = update.message.message_id
                for i in range(5): # Attempts to clear the last 5 messages for a fresh start
                    try:
                        await tg_app.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_id - i)
                    except:
                        continue
                
                if update.message.text in ["/start", "/menu"]:
                    await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            
            elif update.callback_query:
                await handle_callback(update)
        loop.run_until_complete(process())
        loop.close()
        return "OK", 200
    except: return "OK", 200
