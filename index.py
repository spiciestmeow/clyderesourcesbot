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

# --- Helper Functions ---

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
        [InlineKeyboardButton("📊 Check Activation Key Stats", callback_data="check_vamt")],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("📞 Contact & Advertise", url="https://t.me/YOUR_USERNAME")]
    ])

async def send_welcome_message(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n"
        "<b>Welcome to the hidden clearing. This space is built to help "
        "you find the resources you need, simply and peacefully.</b>\n\n"
        "<b>Explore the paths below to begin. 🍃</b>"
    )
    GIF_URL = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExanJlb3NqOHlwNDNmbmtlMnZtc2NramxmOXMydnU0a3B4amN3YnBiZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"

    # Protecting this message from being screenshotted/forwarded
    await tg_app.bot.send_animation(
        chat_id=chat_id, animation=GIF_URL, caption=caption,
        parse_mode='HTML', reply_markup=get_main_menu_keyboard(),
        protect_content=True # THIS IS THE KEY SETTING
    )

async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    if query.data == "main_menu":
        # Delete original message and re-send to keep it protected
        await query.message.delete()
        await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "check_vamt":
        try:
            # First, indicate loading by editing the current message
            await query.edit_message_caption(
                caption="🔎 <i>Checking the inventory...</i>",
                parse_mode='HTML', reply_markup=query.message.reply_markup
            )

            data = await get_vamt_data()
            report = "<b>📊 Clyde's Resource Hub Inventory:</b>\n\n"
            
            for item in data:
                product = item.get('service_type', 'Unknown Product')
                count = item.get('remaining', 0)
                # Showing the ACTUAL KEY now as requested
                actual_key = item.get('key_id', 'No Key Found')
                
                name_lower = str(product).lower()
                icon = "📑" if "office" in name_lower else "🪟" if "win" in name_lower else "📦"

                report += f"{icon} <b>{product}</b>\n"
                report += f"└ 🔑 Key: <code>{actual_key}</code>\n"
                report += f"└ 📦 Stock: <b>{count}</b> left\n\n"
            
            report += f"<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🍃"
            back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")]])
            
            # Since you cannot change protect_content on an EDIT, 
            # we delete the old message and send a NEW one to activate protection.
            await query.message.delete()
            await tg_app.bot.send_animation(
                chat_id=update.effective_chat.id,
                animation="https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExanJlb3NqOHlwNDNmbmtlMnZtc2NramxmOXMydnU0a3B4amN3YnBiZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif",
                caption=report,
                parse_mode='HTML',
                reply_markup=back_kb,
                protect_content=True # PREVENTS SCREENSHOTS
            )

        except Exception as e:
            await query.message.reply_text(f"⚠️ Hub Error: <code>{str(e)}</code>", parse_mode='HTML')

@app.route('/', methods=['GET', 'POST'])
@app.route('/api/index', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🍃 Clyde's Resource Hub is online.", 200
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        update_data = request.get_json(force=True)
        async def process():
            if not tg_app.bot_data: await tg_app.initialize()
            update = Update.de_json(update_data, tg_app.bot)
            if update.message and update.message.text in ["/start", "/menu"]:
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            elif update.callback_query:
                await handle_callback(update)
        loop.run_until_complete(process())
        loop.close()
        return "OK", 200
    except: return "OK", 200
