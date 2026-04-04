import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

# --- VERCEL GLOBAL INSTANCE ---
app = Flask(__name__)

# 1. Configuration
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# Using a Giphy link can be slow; consider using a Telegram File ID later.
LOGO_GIF = "https://media.giphy.com/media/cBKMTJGAE8y2Y/giphy.gif"

tg_app = Application.builder().token(TOKEN).build()

# 2. Database Fetcher
async def get_vamt_data():
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"🔴 Supabase Error: {e}")
            return None

# 3. Keyboards
def get_main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎮 Steam Accs", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("🛠️ Digital Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("📊 Check Activation Key Stats", callback_data="check_vamt")],
        [InlineKeyboardButton("🍃 The Digital Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("📞 Contact & Advertise", url="https://t.me/clydedigitals")]
    ])

# 4. Message Handlers
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
            connect_timeout=20
        )
    except Exception as e:
        print(f"GIF failed: {e}")
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=f"<b>🍃 CLYDE'S RESOURCE HUB</b>\n\n{caption}",
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard()
        )

async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    if query.data == "main_menu":
        # Delete old message to keep chat clean and send fresh welcome
        try: await query.message.delete()
        except: pass
        await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "check_vamt":
        # Show loading state
        try:
            await query.edit_message_caption(caption="📜 <i>Searching the thicket for scrolls...</i>", parse_mode='HTML')
        except:
            await query.edit_message_text(text="📜 <i>Searching the thicket for scrolls...</i>", parse_mode='HTML')

        data = await get_vamt_data()
        if data is None:
            await query.edit_message_caption(caption="⚠️ Connection lost. Try again.", reply_markup=get_main_menu_keyboard())
            return

        report = "<b>🍃 CLYDE'S RESOURCE HUB INVENTORY</b>\n"
        report += "━━━━━━━━━━━━━━━━━━━━\n"
        for item in data:
            product = item.get('service_type', 'Product')
            count = item.get('remaining', 0)
            key = item.get('key_id', 'HIDDEN')
            name_l = str(product).lower()
            icon = "📑" if "office" in name_l else "🪟" if "win" in name_l else "📦"
            report += f"{icon} <b>{product}</b>\n└ 🔑 <code>{key}</code>\n└ 📦 Stock: <b>{count}</b>\n\n"
        
        report += f"━━━━━━━━━━━━━━━━━━━━\n<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🌿"
        
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to Clearing", callback_data="main_menu")]])
        
        try:
            await query.edit_message_caption(caption=report, parse_mode='HTML', reply_markup=back_kb)
        except:
            await query.edit_message_text(text=report, parse_mode='HTML', reply_markup=back_kb)

# 5. Webhook Entry Point
@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🍃 Clyde Hub is Rooted and Online.", 200

    update_data = request.get_json(force=True)
    
    async def process_update():
        await tg_app.initialize()
        update = Update.de_json(update_data, tg_app.bot)
        
        if update.message and update.message.text:
            if update.message.text.lower().startswith("/start"):
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
        
        elif update.callback_query:
            await handle_callback(update)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(process_update())
    finally:
        loop.close()

    return "OK", 200
