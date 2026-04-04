import os
import asyncio
import html
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

# 1. Global Setup
app = Flask(__name__)
TOKEN = os.getenv("BOT_TOKEN")
LOGO_GIF = "https://media.giphy.com/media/cBKMTJGAE8y2Y/giphy.gif"

# Initialize TG Application
tg_app = Application.builder().token(TOKEN).build()

# 2. Keyboards
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

# 3. Logic
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
        # Initialize for the request
        await tg_app.initialize()
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOGO_GIF,
            caption=caption,
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard()
        )
        print(f"✅ GIF Welcome sent to {first_name}")
    except Exception as e:
        print(f"❌ GIF Failed, sending text: {e}")
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard()
        )

@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🍃 Clyde Hub is Rooted and Online.", 200

    try:
        update_data = request.get_json(force=True)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def process():
            await tg_app.initialize()
            update = Update.de_json(update_data, tg_app.bot)
            
            if update.message and update.message.text:
                if update.message.text.startswith("/start"):
                    await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
            
            elif update.callback_query:
                # Handle button clicks
                query = update.callback_query
                await query.answer()
                if query.data == "check_vamt":
                    # You can add your Supabase fetching logic here later
                    await query.message.reply_text("📊 Fetching stats from the vault...")

        loop.run_until_complete(process())
        loop.close()
    except Exception as e:
        print(f"⚠️ Webhook Error: {e}")

    return "OK", 200
