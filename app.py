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

# ==================== CONFIG ====================
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ==================== GIFS ====================
WELCOME_GIF   = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExeWZzOHRrYjRycTI4d2Z2eXR6bWNiMm1yYXVqbzVrb3NmczB2ZHdmayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/wsKqNQmHYZfs4/giphy.gif"
MENU_GIF      = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExczJsZ25kM2N1N2twOHhmNWRsd3N6eWlyZ3N5M29pdmxsdDMzOHVscCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"
INVENTORY_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
ABOUT_GIF     = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdTFqMHB0ODVxdmFoMHl3dzZyM2swanlicmRibGk1bjdpcjFsdnl1biZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/x5HlLDaLMZNVS/giphy.gif"
HELP_GIF      = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNWxybTY5bXA0ejg1cGxxNTY3d3IyY3A4NGtkZ2gyOXkxcnlwZzN2NCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/J4FsxFgZgN2HS/giphy.gif"

# 🌿 NEW LOADING GIF
LOADING_GIF   = "https://i.pinimg.com/originals/37/22/13/372213d574636096620bbf68f92e4efb.gif"

tg_app = Application.builder().token(TOKEN).build()
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


# ==================== DATABASE ====================
async def get_vamt_data():
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"🔴 Supabase Error: {e}")
            return None


# ==================== KEYBOARDS ====================
def get_start_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")]
    ])


def get_full_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [
            InlineKeyboardButton("❓ Help", callback_data="help"),
            InlineKeyboardButton("ℹ️ About", callback_data="about")
        ],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/caydigitals")]
    ])


def get_back_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Return to the Enchanted Clearing", callback_data="main_menu")]
    ])


# ==================== MESSAGES ====================
async def send_initial_welcome(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n🌿 <b>Welcome to Clyde's Enchanted Clearing</b>\n\nThe gentle wind carries whispers from the ancient forest...\nHidden treasures and digital wonders await kind-hearted wanderers.\n\n<i>May the forest spirits watch over you.</i> 🍃✨"

    await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())


async def send_full_menu(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n🌿 <b>You have entered the Enchanted Clearing</b>\n\nChoose your path beneath the whispering trees...\n\n<i>May your journey be filled with magic and abundance.</i> 🍃✨"

    await tg_app.bot.send_animation(chat_id=chat_id, animation=MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())

async def send_about(chat_id):
    text = (
        "<b>🌿 About Clyde's Enchanted Clearing</b>\n\n"
        "This is a peaceful digital forest inspired by the magic of Studio Ghibli.\n\n"
        "We gather and share useful digital treasures such as Steam accounts, "
        "learning guides, Windows & Office activation keys — all with care and good spirit.\n\n"
        "<i>May this small corner of the internet bring you joy and usefulness.</i> 🍃✨"
    )
    try:
        await tg_app.bot.send_animation(chat_id=chat_id, animation=ABOUT_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())
    except:
        await tg_app.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', reply_markup=get_back_keyboard())


async def send_help(chat_id):
    text = (
        "<b>❓ Help - How to Use This Bot</b>\n\n"
        "🌿 <b>Navigation:</b>\n"
        "• Tap <b>🌿 Enter the Enchanted Clearing</b> to begin\n"
        "• Use <b>/menu</b> anytime to open the full menu\n\n"
        "📋 <b>How to get activation keys:</b>\n"
        "1. Go to <b>🌿 Check Forest Inventory</b>\n"
        "2. Long-press the key (in the blue code box)\n"
        "3. Tap <b>Copy</b>\n\n"
        "🕊️ <b>Contact:</b>\n"
        "Use <b>Messenger of the Wind</b> to reach us\n\n"
        "<i>May the forest spirits guide your path.</i> 🍃✨"
    )
    try:
        await tg_app.bot.send_animation(chat_id=chat_id, animation=HELP_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())
    except:
        await tg_app.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', reply_markup=get_back_keyboard())



# ==================== CALLBACK ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    try:
        await query.message.delete()
    except:
        pass

    if query.data in ["show_main_menu", "main_menu"]:
        await send_full_menu(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "check_vamt":

        # 🌿 STEP 1: SEND LOADING GIF
        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption="🌿 <b>The forest spirits awaken...</b>",
            parse_mode='HTML'
        )

        # 🌿 STEP 2: FAKE PROGRESS (IMMERSION)
        await asyncio.sleep(1.5)
        await tg_app.bot.edit_message_caption(
            chat_id=loading_msg.chat_id,
            message_id=loading_msg.message_id,
            caption="🍃 The trees whisper... counting hidden treasures...",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.5)
        await tg_app.bot.edit_message_caption(
            chat_id=loading_msg.chat_id,
            message_id=loading_msg.message_id,
            caption="✨ Ancient magic is revealing the inventory...",
            parse_mode='HTML'
        )

        # 🌿 STEP 3: FETCH DATA
        data = await get_vamt_data()

        if not data:
            await tg_app.bot.edit_message_caption(
                chat_id=loading_msg.chat_id,
                message_id=loading_msg.message_id,
                caption="🌫️ The forest spirits lost their way...",
                parse_mode='HTML'
            )
            return

        # 🌿 STEP 4: BUILD REPORT
        report = "<b>🌿 CLYDE'S RESOURCE HUB INVENTORY</b>\n━━━━━━━━━━━━━━━━━━━━\n"

        for item in data:
            product = item.get('service_type', 'Product')
            count = item.get('remaining', 0)
            key = item.get('key_id', 'HIDDEN')
            name_l = str(product).lower()
            icon = "📑" if "office" in name_l else "🪟" if "win" in name_l else "📦"

            report += f"{icon} <b>{product}</b>\n└ 🔑 <code>{key}</code>\n└ 📦 Stock: <b>{count}</b>\n\n"

        report += f"━━━━━━━━━━━━━━━━━━━━\n<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🌿"

        # 🌿 STEP 5: DELETE LOADING
        await tg_app.bot.delete_message(
            chat_id=loading_msg.chat_id,
            message_id=loading_msg.message_id
        )

        # 🌿 STEP 6: SHOW RESULT
        await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=INVENTORY_GIF,
            caption=report,
            parse_mode='HTML',
            reply_markup=get_back_keyboard(),
            protect_content=True
        )

    elif query.data == "about":
        await send_about(update.effective_chat.id)

    elif query.data == "help":
        await send_help(update.effective_chat.id)


# ==================== WEBHOOK ====================
@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🌿 Clyde's Enchanted Clearing is awake.", 200

    update_data = request.get_json(silent=True)

    async def process_update():
        await tg_app.initialize()
        update = Update.de_json(update_data, tg_app.bot)

        if update.message and update.message.text:
            text = update.message.text.lower().strip()
            if text.startswith("/start"):
                await send_initial_welcome(update.effective_chat.id, update.effective_user.first_name)
            elif text.startswith("/menu"):
                await send_full_menu(update.effective_chat.id, update.effective_user.first_name)

        elif update.callback_query:
            await handle_callback(update)

    loop.run_until_complete(process_update())
    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
