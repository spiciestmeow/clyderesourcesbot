import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

# --- VERCEL FLASK APP ---
app = Flask(__name__)

# ==================== CONFIGURATION ====================
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

LOGO_GIF = "https://media.giphy.com/media/cBKMTJGAE8y2Y/giphy.gif"

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
    """Initial welcome - only big Start button"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")]
    ])


def get_main_menu_keyboard():
    """Full Ghibli menu"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account"),
            InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")
        ],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/clydedigitals")]
    ])


# ==================== MESSAGES ====================
async def send_initial_welcome(chat_id, first_name):
    """First message when user types /start - shows only big button"""
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n"
        "🌿 <b>Welcome to Clyde's Enchanted Clearing</b>\n\n"
        "The gentle wind carries whispers from the ancient forest...\n"
        "Here lie hidden digital treasures and wonders waiting for you.\n\n"
        "<i>May the forest spirits guide your steps.</i> 🍃✨"
    )

    try:
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOGO_GIF,
            caption=caption,
            parse_mode='HTML',
            reply_markup=get_start_keyboard(),
            connect_timeout=30,
            read_timeout=30,
            write_timeout=30
        )
    except Exception as e:
        print(f"GIF failed: {e}")
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=f"<b>🌿 Clyde's Enchanted Clearing</b>\n\n{caption}",
            parse_mode='HTML',
            reply_markup=get_start_keyboard()
        )


async def send_main_menu(chat_id, first_name):
    """Full menu with all buttons"""
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(first_name)}</b>!\n\n"
        "🌿 <b>You have entered the Enchanted Clearing</b>\n\n"
        "Choose your path among the whispering trees...\n\n"
        "<i>May your journey be filled with magic and abundance.</i> 🍃✨"
    )

    try:
        await tg_app.bot.send_animation(
            chat_id=chat_id,
            animation=LOGO_GIF,
            caption=caption,
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard(),
            connect_timeout=30,
            read_timeout=30,
            write_timeout=30
        )
    except Exception as e:
        print(f"GIF failed: {e}")
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=f"<b>🌿 Clyde's Enchanted Clearing</b>\n\n{caption}",
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard()
        )


# ==================== CALLBACK HANDLER ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    if query.data == "show_main_menu":
        # User clicked the big start button → show full menu
        try:
            await query.message.delete()
        except:
            pass
        await send_main_menu(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "main_menu":
        # Return to full menu from anywhere
        try:
            await query.message.delete()
        except:
            pass
        await send_main_menu(update.effective_chat.id, update.effective_user.first_name)

    elif query.data == "check_vamt":
        try:
            await query.edit_message_caption(
                caption="🌬️ <i>The wind spirits are searching deep within the forest...</i>",
                parse_mode='HTML'
            )
        except:
            try:
                await query.edit_message_text(
                    text="🌬️ <i>The wind spirits are searching deep within the forest...</i>",
                    parse_mode='HTML'
                )
            except:
                pass

        data = await get_vamt_data()

        if data is None:
            try:
                await query.edit_message_caption(
                    caption="🌫️ The forest spirits lost their way... Please try again.",
                    reply_markup=get_main_menu_keyboard()
                )
            except:
                await tg_app.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="🌫️ The forest spirits lost their way... Please try again.",
                    reply_markup=get_main_menu_keyboard()
                )
            return

        # Inventory Content (Unchanged)
        report = "<b>🌿 CLYDE'S RESOURCE HUB INVENTORY</b>\n"
        report += "━━━━━━━━━━━━━━━━━━━━\n"
        for item in data:
            product = item.get('service_type', 'Product')
            count = item.get('remaining', 0)
            key = item.get('key_id', 'HIDDEN')
            name_l = str(product).lower()
            icon = "📑" if "office" in name_l else "🪟" if "win" in name_l else "📦"
            report += f"{icon} <b>{product}</b>\n└ 🔑 <code>{key}</code>\n└ 📦 Stock: <b>{count}</b>\n\n"

        report += f"━━━━━━━━━━━━━━━━━━━━\n<i>Last Sync: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🌿"

        back_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Return to the Enchanted Clearing", callback_data="main_menu")]
        ])

        try:
            await query.message.delete()
        except:
            pass

        try:
            await tg_app.bot.send_animation(
                chat_id=query.message.chat_id,
                animation=LOGO_GIF,
                caption=report,
                parse_mode='HTML',
                reply_markup=back_kb,
                connect_timeout=30,
                read_timeout=30,
                write_timeout=30
            )
        except Exception as e:
            print(f"GIF failed in stats: {e}")
            await tg_app.bot.send_message(
                chat_id=query.message.chat_id,
                text=report,
                parse_mode='HTML',
                reply_markup=back_kb
            )


# ==================== WEBHOOK ====================
@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🌿 Clyde's Enchanted Clearing is awake.", 200

    update_data = request.get_json(silent=True)
    if not update_data:
        return "OK", 200

    async def process_update():
        try:
            await tg_app.initialize()
            update = Update.de_json(update_data, tg_app.bot)

            if update.message and update.message.text:
                if update.message.text.lower().startswith("/start"):
                    await send_initial_welcome(
                        update.effective_chat.id,
                        update.effective_user.first_name
                    )

            elif update.callback_query:
                await handle_callback(update)

        except Exception as e:
            print(f"🔴 Error: {e}")

    try:
        loop.run_until_complete(process_update())
    except RuntimeError as e:
        if "Event loop is closed" not in str(e):
            print(f"Loop error: {e}")

    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
