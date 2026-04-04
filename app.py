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
WELCOME_GIF   = "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExeWZzOHRrYjRycTI4d2Z2eXR6mcbMm1yYXVqbzVrb3NmczB2ZHdmayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/wsKqNQmHYZfs4/giphy.gif"
MENU_GIF      = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExczJsZ25kM2N1N2twOHhmNWRsd3N6eWlyZ3N5M29pdmxsdDMzOHVscCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/cBKMTJGAE8y2Y/giphy.gif"
INVENTORY_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
ABOUT_GIF     = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdTFqMHB0ODVxdmFoMHl3dzZyM2swanlicmRibGk1bjdpcjFsdnl1biZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/x5HlLDaLMZNVS/giphy.gif"
HELP_GIF      = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNWxybTY5bXA0ejg1cGxxNTY3d3IyY3A4NGtkZ2gyOXkxcnlwZzN2NCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/J4FsxFgZgN2HS/giphy.gif"
LOADING_GIF   = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"

tg_app = Application.builder().token(TOKEN).build()

# ==================== DATABASE ====================
async def get_vamt_data():
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{SUPABASE_URL}/rest/v1/vamt_keys?select=*&order=service_type.asc"
        try:
            response = await client.get(url, headers=headers)
            return response.json()
        except Exception as e:
            print(f"🔴 Supabase Error: {e}")
            return None

# ==================== KEYBOARDS ====================
def get_start_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")]])

def get_full_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account"),
         InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="inventory_categories")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("❓ Help", callback_data="help"), InlineKeyboardButton("ℹ️ About", callback_data="about")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/caydigitals")]
    ])

def get_inventory_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪟 Windows Scrolls", callback_data="vamt_filter_win"),
         InlineKeyboardButton("📑 Office Scrolls", callback_data="vamt_filter_office")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to the Clearing", callback_data="main_menu")]])

# ==================== CALLBACKS ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    # 1. Main Menu Transition
    if query.data in ["show_main_menu", "main_menu"]:
        try: await query.message.delete()
        except: pass
        
        user_tz = pytz.timezone('Asia/Manila')
        current_hour = datetime.now(user_tz).hour
        time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
        greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"
        
        caption = f"{time_icon} {greeting}, <b>{html.escape(update.effective_user.first_name)}</b>!\n\n🌿 <b>You have entered the Enchanted Clearing</b>\n\nChoose your path beneath the whispering trees...\n\n<i>May your journey be filled with magic.</i> 🍃✨"
        await tg_app.bot.send_animation(update.effective_chat.id, MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())

    # 2. Inventory Category Selection
    elif query.data == "inventory_categories":
        try:
            await query.message.edit_caption(
                caption="🌿 <b>The Ancient Library</b>\n\nWhich digital scrolls do you wish to consult today, wanderer?",
                parse_mode='HTML',
                reply_markup=get_inventory_keyboard()
            )
        except:
            await query.message.reply_text("🌿 Choose your scrolls:", reply_markup=get_inventory_keyboard())

    # 3. Filtered Inventory Logic
    elif query.data.startswith("vamt_filter_"):
        category = query.data.replace("vamt_filter_", "") # "win" or "office"
        
        try: await query.message.delete()
        except: pass

        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption=f"✨ <i>The spirits are searching for {category.upper()} scrolls...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.2)
        data = await get_vamt_data()
        
        if not data:
            await loading_msg.edit_caption("🌫️ The mist has thickened... try again later.", parse_mode='HTML')
            return

        # Filtering Logic
        filtered_data = [item for item in data if category in str(item.get('service_type', '')).lower()]
        
        report = f"<b>📜 THE {category.upper()} SCROLLS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        if not filtered_data:
            report += "<i>Alas, these scrolls are currently hidden from view.</i>"
        else:
            for item in filtered_data:
                product = item.get('service_type', 'Product')
                count = item.get('remaining', 0)
                key = item.get('key_id', 'HIDDEN')
                icon = "🪟" if "win" in category else "📑"
                report += f"{icon} <b>{product}</b>\n└ 🔑 <code>{key}</code>\n└ 📦 Stock: <b>{count}</b>\n\n"

        report += f"━━━━━━━━━━━━━━━━━━━━\n<i>Revealed at: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🌿"

        try: await tg_app.bot.delete_message(loading_msg.chat_id, loading_msg.message_id)
        except: pass

        await tg_app.bot.send_animation(
            update.effective_chat.id,
            animation=INVENTORY_GIF,
            caption=report,
            parse_mode='HTML',
            reply_markup=get_back_keyboard(),
            protect_content=True
        )

    # 4. Lore & Guidance
    elif query.data == "about":
        text = "<b>🌿 About Clyde's Enchanted Clearing</b>\n\nA digital forest inspired by Studio Ghibli. We share treasures like Steam accounts and activation keys with care. 🍃✨"
        await tg_app.bot.send_animation(update.effective_chat.id, ABOUT_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

    elif query.data == "help":
        text = "<b>❓ Help & Guidance</b>\n\n🌿 <b>Navigation:</b> Use /menu to return.\n📜 <b>Copying:</b> Long-press the code box.\n🕊️ <b>Support:</b> Contact the Messenger of the Wind."
        await tg_app.bot.send_animation(update.effective_chat.id, HELP_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

# ==================== WEBHOOK ====================
@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        return "🍃 Clyde Resources Bot is Online.", 200

    update_data = request.get_json(silent=True)
    if not update_data:
        return "No data", 400

    async def process():
        # Setup bot for this request
        if not tg_app.bot_data:
            await tg_app.initialize()
            await tg_app.start()

        update = Update.de_json(update_data, tg_app.bot)

        if update.message and update.message.text:
            text = update.message.text.lower().strip()
            if text.startswith("/start"):
                await tg_app.bot.send_animation(update.effective_chat.id, WELCOME_GIF, caption="🌿 <b>Welcome traveler.</b>", parse_mode='HTML', reply_markup=get_start_keyboard())
            elif text.startswith("/menu"):
                await tg_app.bot.send_animation(update.effective_chat.id, MENU_GIF, caption="🌿 <b>The Clearing</b>", parse_mode='HTML', reply_markup=get_full_menu_keyboard())
        
        elif update.callback_query:
            await handle_callback(update)

    try:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(process())
        new_loop.close()
    except Exception as e:
        print(f"🔴 Error: {e}")
        
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
