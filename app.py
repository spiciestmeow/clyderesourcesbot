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

# Global application instance
tg_app = Application.builder().token(TOKEN).build()

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
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌿 Enter the Enchanted Clearing", callback_data="show_main_menu")]])

def get_full_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪄 Spirit Treasures", url="https://clyderesourcehub.short.gy/steam-account"),
         InlineKeyboardButton("📜 Ancient Scrolls", url="https://clyderesourcehub.short.gy/learn-and-guides")],
        [InlineKeyboardButton("🌿 Check Forest Inventory", callback_data="check_vamt")],
        [InlineKeyboardButton("🌲 The Whispering Forest", url="https://clyderesourcehub.short.gy/")],
        [InlineKeyboardButton("❓ Guidance", callback_data="help"),
         InlineKeyboardButton("ℹ️ Lore", callback_data="about")],
        [InlineKeyboardButton("🕊️ Messenger of the Wind", url="https://t.me/caydigitals")]
    ])

def get_inventory_categories():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪟 Windows Keys", callback_data="vamt_filter_win"),
         InlineKeyboardButton("📑 Office Keys", callback_data="vamt_filter_office")],
        [InlineKeyboardButton("⬅️ Back to Clearing", callback_data="main_menu")]
    ])

def get_back_to_inventory_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Scroll Selection", callback_data="check_vamt")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to the Clearing", callback_data="main_menu")]])

# ==================== MESSAGES ====================
async def send_initial_welcome(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"
    caption = f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n🌿 <b>Welcome to Clyde's Enchanted Clearing</b>\n\nThe gentle wind carries whispers from the ancient forest...\n\n<i>May the forest spirits watch over you.</i> 🍃✨"
    await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())

async def send_full_menu(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"
    caption = f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n🌿 <b>You have entered the Enchanted Clearing</b>\n\nChoose your path beneath the whispering trees... 🍃✨"
    await tg_app.bot.send_animation(chat_id=chat_id, animation=MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())

# ==================== CALLBACK HANDLER ====================
async def handle_callback(update: Update):
    query = update.callback_query
    
    # 🌿 STEP 1: Always answer immediately to stop the spinning circle
    try: await query.answer()
    except: pass

    # 🌟 MENU TRANSITION
    if query.data in ["show_main_menu", "main_menu"]:
        try:
            await query.message.edit_caption(caption="✨ <i>The mist begins to part...</i>", parse_mode='HTML', reply_markup=None)
            await asyncio.sleep(0.6)
            await query.message.delete()
        except: pass
        
        loading_msg = await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=LOADING_GIF, caption="🍃 <i>Guided by fireflies...</i>", parse_mode='HTML')
        await asyncio.sleep(1.0); await loading_msg.edit_caption(caption="🌲 <i>Path revealing...</i>", parse_mode='HTML')
        await asyncio.sleep(1.0); await loading_msg.edit_caption(caption="✨ <i>Arrived at the heart of the clearing.</i>", parse_mode='HTML')
        
        try: await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except: pass
        await send_full_menu(update.effective_chat.id, update.effective_user.first_name)

    # 🌟 INVENTORY CATEGORIES
    elif query.data == "check_vamt":
        try:
            await query.message.edit_caption(caption="🌿 <b>The Ancient Library</b>\n\nWhich digital scrolls do you seek?", parse_mode='HTML', reply_markup=get_inventory_categories())
        except: pass

    # 🌟 FILTERED KEY RESULTS
    elif query.data.startswith("vamt_filter_"):
        category = query.data.replace("vamt_filter_", "")
        try: await query.message.delete()
        except: pass

        loading_msg = await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=LOADING_GIF, caption=f"✨ <i>Searching for {category.upper()} scrolls...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2); await loading_msg.edit_caption(caption="🍃 <i>Counting hidden treasures...</i>", parse_mode='HTML')

        data = await get_vamt_data()
        filtered = [item for item in data if category in str(item.get('service_type', '')).lower()] if data else []
        
        report = f"<b>📜 THE {category.upper()} SCROLLS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        if not filtered: report += "<i>No scrolls found in this grove today.</i>"
        else:
            for item in filtered:
                report += f"✨ <b>{item.get('service_type')}</b>\n└ 🔑 <code>{item.get('key_id')}</code>\n└ 📦 Stock: <b>{item.get('remaining')}</b>\n\n"
        report += f"━━━━━━━━━━━━━━━━━━━━\n<i>Revealed: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i>"

        try: await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except: pass
        await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=INVENTORY_GIF, caption=report, parse_mode='HTML', reply_markup=get_back_to_inventory_keyboard(), protect_content=True)

    # 🌟 LORE (ABOUT)
    elif query.data == "about":
        try: await query.message.delete()
        except: pass
        loading_msg = await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=LOADING_GIF, caption="✨ <i>Consulting ancient records...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        text = "<b>🌿 About Clyde's Enchanted Clearing</b>\n\nA digital grove inspired by Ghibli. We share Steam accounts and keys with care. 🍃✨"
        try: await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except: pass
        await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=ABOUT_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

    # 🌟 GUIDANCE (HELP)
    elif query.data == "help":
        try: await query.message.delete()
        except: pass
        loading_msg = await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=LOADING_GIF, caption="✨ <i>Whispering to soot sprites...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        text = (
            "<b>❓ Guidance for the Wandering Soul</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
            "🌿 <b>Navigation:</b> Use <b>/menu</b> to return.\n"
            "📜 <b>Copying:</b> Long-press codes to copy.\n"
            "🕊️ <b>Support:</b> Message the Wind via the button below."
        )
        try: await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except: pass
        await tg_app.bot.send_animation(chat_id=update.effective_chat.id, animation=HELP_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

# ==================== WEBHOOK & LOOP CONTROL ====================
@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🍃 Online.", 200
    
    update_data = request.get_json(silent=True)
    if not update_data: return "No data", 400

    async def process():
        # Ensure bot is initialized for this thread/instance
        if not tg_app.bot_data:
            await tg_app.initialize()
            await tg_app.start()

        update = Update.de_json(update_data, tg_app.bot)
        
        # Handle Text
        if update.message and update.message.text:
            text = (update.message.text or "").lower().strip()
            chat_id = update.effective_chat.id
            name = update.effective_user.first_name if update.effective_user else "Traveler"
            if text.startswith("/start"): await send_initial_welcome(chat_id, name)
            elif text.startswith("/menu"): await send_full_menu(chat_id, name)
        
        # Handle Callback (Buttons)
        elif update.callback_query:
            await handle_callback(update)

    try:
        # Create and run a fresh loop to prevent 'loop closed' errors
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process())
        loop.close()
    except Exception as e:
        print(f"🔴 Webhook Error: {e}")
        
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
