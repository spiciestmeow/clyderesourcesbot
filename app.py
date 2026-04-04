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

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to the Clearing", callback_data="main_menu")]])

# ==================== CALLBACKS ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    # 🌟 MENU TRANSITION
    if query.data in ["show_main_menu", "main_menu"]:
        try:
            await query.message.edit_caption(caption="✨ <i>The mist begins to part...</i>", parse_mode='HTML', reply_markup=None)
            await asyncio.sleep(0.5)
            await query.message.delete()
        except: pass
        
        load_msg = await tg_app.bot.send_animation(update.effective_chat.id, LOADING_GIF, caption="🍃 <i>Guided by fireflies...</i>", parse_mode='HTML')
        await asyncio.sleep(0.8); await load_msg.edit_caption(caption="🌲 <i>Path revealing...</i>", parse_mode='HTML')
        await asyncio.sleep(0.6)
        
        try: await tg_app.bot.delete_message(load_msg.chat_id, load_msg.message_id)
        except: pass
        
        user_tz = pytz.timezone('Asia/Manila')
        current_hour = datetime.now(user_tz).hour
        time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
        greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"
        
        caption = f"{time_icon} {greeting}, <b>{html.escape(str(update.effective_user.first_name))}</b>!\n\n🌿 <b>You have entered the Enchanted Clearing</b>\n\nChoose your path beneath the whispering trees... 🍃✨"
        await tg_app.bot.send_animation(update.effective_chat.id, MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())

    # 🌟 INVENTORY
    elif query.data == "check_vamt":
        await query.message.edit_caption(caption="🌿 <b>The Ancient Library</b>\n\nWhich digital scrolls do you seek today, wanderer?", parse_mode='HTML', reply_markup=get_inventory_categories())

    # 🌟 FILTERED DATA
    elif query.data.startswith("vamt_filter_"):
        cat = query.data.replace("vamt_filter_", "")
        try: await query.message.delete()
        except: pass
        
        load_msg = await tg_app.bot.send_animation(update.effective_chat.id, LOADING_GIF, caption=f"✨ <i>Searching for {cat.upper()} scrolls...</i>", parse_mode='HTML')
        data = await get_vamt_data()
        filtered = [item for item in data if cat in str(item.get('service_type', '')).lower()] if data else []
        
        report = f"<b>📜 THE {cat.upper()} SCROLLS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        if not filtered: report += "<i>Alas, these scrolls are currently hidden.</i>"
        else:
            for item in filtered:
                report += f"✨ <b>{item.get('service_type')}</b>\n└ 🔑 <code>{item.get('key_id')}</code>\n└ 📦 Stock: <b>{item.get('remaining')}</b>\n\n"
        report += f"━━━━━━━━━━━━━━━━━━━━\n<i>Revealed at: {datetime.now(pytz.timezone('Asia/Manila')).strftime('%I:%M %p')}</i> 🌿"
        
        try: await tg_app.bot.delete_message(load_msg.chat_id, load_msg.message_id)
        except: pass
        await tg_app.bot.send_animation(update.effective_chat.id, INVENTORY_GIF, caption=report, parse_mode='HTML', reply_markup=get_back_keyboard(), protect_content=True)

    # 🌟 ABOUT (LORE)
    elif query.data == "about":
        try: await query.message.delete()
        except: pass
        text = "<b>🌿 About Clyde's Enchanted Clearing</b>\n\nA peaceful digital grove inspired by Ghibli magic. We share treasures with care. 🍃✨"
        await tg_app.bot.send_animation(update.effective_chat.id, ABOUT_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

    # 🌟 HELP (GUIDANCE)
    elif query.data == "help":
        try: await query.message.delete()
        except: pass
        text = (
            "<b>❓ Guidance for the Wandering Soul</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
            "🌿 <b>Navigation:</b> Use <b>/menu</b> to return.\n"
            "📜 <b>Copying:</b> Long-press blue codes to copy.\n"
            "🕊️ <b>Support:</b> Contact the Messenger of the Wind."
        )
        await tg_app.bot.send_animation(update.effective_chat.id, HELP_GIF, caption=text, parse_mode='HTML', reply_markup=get_back_keyboard())

# ==================== VERCEL WEBHOOK ====================
@app.route('/', methods=['POST', 'GET'])
def webhook():
    if request.method == 'GET': return "🍃 Clearing is Awake.", 200
    
    data = request.get_json(force=True, silent=True)
    if not data: return "OK", 200

    async def run():
        if not tg_app.bot_data:
            await tg_app.initialize()
            await tg_app.start()

        update = Update.de_json(data, tg_app.bot)
        if update.message and update.message.text:
            t = update.message.text.lower()
            cid = update.effective_chat.id
            if "/start" in t:
                await tg_app.bot.send_animation(cid, WELCOME_GIF, caption="🌿 <b>Welcome traveler.</b>", parse_mode='HTML', reply_markup=get_start_keyboard())
            elif "/menu" in t:
                await tg_app.bot.send_animation(cid, MENU_GIF, caption="🌿 <b>The Clearing</b>", parse_mode='HTML', reply_markup=get_full_menu_keyboard())
        elif update.callback_query:
            await handle_callback(update)

    try:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(run())
        new_loop.close()
    except Exception as e: print(f"Error: {e}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
