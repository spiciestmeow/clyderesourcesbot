import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

# { chat_id: [msg_id1, msg_id2, ...] }
forest_memory = {}
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
LOADING_GIF   = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"
MYID_GIF = "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ29vdXY3cW1uOWkyajNkcHN2bXM5OTJ3dDNzejBzZnViNnRobDE2OSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/ym6PmLonLGfv2/giphy.gif"
CLEAN_GIF   = "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeXkxbmR2bjF1bXdpd2Y1eDI5OWgzcmNxeGRnOHVqdmQ1bHN2ZTlxOCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/VGACXbkf0AeGs/giphy.gif"


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
         InlineKeyboardButton("📑 Office Keys", callback_data="vamt_filter_office")
        ],
        [InlineKeyboardButton("🍿 Netflix Keys", callback_data="vamt_filter_netflix")],
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

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
        "🌿 <b>Welcome, dear wanderer, to Clyde's Enchanted Clearing</b>\n\n"
        "The ancient wind carries soft whispers through the leaves...\n"
        "Hidden wonders and gentle magic await those with kind hearts.\n\n"
        "<i>May the forest spirits walk beside you on your journey.</i> 🍃✨"
    )

    msg = await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())
    if chat_id not in forest_memory: forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

async def send_full_menu(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"

    caption = (
        f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n"
        "🌿 <b>You have stepped into the heart of the Enchanted Clearing</b>\n\n"
        "Beneath the whispering ancient trees, many paths lie before you...\n"
        "Choose with care, kind wanderer.\n\n"
        "<i>May your steps be guided by gentle forest magic.</i> 🍃✨"
    )

    msg = await tg_app.bot.send_animation(chat_id=chat_id, animation=MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())
    
    if chat_id not in forest_memory: forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

async def send_myid(chat_id):
    caption_text = (
        "🌿 <b>Forest Spirit Identification</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "✨ <i>The mist clears to reveal your true essence...</i>\n\n"
        f"🆔 <b>User ID:</b> <code>{chat_id}</code>\n\n"
        "🍃 <i>Safe travels through the clearing, wanderer.</i>"
    )
    
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=MYID_GIF,
        caption=caption_text,
        parse_mode="HTML"
    )
    
    if chat_id not in forest_memory: 
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

# ==================== FEEDBACK COMMAND ======================
async def handle_feedback(chat_id, first_name, feedback_text):
    # Get current time in Philippines timezone for display
    user_tz = pytz.timezone('Asia/Manila')
    timestamp = datetime.now(user_tz).strftime("%B %d, %Y • %I:%M %p")

    # === Save Feedback to Supabase ===
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    payload = {
        "chat_id": int(chat_id),      # ensure it's integer
        "first_name": str(first_name),
        "feedback_text": feedback_text.strip()
    }

    saved = False
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(
                f"{SUPABASE_URL}/rest/v1/feedback",
                headers=headers,
                json=payload
            )
            
            if response.status_code in (200, 201):
                print(f"✅ Feedback saved to Supabase | User: {chat_id}")
                saved = True
            else:
                print(f"⚠️ Supabase insert failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"🔴 Supabase Error while saving feedback: {e}")

    # === Thank you message to user (timestamp at bottom) ===
    thank_you = (
        "🕊️ <b>A Message Carried by the Wind</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Dear <b>{html.escape(str(first_name))}</b>,\n\n"
        "The gentle breeze has carried your words through the ancient trees...\n"
        "They have reached the caretaker of the Enchanted Clearing.\n\n"
        "Thank you for sharing your thoughts with this small, magical corner of the forest.\n\n"
        "<i>May your voice help the clearing bloom even brighter.</i> 🍃✨\n\n"
        f"🕒 <b>Sent:</b> {timestamp}"
    )

    await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=HELP_GIF,
        caption=thank_you,
        parse_mode='HTML'
    )

    # === Notification to owner (timestamp at bottom) ===
    status = "✅ Saved to database" if saved else "⚠️ Failed to save to database"

    owner_message = (
        f"🌿 <b>New Feedback Received from the Forest</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 From: <b>{first_name}</b>\n"
        f"🆔 User ID: <code>{chat_id}</code>\n\n"
        f"💬 <b>Message:</b>\n{feedback_text}\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🕒 <b>Received:</b> {timestamp}\n"
        f"💾 <b>Status:</b> {status}"
    )

    try:
        await tg_app.bot.send_message(
            chat_id=7399488750,
            text=owner_message,
            parse_mode='HTML'
        )
    except Exception as e:
        print(f"Failed to send feedback to owner: {e}")

# --- CLEAR FUNCTION ---
async def handle_clear(chat_id, user_command_id):
    try: 
        await tg_app.bot.delete_message(chat_id=chat_id, message_id=user_command_id)
    except: 
        pass

    if chat_id in forest_memory:
        for msg_id in forest_memory[chat_id]:
            try: 
                await tg_app.bot.send_chat_action(chat_id, "typing")
                await tg_app.bot.delete_message(chat_id, msg_id)
            except: 
                pass
        forest_memory[chat_id] = []

    sent_msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=LOADING_GIF,
        caption="🍃 <b>The Forest Mist Clears...</b>\n\nYour path is now fresh and open.",
        parse_mode="HTML",
        reply_markup=get_start_keyboard()
    )
    
    forest_memory[chat_id].append(sent_msg.message_id)
    
# ==================== CALLBACK ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    # ====================== MAIN MENU & CLEARING ======================
    if query.data in ["show_main_menu", "main_menu"]:
        try:
            await query.message.delete()
        except:
            pass

        await asyncio.sleep(0.8)

        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption="🌫️ <i>The ancient mist begins to lift once more...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.3)
        await loading_msg.edit_caption("🌿 <i>The whispering trees lean in to welcome you home...</i>", parse_mode='HTML')

        await asyncio.sleep(1.3)
        await loading_msg.edit_caption("✨ <i>You stand again in the heart of the Enchanted Clearing...</i>", parse_mode='HTML')

        await asyncio.sleep(1.0)

        await send_full_menu(update.effective_chat.id, update.effective_user.first_name)

        try:
            await tg_app.bot.delete_message(loading_msg.chat_id, loading_msg.message_id)
        except:
            pass

    elif query.data == "check_vamt":
        await query.message.edit_caption(
            caption="📜 <i>The doors of the Ancient Library creak open...</i>\n\n"
                    "Which scrolls call to your heart today, wanderer?\n\n"
                    "<i>The forest spirits await your choice.</i>", 
            parse_mode='HTML', 
            reply_markup=get_inventory_categories()
        )

    # ====================== FILTERED INVENTORY ======================
    elif query.data.startswith("vamt_filter_") or query.data.startswith("vamt_all_"):
        is_full_view = query.data.startswith("vamt_all_")
        category = query.data.replace("vamt_filter_", "").replace("vamt_all_", "").lower()

        loading_text = "📜 <i>Unrolling the ancient scroll...</i>" if is_full_view else f"✨ <i>Searching the glade for {category.upper()}...</i>"
        await query.message.edit_caption(caption=loading_text, parse_mode='HTML')

        data = await get_vamt_data()
        
        if not data:
            await query.message.edit_caption(
                caption="🌫️ <i>The mist is too thick... Database connection failed.</i>",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        filtered = []
        for item in data:
            s_type = str(item.get('service_type', '')).lower().strip()

            if category == "netflix":
                if "netflix" in s_type:
                    filtered.append(item)
            elif category == "win":
                if any(x in s_type for x in ["windows", "win"]):
                    filtered.append(item)
            elif category == "office":
                if "office" in s_type:
                    filtered.append(item)
            else:
                if category in s_type:
                    filtered.append(item)

        if not filtered:
            await query.message.edit_caption(
                caption=f"🍃 <i>No {category.upper()} scrolls found in the clearing right now.</i>",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        # ====================== NETFLIX ======================
        if category == "netflix":
            report = (
                "<b>🍿 Secret Netflix Cookies of the Forest</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>{len(filtered)} Cookies Resting in the Glade</b>\n\n"
                "<i>Which one whispers to your spirit?</i>\n\n"
            )

            buttons = []
            for idx, item in enumerate(filtered, 1):
                display_name = str(item.get('display_name') or '').strip() or f"Forest Cookie {idx}"
                status_text = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"

                report += f"✨ <b>{display_name}</b>\n"
                report += f"   Status: {status_text}\n"
                report += f"   Remaining: {item.get('remaining', 0)}\n\n"

                buttons.append([
                    InlineKeyboardButton(f"🔓 Reveal {display_name}", callback_data=f"reveal_nf|{idx}")
                ])

            buttons.append([InlineKeyboardButton("⬅️ Back to the Clearing", callback_data="check_vamt")])

            kb = InlineKeyboardMarkup(buttons)

            await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)
            return

        # ====================== WINDOWS & OFFICE ======================
        limit = len(filtered) if is_full_view else 3
        report = f"<b>📜 {category.upper()} Scrolls</b>\n━━━━━━━━━━━━━━━━━━\n\n"

        for item in filtered[:limit]:
            product = item.get('service_type', 'Unknown')
            key = item.get('key_id', 'HIDDEN')
            raw_val = int(item.get('remaining') or 0)
            stock_text = f"{raw_val}" if raw_val > 0 else "Out of stock"

            report += f"✨ <b>{product}</b>\n└ 🔑 <code>{key}</code>\n└ 📦 Stock: <b>{stock_text}</b>\n\n"

        if not is_full_view and len(filtered) > 3:
            report += f"━━━━━━━━━━━━━━━━━━\n<i>... and {len(filtered) - 3} more scrolls hidden in the shadows.</i>"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📜 Show All", callback_data=f"vamt_all_{category}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="check_vamt")]
            ])
        else:
            kb = get_back_to_inventory_keyboard()

        await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)

    # ====================== REVEAL NETFLIX ======================
    elif query.data.startswith("reveal_nf|"):
        try:
            idx = int(query.data.split("|", 1)[1])
        except:
            await query.answer("Invalid selection", show_alert=True)
            return

        data = await get_vamt_data()
        if not data:
            await query.answer("Database error", show_alert=True)
            return

        netflix_items = [item for item in data if "netflix" in str(item.get('service_type', '')).lower()]
        netflix_items.sort(key=lambda x: (str(x.get('display_name') or ''), str(x.get('last_updated') or '')))

        if idx < 1 or idx > len(netflix_items):
            await query.answer(f"❌ Cookie #{idx} not found", show_alert=True)
            return

        item = netflix_items[idx - 1]
        cookie = str(item.get('key_id', '')).strip()
        display_name = str(item.get('display_name') or '').strip() or f"Forest Cookie {idx}"

        status = "✅ Awakened" if str(item.get('status', '')).lower() == "active" else "⚠️ Resting"

        report = (
            f"<b>🍿 {display_name} Revealed</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"🌿 Status: <b>{status}</b>\n"
            f"📦 Remaining: <b>{item.get('remaining', 0)}</b>\n\n"
            "<b>📋 The Hidden Cookie:</b>\n"
            f"<code>{html.escape(cookie[:800])}</code>\n\n"
            "<i>Long-press the code above to copy.\nUse it quickly before the magic fades.</i> 🍃"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Netflix Cookies", callback_data="vamt_filter_netflix")]
        ])

        await query.message.edit_caption(caption=report, parse_mode='HTML', reply_markup=kb)

    # ====================== ABOUT (Lore) ======================
    elif query.data == "about":
        try: await query.message.delete()
        except: pass

        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption="🌌 <i>The oldest spirits of the forest begin to stir...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.0)
        await loading_msg.edit_caption("📜 <i>They gather to share the forgotten tale of this clearing...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        await loading_msg.edit_caption("✨ <i>The story of the Enchanted Clearing gently unfolds...</i>", parse_mode='HTML')
        await asyncio.sleep(0.8)

        text = (
            "<b>🌿 About Clyde's Enchanted Clearing</b>\n\n"
            "This is a peaceful digital forest inspired by the magic of Studio Ghibli.\n\n"
            "We gather digital treasures like Steam accounts, learning guides, and activation keys.\n\n"
            "<i>May this small corner bring you joy and wonder.</i> 🍃✨"
        )

        final_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=ABOUT_GIF,
            caption=text,
            parse_mode='HTML',
            reply_markup=get_back_keyboard()
        )

        try: await tg_app.bot.delete_message(loading_msg.chat_id, loading_msg.message_id)
        except: pass

        chat_id = update.effective_chat.id
        if chat_id not in forest_memory: forest_memory[chat_id] = []
        forest_memory[chat_id].append(final_msg.message_id)

    # ====================== HELP (Guidance) ======================
    elif query.data == "help":
        try: await query.message.delete()
        except: pass

        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption="🪶 <i>The wind carries soft voices from the depths of the forest...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.0)
        await loading_msg.edit_caption("🍃 <i>The forest guides gather their ancient wisdom...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        await loading_msg.edit_caption("🌟 <i>The path of guidance is now revealed before you...</i>", parse_mode='HTML')
        await asyncio.sleep(0.8)

        text = (
            "<b>❓ Guidance - How to Use the Enchanted Clearing</b>\n\n"
            "🌿 <b>Basic Navigation:</b>\n"
            "• Tap any button to move through the forest.\n"
            "• Use <b>/menu</b> to return to the main clearing anytime.\n\n"
            
            "📜 <b>Available Commands:</b>\n"
            "• <code>/start</code> — Begin your journey anew\n"
            "• <code>/menu</code> — Return to the Enchanted Clearing\n"
            "• <code>/myid</code> — Reveal your forest spirit ID\n"
            "• <code>/clear</code> — Clean the chat and start fresh\n"
            "• <code>/feedback</code> — Send your thoughts to the caretaker\n\n"
            
            "🌲 <b>Main Features:</b>\n"
            "• 🪄 Spirit Treasures → Steam accounts\n"
            "• 📜 Ancient Scrolls → Learning guides\n"
            "• 🌿 Forest Inventory → Windows, Office & Netflix keys\n"
            "• 🌲 The Whispering Forest → Main resource hub\n"
            "• ℹ️ Lore → The story of this clearing\n"
            "• 🕊️ Messenger → Contact the caretaker directly\n\n"
            
            "<i>May these paths guide you well, kind wanderer.</i> 🍃✨"
        )

        final_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=HELP_GIF,
            caption=text,
            parse_mode='HTML',
            reply_markup=get_back_keyboard()
        )

        try: await tg_app.bot.delete_message(loading_msg.chat_id, loading_msg.message_id)
        except: pass

        chat_id = update.effective_chat.id
        if chat_id not in forest_memory: forest_memory[chat_id] = []
        forest_memory[chat_id].append(final_msg.message_id)
        
# ==================== WEBHOOK ====================
async def start_tg_app():
    await tg_app.initialize()
    await tg_app.start()

loop.run_until_complete(start_tg_app())

@app.route('/', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🌿 Clyde's Enchanted Clearing is awake.", 200
    update_data = request.get_json(silent=True)
    if not update_data: return "No data", 400

    async def process_update():
        update = Update.de_json(update_data, tg_app.bot)
        if update.message and update.message.text:
            text = update.message.text.lower().strip()
            chat_id = update.effective_chat.id
            user_msg_id = update.message.message_id
            name = update.effective_user.first_name if update.effective_user else "Traveler"
            if chat_id not in forest_memory: forest_memory[chat_id] = []
            forest_memory[chat_id].append(user_msg_id)
        
            if text.startswith("/start"): 
                await send_initial_welcome(chat_id, name)
            elif text.startswith("/menu"): 
                await send_full_menu(chat_id, name)
            elif text.startswith("/myid"): 
                await send_myid(chat_id)
            elif text.startswith("/clear"): 
                await handle_clear(chat_id, user_msg_id)
            elif text.startswith("/feedback"):
                feedback_text = text.replace("/feedback", "").strip()
                if feedback_text:
                    await handle_feedback(chat_id, name, feedback_text)
                else:
                    await tg_app.bot.send_message(
                        chat_id=chat_id,
                        text="🌿 Please write your feedback after the /feedback command.\n\n"
                             "Example: `/feedback I really like the immersive captions!`"
                    )
        elif update.callback_query: 
            await handle_callback(update)

    try: loop.run_until_complete(process_update())
    except Exception as e: print(f"🔴 Webhook Error: {e}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
