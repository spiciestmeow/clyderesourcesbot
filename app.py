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
    caption = f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n🌿 <b>Welcome to Clyde's Enchanted Clearing</b>\n\nThe gentle wind carries whispers from the ancient forest...\nHidden treasures and digital wonders await kind-hearted wanderers.\n\n<i>May the forest spirits watch over you.</i> 🍃✨"

    # --- SECRET INGREDIENT START ---
    msg = await tg_app.bot.send_animation(chat_id=chat_id, animation=WELCOME_GIF, caption=caption, parse_mode='HTML', reply_markup=get_start_keyboard())
    if chat_id not in forest_memory: forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)
    # --- SECRET INGREDIENT END ---

async def send_full_menu(chat_id, first_name):
    user_tz = pytz.timezone('Asia/Manila')
    current_hour = datetime.now(user_tz).hour
    time_icon = "🌅" if 5 <= current_hour < 12 else "🌤️" if 12 <= current_hour < 18 else "🌙"
    greeting = "Good morning" if 5 <= current_hour < 12 else "Good afternoon" if 12 <= current_hour < 18 else "Good evening"
    caption = f"{time_icon} {greeting}, <b>{html.escape(str(first_name))}</b>!\n\n🌿 <b>You have entered the Enchanted Clearing</b>\n\nChoose your path beneath the whispering trees...\n\n<i>May your journey be filled with magic and abundance.</i> 🍃✨"

    # --- SECRET INGREDIENT START ---
    msg = await tg_app.bot.send_animation(chat_id=chat_id, animation=MENU_GIF, caption=caption, parse_mode='HTML', reply_markup=get_full_menu_keyboard())
    
    if chat_id not in forest_memory: forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)
    # --- SECRET INGREDIENT END ---


async def send_myid(chat_id):
    caption_text = (
        "🌿 <b>Forest Spirit Identification</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "✨ <i>The mist clears to reveal your essence...</i>\n\n"
        f"🆔 <b>User ID:</b> <code>{chat_id}</code>\n\n"
        "🍃 <i>Safe travels through the clearing, wanderer.</i>"
    )
    
    # 1. Capture the message when it is sent
    msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=MYID_GIF,
        caption=caption_text,
        parse_mode="HTML"
    )
    
    # 2. Add it to the memory for the /clear sweep
    if chat_id not in forest_memory: 
        forest_memory[chat_id] = []
    forest_memory[chat_id].append(msg.message_id)

# --- CLEAR FUNCTION ---
async def handle_clear(chat_id, user_command_id):
    # 1. Delete the user's "/clear" message immediately
    try: 
        await tg_app.bot.delete_message(chat_id=chat_id, message_id=user_command_id)
    except: 
        pass

    # 2. Sweep away all previous commands and bot messages
    if chat_id in forest_memory:
        for msg_id in forest_memory[chat_id]:
            try: 
                await tg_app.bot.send_chat_action(chat_id, "typing") # Optional: adds a 'magical' feel
                await tg_app.bot.delete_message(chat_id, msg_id)
            except: 
                pass
        forest_memory[chat_id] = [] # Reset memory

    # 3. Send the "Fresh Start" message
    sent_msg = await tg_app.bot.send_animation(
        chat_id=chat_id,
        animation=LOADING_GIF,
        caption="🍃 <b>The Forest Mist Clears...</b>\n\nYour path is now fresh and open.",
        parse_mode="HTML",
        reply_markup=get_start_keyboard()
    )
    
    # Save this new message ID so it can be cleared later
    forest_memory[chat_id].append(sent_msg.message_id)
    
# ==================== CALLBACK ====================
async def handle_callback(update: Update):
    query = update.callback_query
    await query.answer()

    # Main Menu & Clearing
    if query.data in ["show_main_menu", "main_menu"]:
        # Delete current message (About/Help/Guidance) so it disappears
        try:
            await query.message.delete()
        except:
            pass

        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id, 
            animation=LOADING_GIF, 
            caption="..."
        )
        
        await asyncio.sleep(1.2)
        await loading_msg.edit_caption(caption="🌲 <i>The ancient trees bow to reveal a hidden path...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        await loading_msg.edit_caption(caption="✨ <i>You have arrived at the heart of the clearing.</i>", parse_mode='HTML')
        await asyncio.sleep(0.8)
        
        await send_full_menu(update.effective_chat.id, update.effective_user.first_name)
        
        try:
            await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except:
            pass

    elif query.data == "check_vamt":
        await query.message.edit_caption(
            caption="🌿 <b>The Ancient Library</b>\n\nWhich digital scrolls are you looking for today?\n\n<i>The forest spirits wait for your choice.</i>", 
            parse_mode='HTML', 
            reply_markup=get_inventory_categories()
        )

    # ====================== FILTERED INVENTORY ======================
    elif query.data.startswith("vamt_filter_") or query.data.startswith("vamt_all_"):
        is_full_view = query.data.startswith("vamt_all_")
        category = query.data.replace("vamt_filter_", "").replace("vamt_all_", "").lower()

        loading_text = "📜 <i>Unrolling the full scroll...</i>" if is_full_view else f"✨ <i>Searching for {category.upper()}...</i>"
        await query.message.edit_caption(caption=loading_text, parse_mode='HTML')

        data = await get_vamt_data()
        
        if not data:
            await query.message.edit_caption(
                caption="🌫️ <i>Database connection failed. Please try again later.</i>",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        # Filter items
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

        print(f"DEBUG: Loaded {len(data)} total items | Found {len(filtered)} {category} items")

        if not filtered:
            await query.message.edit_caption(
                caption=f"🍃 <i>No {category.upper()} scrolls found right now.</i>",
                reply_markup=get_back_to_inventory_keyboard()
            )
            return

        # ====================== NETFLIX - MULTIPLE COOKIES ======================
        if category == "netflix":
            report = (
                "<b>🍿 NETFLIX PREMIUM COOKIES</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 Available: <b>{len(filtered)}</b>\n\n"
                "<i>Choose a cookie to reveal:</i>\n\n"
            )

            buttons = []
            for idx, item in enumerate(filtered, 1):
                # Use display_name if filled, otherwise fallback
                display_name = str(item.get('display_name') or '').strip()
                if not display_name:
                    display_name = f"Netflix Cookie {idx}"

                status_text = "✓ Active" if str(item.get('status', '')).lower() == "active" else "⚠️ Inactive"

                report += f"✨ <b>{display_name}</b>\n   Status: {status_text}\n\n"

                # Use idx for callback (we'll keep it stable below)
                buttons.append([
                    InlineKeyboardButton(f"🔓 Reveal {display_name}", callback_data=f"reveal_nf|{idx}")
                ])

            buttons.append([InlineKeyboardButton("⬅️ Back to Inventory", callback_data="check_vamt")])

            kb = InlineKeyboardMarkup(buttons)

            await query.message.edit_caption(
                caption=report, 
                parse_mode='HTML', 
                reply_markup=kb
            )
            return

        # ====================== WINDOWS & OFFICE ======================
        limit = len(filtered) if is_full_view else 3

        report = f"<b>📜 {category.upper()} SCROLLS</b>\n━━━━━━━━━━━━━━━━━━\n\n"

        for item in filtered[:limit]:
            product = item.get('service_type', 'Unknown')
            key = item.get('key_id', 'HIDDEN')
            raw_val = int(item.get('remaining') or 0)
            stock_text = f"{raw_val}" if raw_val > 0 else "Out of stock"

            report += (
                f"✨ <b>{product}</b>\n"
                f"└ 🔑 <code>{key}</code>\n"
                f"└ 📦 Stock: <b>{stock_text}</b>\n\n"
            )

        if not is_full_view and len(filtered) > 3:
            report += f"━━━━━━━━━━━━━━━━━━\n<i>... and {len(filtered) - 3} more scrolls hidden.</i>"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📜 Show All", callback_data=f"vamt_all_{category}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="check_vamt")]
            ])
        else:
            kb = get_back_to_inventory_keyboard()

        await query.message.edit_caption(
            caption=report,
            parse_mode='HTML',
            reply_markup=kb
        )

    # ====================== REVEAL NETFLIX COOKIE ======================
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

        # Get Netflix items only
        netflix_items = [
            item for item in data 
            if "netflix" in str(item.get('service_type', '')).lower()
        ]

        # Stable sort: Use display_name first, then last_updated as backup
        netflix_items.sort(key=lambda x: (
            str(x.get('display_name') or ''), 
            str(x.get('last_updated') or '')
        ))

        print(f"DEBUG Reveal: Requested #{idx}, Found {len(netflix_items)} Netflix cookies")

        if idx < 1 or idx > len(netflix_items):
            await query.answer(f"❌ Cookie #{idx} not found", show_alert=True)
            return

        item = netflix_items[idx - 1]
        cookie = str(item.get('key_id', '')).strip()
        display_name = str(item.get('display_name') or '').strip() or f"Netflix Cookie {idx}"

        status = "✓ Active" if str(item.get('status', '')).lower() == "active" else "⚠️ Expired / Inactive"

        report = (
            f"<b>🍿 {display_name} REVEALED</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"🌿 Status: <b>{status}</b>\n"
            f"📦 Remaining: <b>{item.get('remaining', 0)}</b>\n\n"
            "<b>📋 Cookie:</b>\n"
            f"<code>{html.escape(cookie[:800])}</code>\n\n"
            "<i>Long-press the code above to copy.\n"
            "Use it quickly before it expires 🍃</i>"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Netflix Cookies", callback_data="vamt_filter_netflix")]
        ])

        await query.message.edit_caption(
            caption=report,
            parse_mode='HTML',
            reply_markup=kb
        )

    # 🌟 ABOUT (Lore)
    elif query.data == "about":
        try:
            await query.message.delete()
        except:
            pass

        # Send loading animation
        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption="✨ <i>Consulting the ancient records...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.0)
        await loading_msg.edit_caption(caption="🍃 <i>Gathering history from the leaves...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        await loading_msg.edit_caption(caption="✨ <i>The story is ready...</i>", parse_mode='HTML')
        await asyncio.sleep(0.8)

        text = (
            "<b>🌿 About Clyde's Enchanted Clearing</b>\n\n"
            "This is a peaceful digital forest inspired by the magic of Studio Ghibli.\n\n"
            "We gather digital treasures like Steam accounts, learning guides, and activation keys.\n\n"
            "<i>May this small corner bring you joy.</i> 🍃✨"
        )

        # Send final message
        final_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=ABOUT_GIF,
            caption=text,
            parse_mode='HTML',
            reply_markup=get_back_keyboard()
        )

        # Delete the loading animation so it doesn't remain
        try:
            await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except:
            pass

        # Save final message to memory
        chat_id = update.effective_chat.id
        if chat_id not in forest_memory:
            forest_memory[chat_id] = []
        forest_memory[chat_id].append(final_msg.message_id)

    # 🌟 HELP (Guidance)
    elif query.data == "help":
        try:
            await query.message.delete()
        except:
            pass

        # Send loading animation
        loading_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=LOADING_GIF,
            caption="✨ <i>Calling the forest guides...</i>",
            parse_mode='HTML'
        )

        await asyncio.sleep(1.0)
        await loading_msg.edit_caption(caption="🍃 <i>Clearing the path for a wanderer...</i>", parse_mode='HTML')
        await asyncio.sleep(1.2)
        await loading_msg.edit_caption(caption="✨ <i>The map is revealed...</i>", parse_mode='HTML')
        await asyncio.sleep(0.8)

        text = (
            "<b>❓ Guidance - How to Use</b>\n\n"
            "🌿 <b>Navigation:</b>\n"
            "• Tap buttons to move through the clearing.\n"
            "• Use <b>/menu</b> for the full list.\n\n"
            "📋 <b>Activation Keys:</b>\n"
            "1. Go to Inventory.\n"
            "2. Choose a category.\n"
            "3. Long-press the code to copy.\n\n"
            "✨ <b>Button Guide:</b>\n"
            "• 🪄 Spirit Treasures: Steam accounts\n"
            "• 📜 Ancient Scrolls: Learning guides\n"
            "• 🌿 Forest Inventory: Windows / Office / Netflix keys\n"
            "• 🌲 Whispering Forest: Main resource hub\n"
            "• ℹ️ Lore: The story of this clearing\n"
            "• 🕊️ Messenger: Contact the caretaker"
        )

        # Send final message
        final_msg = await tg_app.bot.send_animation(
            chat_id=update.effective_chat.id,
            animation=HELP_GIF,
            caption=text,
            parse_mode='HTML',
            reply_markup=get_back_keyboard()
        )

        # Delete the loading animation
        try:
            await tg_app.bot.delete_message(chat_id=loading_msg.chat_id, message_id=loading_msg.message_id)
        except:
            pass

        # Save final message to memory
        chat_id = update.effective_chat.id
        if chat_id not in forest_memory:
            forest_memory[chat_id] = []
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
        
            if text.startswith("/start"): await send_initial_welcome(chat_id, name)
            elif text.startswith("/menu"): await send_full_menu(chat_id, name)
            elif text.startswith("/myid"): await send_myid(chat_id)
            elif text.startswith("/clear"): await handle_clear(chat_id, user_msg_id)
        elif update.callback_query: await handle_callback(update)

    try: loop.run_until_complete(process_update())
    except Exception as e: print(f"🔴 Webhook Error: {e}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
