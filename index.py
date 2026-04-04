# ... (Keep your imports and config the same)

# 💡 PRO-TIP: Once the bot sends the GIF successfully, check your logs for the 'file_id' 
# and use that instead of the URL for 10x faster loading.
LOGO_GIF = "https://media.giphy.com/media/cBKMTJGAE8y2Y/giphy.gif" 

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
        # Force Telegram to treat this as an animation (GIF)
        await tg_app.bot.send_animation(
            chat_id=chat_id, 
            animation=LOGO_GIF, 
            caption=caption,
            parse_mode='HTML', 
            reply_markup=get_main_menu_keyboard(),
            connect_timeout=10, # Give it time to load the GIF
            read_timeout=10
        )
    except Exception as e:
        print(f"GIF Error: {e}")
        # Reliable Fallback
        await tg_app.bot.send_message(
            chat_id=chat_id,
            text=f"<b>🍃 CLYDE'S RESOURCE HUB</b>\n\n{caption}",
            parse_mode='HTML',
            reply_markup=get_main_menu_keyboard()
        )

# --- WEBHOOK LOGIC FIX ---
@app.route('/', methods=['GET', 'POST'])
@app.route('/api/index', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET': return "🍃 Online.", 200
    
    update_data = request.get_json(force=True)
    
    async def process():
        # Ensure the bot is fully ready before processing
        if not tg_app.bot_data: 
            await tg_app.initialize()
            await tg_app.start() # Critical for some environments

        update = Update.de_json(update_data, tg_app.bot)
        
        # Handle /start or any text that looks like a start command
        if update.message and update.message.text:
            text = update.message.text.lower()
            if text.startswith("/start") or text.startswith("/menu"):
                await send_welcome_message(update.effective_chat.id, update.effective_user.first_name)
        
        # Handle Buttons
        elif update.callback_query:
            await handle_callback(update)

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process())
        loop.close()
    except Exception as e:
        print(f"Webhook Error: {e}")
        
    return "OK", 200
