import os
import asyncio
import html
import httpx
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from datetime import datetime
import pytz

# CRITICAL: This must be at the very top level
app = Flask(__name__)

TOKEN = os.getenv("BOT_TOKEN")
tg_app = Application.builder().token(TOKEN).build()

# ... (Insert your get_main_menu_keyboard function here)

async def send_welcome_message(chat_id, first_name):
    # (Use the send_animation logic we discussed)
    # Adding a print here helps you see if the function even triggers in Vercel Logs
    print(f"Attempting to send welcome to {first_name}") 
    pass 

@app.route('/', methods=['GET', 'POST'])
def webhook():
    # If you open the URL in a browser, you should see this:
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
                # Add your callback handler call here
                pass

        loop.run_until_complete(process())
        loop.close()
    except Exception as e:
        print(f"Webhook Error: {e}")

    return "OK", 200
