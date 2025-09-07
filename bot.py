# === 1. IMPORTS ===
from dotenv import load_dotenv
import os
import requests
import telegram
import asyncio
import json
import pytz
import logging
import sys
import redis
import threading
import time
from datetime import datetime, time, timedelta
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, CallbackQueryHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, NetworkError, TimedOut, Forbidden

# Load .env file
load_dotenv()

# === 2. RAILWAY ENVIRONMENT DETECTION ===
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def is_running_locally():
    """Check if running locally"""
    return os.getenv('RUNNING_LOCAL', 'false').lower() == 'true'

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'telegram-bot')

# === 3. ENHANCED LOGGING SETUP FOR RAILWAY ===
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# Create data directory if it doesn't exist
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception as e:
    print(f"Warning: Could not create data directory: {e}")

# Configure comprehensive logging WITHOUT HTTP requests
log_format = '%(asctime)s - TELEGRAM_BOT - %(levelname)s - %(message)s'
handlers = [logging.StreamHandler(sys.stdout)]

# Add file handler if directory is writable
if os.access(DATA_DIR, os.W_OK):
    try:
        log_file_path = os.path.join(DATA_DIR, "telegram_bot.log")
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
    except Exception as e:
        print(f"Warning: Could not create file handler: {e}")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=log_format,
    handlers=handlers,
    force=True
)

logger = logging.getLogger("telegram_bot")

# === 4. DISABLE HTTP REQUEST LOGGING ===
# Disable HTTP logs from telegram and httpx libraries
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("telegram.bot").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# === 5. GLOBAL ERROR HANDLER ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Global error handler for the bot"""
    try:
        logger.error(f"Exception while handling an update: {context.error}")
        
        # Handle specific Telegram errors
        if isinstance(context.error, BadRequest):
            if "Message to be replied not found" in str(context.error):
                logger.warning("Message not found error - likely message was deleted")
                return
            elif "Message can't be deleted" in str(context.error):
                logger.warning("Message can't be deleted - likely already deleted")
                return
        
        elif isinstance(context.error, NetworkError):
            logger.error(f"Network error: {context.error}")
            return
            
        elif isinstance(context.error, TimedOut):
            logger.error(f"Request timed out: {context.error}")
            return
            
        elif isinstance(context.error, Forbidden):
            logger.error(f"Bot was blocked by user or lacks permissions: {context.error}")
            return
        
        # Log the full traceback for debugging
        logger.exception("Full traceback:")
        
    except Exception as e:
        logger.error(f"Error in error handler: {e}")

# === 6. ENHANCED SAFE MESSAGE SENDING FUNCTIONS ===
async def safe_send_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, **kwargs):
    """Safely send a message with enhanced permission error handling"""
    try:
        return await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except Forbidden as e:
        logger.warning(f"❌ Bot lacks permission to send messages to chat {chat_id}: {e}")
        logger.info(f"💡 Solution: Promote bot to admin in chat {chat_id} with 'Send Messages' permission")
        
        # Try to get chat info for better logging
        try:
            chat = await context.bot.get_chat(chat_id)
            logger.info(f"📋 Chat details: {chat.title} (Type: {chat.type})")
        except:
            pass
            
        return None
    except BadRequest as e:
        logger.warning(f"Failed to send message: {e}")
        return None
    except NetworkError as e:
        logger.error(f"Network error sending message: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error sending message: {e}")
        return None

async def safe_reply_text(message, text: str, **kwargs):
    """Safely reply to a message with enhanced error handling"""
    try:
        return await message.reply_text(text, **kwargs)
    except Forbidden as e:
        logger.warning(f"❌ Bot lacks permission to reply in chat {message.chat_id}: {e}")
        logger.info(f"💡 Solution: Promote bot to admin in chat {message.chat_id}")
        return None
    except BadRequest as e:
        if "Message to be replied not found" in str(e):
            logger.warning("Original message not found, trying to send as new message")
            # Try to send as new message instead
            try:
                # Fixed: Use context parameter properly
                return await message.bot.send_message(chat_id=message.chat_id, text=text, **kwargs)
            except:
                return None
        else:
            logger.warning(f"Failed to reply: {e}")
            return None
    except Exception as e:
        logger.error(f"Unexpected error replying: {e}")
        return None

async def safe_send_sticker(context: ContextTypes.DEFAULT_TYPE, chat_id: int, sticker: str):
    """Safely send a sticker with enhanced error handling"""
    try:
        return await context.bot.send_sticker(chat_id=chat_id, sticker=sticker)
    except Forbidden as e:
        logger.warning(f"❌ Bot lacks permission to send stickers to chat {chat_id}: {e}")
        return None
    except BadRequest as e:
        logger.warning(f"Failed to send sticker: {e}")
        return None
    except NetworkError as e:
        logger.error(f"Network error sending sticker: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error sending sticker: {e}")
        return None

async def safe_delete_message(message):
    """Safely delete a message with error handling"""
    try:
        await message.delete()
        return True
    except Forbidden as e:
        logger.warning(f"❌ Bot lacks permission to delete message: {e}")
        return False
    except BadRequest as e:
        if "Message can't be deleted" in str(e):
            logger.debug("Message can't be deleted - likely already deleted")
        else:
            logger.warning(f"Failed to delete message: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting message: {e}")
        return False

# === 7. BOT PERMISSION CHECKER ===
async def check_bot_admin_status(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Check if bot has admin permissions in the chat"""
    try:
        bot_member = await context.bot.get_chat_member(chat_id, context.bot.id)
        chat = await context.bot.get_chat(chat_id)
        
        if bot_member.status in ['administrator', 'creator']:
            logger.info(f"✅ Bot is admin in {getattr(chat, 'title', 'Unknown')} ({chat_id})")
            return True, True  # is_member, is_admin
        elif bot_member.status == 'member':
            if chat.type in ['group', 'supergroup']:
                logger.warning(f"⚠️ Bot is not admin in {getattr(chat, 'title', 'Unknown')} ({chat_id}) - may not send messages")
                return True, False  # is_member, not_admin
            else:
                # Private chat - should work fine
                return True, True
        else:
            logger.warning(f"❌ Bot status in chat {chat_id}: {bot_member.status}")
            return False, False
            
    except Forbidden:
        logger.warning(f"❌ Bot was removed or blocked from chat {chat_id}")
        return False, False
    except Exception as e:
        logger.error(f"❌ Error checking bot permissions for chat {chat_id}: {e}")
        return False, False

# Log environment info
if is_railway_environment():
    logger.info(f"🚂 Multi-Chat Telegram Bot starting on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
    logger.info(f"🔍 Log level: {LOG_LEVEL}")
    logger.info("📡 Data source: Redis (direct connection)")
else:
    logger.info("🏠 Multi-Chat Telegram Bot starting in local environment")
    logger.info(f"🔍 Log level: {LOG_LEVEL}")
    if is_running_locally():
        logger.info("📡 Data source: API (local mode)")
    else:
        logger.info("📡 Data source: Redis (direct connection)")

# === 8. REDIS CONFIGURATION ===
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
redis_client = None

# Redis keys from color_worker_monitor.py
REDIS_COLOR_PREDICTION_KEY = "lottery:color_prediction"
REDIS_SIZE_PREDICTION_KEY = "lottery:size_prediction"
REDIS_COLOR_PREDICTION_HISTORY_KEY = "lottery:color_prediction_history"
REDIS_SIZE_PREDICTION_HISTORY_KEY = "lottery:size_prediction_history"
REDIS_HISTORY_KEY = "lottery:history"
REDIS_COLOR_ACCURACY_KEY = "lottery:color_accuracy"
REDIS_SIZE_ACCURACY_KEY = "lottery:size_accuracy"

# === MULTI-CHAT REDIS PERSISTENCE KEYS ===
CHAT_STATE_PREFIX = "telegram_chat:"
CHAT_SCHEDULES_PREFIX = "telegram_chat_schedules:"

# === AUTHENTICATION CONFIG ===
BOT_PASSWORD = "Risky"

# === THREADING LOCK FOR SESSION MANAGEMENT ===
session_lock = threading.Lock()

def initialize_redis():
    """Initialize Redis connection"""
    global redis_client
    try:
        redis_client = redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
            retry_on_timeout=True
        )
        redis_client.ping()
        logger.info(f"✅ Redis connected: {REDIS_URL[:25]}...")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"❌ Redis connection failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Redis initialization error: {e}")
        return False

def get_redis_json(key, default=None):
    """Safely get and parse JSON data from Redis"""
    try:
        if not redis_client:
            return default
        
        raw_data = redis_client.get(key)
        if raw_data is None:
            return default
        
        return json.loads(raw_data)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for key {key}: {e}")
        return default
    except Exception as e:
        logger.error(f"Error getting Redis data for key {key}: {e}")
        return default

def set_redis_json(key, data):
    """Safely set JSON data to Redis"""
    try:
        if not redis_client:
            logger.warning("Redis client not available for saving data")
            return False
        
        json_data = json.dumps(data, indent=None, default=str)
        redis_client.set(key, json_data)
        return True
    except Exception as e:
        logger.error(f"Error setting Redis data for key {key}: {e}")
        return False

def get_data_from_source():
    """Get prediction data from Redis or API based on environment"""
    try:
        if is_running_locally():
            # Use API when running locally
            logger.debug("🌐 Fetching data from local API")
            response = requests.get('http://localhost:5000/api/v2/insights', timeout=10)
            return response.json()
        else:
            # Use Redis directly on Railway
            logger.debug("🔗 Fetching data from Redis")
            
            # Get prediction data
            color_prediction = get_redis_json(REDIS_COLOR_PREDICTION_KEY, {})
            size_prediction = get_redis_json(REDIS_SIZE_PREDICTION_KEY, {})
            
            # Get history data 
            color_history = get_redis_json(REDIS_COLOR_PREDICTION_HISTORY_KEY, [])
            size_history = get_redis_json(REDIS_SIZE_PREDICTION_HISTORY_KEY, [])
            
            # Get accuracy data
            color_accuracy = get_redis_json(REDIS_COLOR_ACCURACY_KEY, {})
            size_accuracy = get_redis_json(REDIS_SIZE_ACCURACY_KEY, {})
            
            # Structure data like API response
            return {
                'color_prediction': color_prediction,
                'size_prediction': size_prediction,
                'color_prediction_history': color_history,
                'size_prediction_history': size_history,
                'color_accuracy': color_accuracy,
                'size_accuracy': size_accuracy
            }
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error fetching data: {e}")
        return None

# Initialize Redis connection
if not initialize_redis():
    if not is_running_locally():
        logger.critical("❌ Could not connect to Redis and not running locally. Exiting.")
        sys.exit(1)
    else:
        logger.warning("⚠️ Redis unavailable, will use API in local mode")

# === 9. BOT CONFIGURATION & STICKERS ===
BOT_TOKEN = os.getenv('BOT_TOKEN')

if not BOT_TOKEN:
    logger.critical("❌ BOT_TOKEN not found in environment variables!")
    sys.exit(1)
else:
    logger.info(f"✅ BOT_TOKEN loaded (ending with: ...{BOT_TOKEN[-6:]})")

STICKERS = {
    "start": os.getenv('START_STICKER_ID'),
    "stop": os.getenv('STOP_STICKER_ID'),
    "wins": [os.getenv(f'WIN_{i}_STICKER_ID') for i in range(1, 31)]
}

sticker_count = sum(1 for s in STICKERS["wins"] if s) + (1 if STICKERS["start"] else 0) + (1 if STICKERS["stop"] else 0)
logger.info(f"🎭 Loaded {sticker_count} stickers")

# === 10. MULTI-CHAT STATE MANAGEMENT ===
def load_chat_state(chat_id):
    """Load bot state for specific chat from Redis"""
    default_tz = 'Asia/Kolkata'
    
    # Load from Redis
    redis_key = f"{CHAT_STATE_PREFIX}{chat_id}"
    logger.debug(f"📂 Loading chat state from Redis: {redis_key}")
    
    redis_state = get_redis_json(redis_key)
    if redis_state:
        logger.debug(f"✅ Chat state loaded successfully from Redis for chat {chat_id}")
        
        # Ensure all keys are present
        redis_state.setdefault('authenticated', False)
        redis_state.setdefault('is_running', False)
        redis_state.setdefault('current_mode', 'continuous')
        redis_state.setdefault('win_target', 15)
        redis_state.setdefault('session_win_count', 0)
        redis_state.setdefault('current_level', 1)
        redis_state.setdefault('max_level_session', 1)
        redis_state.setdefault('last_predicted_issue', None)
        redis_state.setdefault('timezone', default_tz)
        redis_state.setdefault('schedule', {})
        redis_state.setdefault('auto_win_target', 10)
        redis_state.setdefault('today_total_wins', 0)
        redis_state.setdefault('today_date', datetime.now(pytz.timezone(redis_state.get('timezone', default_tz))).strftime('%Y-%m-%d'))
        redis_state.setdefault('today_max_level', 1)
        redis_state.setdefault('today_session_count', 0)
        redis_state.setdefault('session_type', 'manual')  # NEW: track session type
        
        return redis_state
    
    # Create default state for new chat
    state = create_default_chat_state(default_tz)
    save_chat_state(chat_id, state)
    return state

def create_default_chat_state(default_tz):
    """Create default state structure for a chat"""
    return {
        "authenticated": False, "is_running": False, "current_mode": "continuous", "win_target": 15,
        "session_win_count": 0, "current_level": 1, "max_level_session": 1,
        "last_predicted_issue": None, "timezone": default_tz, "schedule": {},
        "auto_win_target": 10, "today_total_wins": 0,
        "today_date": datetime.now(pytz.timezone(default_tz)).strftime('%Y-%m-%d'),
        "today_max_level": 1, "today_session_count": 0, "session_type": "manual"
    }

def save_chat_state(chat_id, state):
    """Save chat state to Redis with enhanced error handling and retry"""
    redis_key = f"{CHAT_STATE_PREFIX}{chat_id}"
    
    # Enhanced debug logging
    logger.debug(f"💾 Saving state for chat {chat_id}: session_wins={state.get('session_win_count', 0)}, today_wins={state.get('today_total_wins', 0)}, running={state.get('is_running', False)}")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if set_redis_json(redis_key, state):
                logger.debug(f"💾 Chat state saved successfully to Redis for chat {chat_id} (attempt {attempt + 1})")
                return True
            else:
                logger.warning(f"⚠️ Redis save returned False for chat {chat_id} (attempt {attempt + 1})")
        except Exception as e:
            logger.error(f"❌ Error saving chat state for chat {chat_id} (attempt {attempt + 1}): {e}")
        
        if attempt < max_retries - 1:
            time.sleep(0.1)  # Brief delay before retry
    
    logger.error(f"❌ CRITICAL: Failed to save chat state after {max_retries} attempts for chat {chat_id}")
    return False

# === 11. AUTHENTICATION SYSTEM ===
def is_authenticated(chat_id):
    """Check if chat is authenticated"""
    state = load_chat_state(chat_id)
    return state.get('authenticated', False)

def authenticate_chat(chat_id):
    """Mark chat as authenticated"""
    state = load_chat_state(chat_id)
    state['authenticated'] = True
    save_chat_state(chat_id, state)
    logger.info(f"🔐 Chat {chat_id} authenticated successfully")

def require_auth(func):
    """Decorator to require authentication before executing command"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        
        if not is_authenticated(chat_id):
            await safe_reply_text(update.message, 
                "🔒 **Authentication Required**\n\n"
                "Please send the password to access bot features.\n"
                "Password hint: Ask Owner...",
                parse_mode='Markdown'
            )
            return
        
        return await func(update, context)
    
    return wrapper

# === 12. SESSION CONFLICT PREVENTION ===
def check_session_conflict(chat_id):
    """Check if a session is already running"""
    chat_state = load_chat_state(chat_id)
    return chat_state.get('is_running', False)

def start_session(chat_id, session_type='manual'):
    """Start a session with proper counter reset and conflict checking"""
    with session_lock:
        if check_session_conflict(chat_id):
            return False, "A session is already running"
        
        chat_state = load_chat_state(chat_id)
        chat_state['is_running'] = True
        chat_state['session_type'] = session_type
        chat_state['current_level'] = 1
        chat_state['session_win_count'] = 0  # ✅ CRITICAL: Reset to 0
        chat_state['max_level_session'] = 1
        chat_state['last_predicted_issue'] = None
        chat_state['today_session_count'] += 1
        
        # ✅ CRITICAL: Save immediately after reset
        save_success = save_chat_state(chat_id, chat_state)
        
        if not save_success:
            logger.error(f"❌ CRITICAL: Failed to save session start state for chat {chat_id}")
            return False, "Failed to initialize session"
        
        logger.info(f"✅ Session started for chat {chat_id}: type={session_type}, session_count={chat_state['today_session_count']}")
        return True, f"Session started ({session_type})"

def stop_session(chat_id):
    """Stop a session"""
    with session_lock:
        chat_state = load_chat_state(chat_id)
        if not chat_state.get('is_running', False):
            return False, "No session is currently running"
        
        # Update today's max level at the end of a session
        if chat_state['max_level_session'] > chat_state['today_max_level']:
            chat_state['today_max_level'] = chat_state['max_level_session']
        
        chat_state['is_running'] = False
        chat_state['session_type'] = 'manual'
        save_chat_state(chat_id, chat_state)
        return True, "Session stopped"

# === 13. MESSAGE HANDLER WITH SECURE PASSWORD DELETION ===
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all text messages for password authentication with auto-deletion"""
    chat_id = update.effective_chat.id
    message = update.message
    message_text = message.text
    
    # Skip if already authenticated
    if is_authenticated(chat_id):
        return
    
    # Check password
    if message_text and message_text.strip() == BOT_PASSWORD:
        # ✅ SECURITY: Delete password message immediately
        try:
            await message.delete()
            logger.info(f"🔒 Password message deleted from chat {chat_id} for security")
        except Forbidden:
            logger.warning(f"⚠️ Bot lacks permission to delete messages in chat {chat_id}")
        except BadRequest as e:
            logger.warning(f"⚠️ Could not delete password message in chat {chat_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error deleting password message in chat {chat_id}: {e}")
        
        # Authenticate the chat
        authenticate_chat(chat_id)
        
        chat_type = "group" if update.effective_chat.type in ['group', 'supergroup'] else "private chat"
        
        # Check bot permissions after authentication
        is_member, is_admin = await check_bot_admin_status(context, chat_id)
        
        permission_status = ""
        if update.effective_chat.type in ['group', 'supergroup']:
            if is_admin:
                permission_status = "✅ Bot has admin permissions"
            else:
                permission_status = "⚠️ Bot needs admin permissions for full functionality"
        
        # Send success message
        success_message = (
            f"✅ **Access Granted!** 🔓\n\n"
            f"🆔 **Chat ID:** `{chat_id}`\n"
            f"📊 **Chat Type:** {chat_type}\n"
            f"{permission_status}\n\n"
            f"🔒 **Security:** Your password message has been automatically deleted.\n\n"
            f"🤖 You can now use all bot commands!\n\n"
            f"**Quick Start:**\n"
            f"• `/start` - Start prediction session\n"
            f"• `/status` - Check current status\n"
            f"• `/schedule` - Setup auto sessions\n"
            f"• `/id` - Show chat ID"
        )
        
        await safe_send_message(context, chat_id, success_message, parse_mode='Markdown')
        logger.info(f"🔐 New chat authenticated: {chat_id} ({chat_type}) - Password deleted")
        
    else:
        # Handle incorrect password
        error_message = (
            "❌ **Incorrect Password** 🔒\n\n"
            "Please enter the correct password to access bot features.\n"
            "Password hint: Ask Owner...\n\n"
            "🔒 **Note:** For security, your password will be automatically deleted once entered correctly."
        )
        
        await safe_send_message(context, chat_id, error_message, parse_mode='Markdown')
        
        # Optional: Delete incorrect password attempts for security
        try:
            await message.delete()
            logger.info(f"🔒 Deleted incorrect password attempt from chat {chat_id}")
        except Exception as e:
            logger.debug(f"Could not delete incorrect password attempt: {e}")

# === 14. COMMAND HANDLERS WITH ENHANCED SESSION MANAGEMENT ===
@require_auth
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"🎬 Start command received from chat {chat_id}")
    
    try:
        # Check for session conflict
        success, message = start_session(chat_id, 'manual')
        if not success:
            await safe_reply_text(update.message, f"⚠️ {message}. Please stop the current session first.")
            return
        
        logger.info(f"📈 Manual session started for chat {chat_id}")

        try:
            logger.info("🌐 Fetching initial data")
            response = get_data_from_source()
            if response and response.get('size_prediction'):
                full_issue_number = response['size_prediction'].get('issue', 'N/A')
                logger.info(f"✅ Data received, issue: {full_issue_number}")
                
                await safe_send_message(context, chat_id, f"▶️ Starting prediction from mixpred.\nIssue: {full_issue_number}")
            else:
                logger.warning("⚠️ No size prediction data available")
                await safe_send_message(context, chat_id, "▶️ Starting prediction from mixpred.\nIssue: N/A")
            
            if STICKERS["start"]:
                await safe_send_sticker(context, chat_id, STICKERS["start"])
                logger.debug("🎭 Start sticker sent")
            
        except Exception as e:
            logger.error(f"❌ Error getting initial data: {e}")
            await safe_reply_text(update.message, "✅ Bot Started (with warnings)")
            
    except Exception as e:
        logger.error(f"❌ Error in start command for chat {chat_id}: {e}")

@require_auth
async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"⏹️ Stop command received from chat {chat_id}")
    
    try:
        success, message = stop_session(chat_id)
        if not success:
            await safe_reply_text(update.message, f"⚠️ {message}")
            return
        
        chat_state = load_chat_state(chat_id)
        
        summary = (
            f"⏹️ **Session Closed** ⏹️\n\n"
            f"Total Session Wins: **{chat_state['session_win_count']}**\n"
            f"Max Level Reached: **L{chat_state['max_level_session']}**"
        )
        await safe_reply_text(update.message, summary, parse_mode='Markdown')
        
        if STICKERS["stop"]:
            await safe_send_sticker(context, chat_id, STICKERS["stop"])
            logger.debug("🎭 Stop sticker sent")
        
        logger.info(f"📊 Session ended: {chat_state['session_win_count']} wins, L{chat_state['max_level_session']} max for chat {chat_id}")
        
    except Exception as e:
        logger.error(f"❌ Error in stop command for chat {chat_id}: {e}")

@require_auth
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"📊 Status command received from chat {chat_id}")
    
    try:
        chat_state = load_chat_state(chat_id)
        status_text = "Running" if chat_state["is_running"] else "Stopped"
        tz = pytz.timezone(chat_state['timezone'])
        
        now = datetime.now(tz)
        current_time_str = now.strftime('%I:%M:%S %p %Z')
        current_date = now.strftime('%Y-%m-%d')
        
        # ✅ FIX: Only reset if it's actually a new day AND we don't have valid data
        if current_date != chat_state.get('today_date', current_date):
            logger.info(f"📅 New day detected: {current_date} (was {chat_state.get('today_date', 'unknown')})")
            # Only reset if we actually have data to preserve
            if chat_state.get('today_total_wins', 0) > 0 or chat_state.get('today_session_count', 0) > 0:
                logger.info(f"📊 Preserving daily stats: {chat_state.get('today_total_wins', 0)} wins, {chat_state.get('today_session_count', 0)} sessions")
            else:
                # Reset to 0 only if no activity detected
                chat_state['today_total_wins'] = 0
                chat_state['today_session_count'] = 0
                chat_state['today_max_level'] = 1
            chat_state['today_date'] = current_date
            save_chat_state(chat_id, chat_state)

        # Check bot permissions
        is_member, is_admin = await check_bot_admin_status(context, chat_id)
        permission_status = "✅ Admin" if is_admin else "⚠️ Member" if is_member else "❌ No access"

        # Get session type info
        session_info = f"({chat_state.get('session_type', 'manual')})" if chat_state["is_running"] else ""

        message = (
            f"📊 **Bot Status** 📊\n\n"
            f"🆔 **Chat ID:** `{chat_id}`\n"
            f"🕒 **Current Time:** **{current_time_str}**\n"
            f"🔐 **Bot Status:** {permission_status}\n\n"
            f"**State:** {status_text} {session_info}\n"
            f"**Mode:** `{chat_state['current_mode']}` (Target: {chat_state.get('win_target', 'N/A')})\n\n"
            f"**-- Current Session --**\n"
            f"Session Wins: **{chat_state.get('session_win_count', 0)}**\n"
            f"Max Level This Session: **L{chat_state.get('max_level_session', 1)}**\n\n"
            f"**-- Today's Stats --**\n"
            f"Today's Total Wins: **{chat_state.get('today_total_wins', 0)}**\n"
            f"Today's Session Count: **{chat_state.get('today_session_count', 0)}**\n"
            f"Today's Max Level: **L{chat_state.get('today_max_level', 1)}**"
        )
        await safe_reply_text(update.message, message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"❌ Error in status command for chat {chat_id}: {e}")

async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show chat ID - works without authentication"""
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'N/A')
    username = getattr(update.effective_chat, 'username', 'N/A')
    
    # Check bot permissions
    is_member, is_admin = await check_bot_admin_status(context, chat_id)
    permission_status = "✅ Admin" if is_admin else "⚠️ Member" if is_member else "❌ No access"
    
    chat_info = "Private Chat" if chat_type == 'private' else f"Group Chat: {chat_title}"
    
    message = (
        f"🆔 **Chat Information**\n\n"
        f"**Chat ID:** `{chat_id}`\n"
        f"**Type:** {chat_info}\n"
        f"**Username:** @{username}" if username != 'N/A' else f"**Username:** None\n"
        f"**Bot Status:** {permission_status}"
    )
    
    await safe_reply_text(update.message, message, parse_mode='Markdown')
    logger.info(f"🆔 ID command executed for chat {chat_id} ({chat_type})")

@require_auth
async def winlimit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"🎯 Win limit command received from chat {chat_id}")
    
    try:
        chat_state = load_chat_state(chat_id)
        target = 15
        if context.args:
            try:
                target = int(context.args[0])
                if target <= 0: target = 15
            except (ValueError, IndexError):
                target = 15
        
        chat_state["current_mode"] = "win_limit"
        chat_state["win_target"] = target
        save_chat_state(chat_id, chat_state)
        
        logger.info(f"🎯 Mode changed to win-limit with target: {target} for chat {chat_id}")
        await safe_reply_text(update.message, f"⚙️ **Mode changed to: Win-Limit** (Target: {target} wins)")
        
    except Exception as e:
        logger.error(f"❌ Error in winlimit command for chat {chat_id}: {e}")

# === 15. ENHANCED SCHEDULING WITH ALERTS ===
async def schedule_session_alert(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Send 5-minute warning alert"""
    try:
        await safe_send_message(context, chat_id, "⏰ **Auto-session will start in 5 minutes!**\n\nGet ready for predictions! 🎯", parse_mode='Markdown')
        logger.info(f"📢 5-minute alert sent to chat {chat_id}")
    except Exception as e:
        logger.error(f"❌ Error sending alert to chat {chat_id}: {e}")

async def auto_session_start(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Start automated session with conflict checking"""
    logger.info(f"🤖 Auto-session attempting to start for chat {chat_id}...")
    
    try:
        chat_state = load_chat_state(chat_id)
        
        # Check if chat is still authenticated
        if not chat_state.get('authenticated', False):
            logger.warning(f"⚠️ Skipping auto-session for unauthenticated chat {chat_id}")
            return
        
        # ✅ FIX: Check for session conflict
        success, message = start_session(chat_id, 'scheduled')
        if not success:
            logger.warning(f"⚠️ Auto-session skipped for chat {chat_id}: {message}")
            await safe_send_message(context, chat_id, f"⚠️ **Auto-session skipped:** {message}")
            return
        
        # Update session mode for auto sessions
        chat_state = load_chat_state(chat_id)
        chat_state['current_mode'] = 'win_limit'
        chat_state['win_target'] = chat_state['auto_win_target']
        save_chat_state(chat_id, chat_state)
        
        logger.info(f"🎯 Auto-session started with target: {chat_state['auto_win_target']} wins for chat {chat_id}")
        
        await safe_send_message(context, chat_id, f"🤖 **Auto-session started!**\n\nTarget: {chat_state['auto_win_target']} wins 🎯", parse_mode='Markdown')
        if STICKERS["start"]:
            await safe_send_sticker(context, chat_id, STICKERS["start"])
            
    except Exception as e:
        logger.error(f"❌ Error in auto session start for chat {chat_id}: {e}")

@require_auth
async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"📅 Schedule command received from chat {chat_id}")
    
    try:
        chat_state = load_chat_state(chat_id)
        message = update.message
        args = context.args

        if args:
            arg = args[0]
            if ":" in arg:
                try:
                    time_parts = arg.split(':')
                    hour, minute = int(time_parts[0]), int(time_parts[1])
                    if not (0 <= hour <= 23 and 0 <= minute <= 59):
                        raise ValueError("Time out of range")
                    
                    await add_or_remove_schedule(chat_id, hour, minute, context)
                    logger.info(f"⏰ Schedule updated for {hour:02d}:{minute:02d} in chat {chat_id}")
                    await safe_reply_text(message, f"✅ Schedule updated for **{hour:02d}:{minute:02d}** with 5-minute alert.", parse_mode='Markdown')
                except (ValueError, IndexError):
                    logger.warning(f"❌ Invalid time format: {arg} in chat {chat_id}")
                    await safe_reply_text(message, "❌ Invalid time format. Please use HH:MM (e.g., `8:12` or `22:45`).")
                return
            
            elif arg.isdigit():
                chat_state['auto_win_target'] = int(arg)
                save_chat_state(chat_id, chat_state)
                logger.info(f"🎯 Auto-session win target set to {chat_state['auto_win_target']} for chat {chat_id}")
                await safe_reply_text(message, f"✅ Auto-session win target set to {chat_state['auto_win_target']}.")
                return

        buttons = []
        for hour in range(8, 21):
            row = []
            time_str_hour = f"{hour:02d}:00"
            text_hour = f"✅ {time_str_hour}" if time_str_hour in chat_state['schedule'] else f"🕘 {time_str_hour}"
            row.append(InlineKeyboardButton(text_hour, callback_data=f"schedule_{chat_id}_{time_str_hour}"))
            
            time_str_half = f"{hour:02d}:30"
            text_half = f"✅ {time_str_half}" if time_str_half in chat_state['schedule'] else f"🕥 {time_str_half}"
            row.append(InlineKeyboardButton(text_half, callback_data=f"schedule_{chat_id}_{time_str_half}"))
            buttons.append(row)

        keyboard = InlineKeyboardMarkup(buttons)
        await safe_reply_text(message,
            f"Select auto-start times (Timezone: {chat_state['timezone']}).\n"
            "To set a **custom time**, use `/schedule HH:MM`.\n"
            f"The win target for auto-sessions is **{chat_state['auto_win_target']}**. To change it, use `/schedule <number>`.\n\n"
            "⏰ **5-minute alerts** will be sent before each scheduled session.",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"❌ Error in schedule command for chat {chat_id}: {e}")

@require_auth
async def viewschedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"👀 View schedule command received from chat {chat_id}")
    
    try:
        chat_state = load_chat_state(chat_id)
        if not chat_state['schedule']:
            await safe_reply_text(update.message, "No sessions are scheduled.")
            return
        
        scheduled_times = sorted(chat_state['schedule'].keys())
        message = "🗓️ **Scheduled Auto-Start Times:**\n" + "\n".join(f"- {t} (with 5-min alert)" for t in scheduled_times)
        await safe_reply_text(update.message, message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"❌ Error in viewschedule command for chat {chat_id}: {e}")

@require_auth
async def clearschedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"🗑️ Clear schedule command received from chat {chat_id}")
    
    try:
        chat_state = load_chat_state(chat_id)
        
        # Remove all scheduled jobs (both alerts and starts)
        for job_name in list(chat_state['schedule'].values()):
            # Remove main session job
            jobs = context.job_queue.get_jobs_by_name(job_name)
            for job in jobs:
                job.schedule_removal()
            
            # Remove alert job
            alert_job_name = f"alert_{job_name}"
            alert_jobs = context.job_queue.get_jobs_by_name(alert_job_name)
            for job in alert_jobs:
                job.schedule_removal()
        
        schedule_count = len(chat_state['schedule'])
        chat_state['schedule'] = {}
        save_chat_state(chat_id, chat_state)
        
        logger.info(f"✅ Cleared {schedule_count} scheduled sessions for chat {chat_id}")
        await safe_reply_text(update.message, "🗑️ All scheduled sessions and alerts have been cleared.")
        
    except Exception as e:
        logger.error(f"❌ Error in clearschedule command for chat {chat_id}: {e}")

@require_auth
async def timezone_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info(f"🌍 Timezone command received from chat {chat_id}")
    
    try:
        chat_state = load_chat_state(chat_id)
        if not context.args:
            await safe_reply_text(update.message, f"Usage: `/timezone Timezone`\nExample: `/timezone Asia/Kolkata`\nCurrent: `{chat_state['timezone']}`")
            return
        
        try:
            tz_str = context.args[0]
            pytz.timezone(tz_str)
            old_tz = chat_state['timezone']
            chat_state['timezone'] = tz_str
            save_chat_state(chat_id, chat_state)
            
            logger.info(f"🌍 Timezone changed from {old_tz} to {tz_str} for chat {chat_id}")
            await safe_reply_text(update.message, f"✅ Timezone set to `{tz_str}`.")
        except pytz.UnknownTimeZoneError:
            logger.warning(f"❌ Invalid timezone attempted: {context.args[0]} for chat {chat_id}")
            await safe_reply_text(update.message, "❌ Invalid timezone. Please use a valid format like 'Asia/Kolkata' or 'UTC'.")
            
    except Exception as e:
        logger.error(f"❌ Error in timezone command for chat {chat_id}: {e}")

# === 16. BUTTON CALLBACK WITH MULTI-CHAT SUPPORT ===
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer()
        logger.debug(f"🔘 Button callback: {query.data}")
        
        # Parse callback data: action_chatid_value
        data_parts = query.data.split('_')
        if len(data_parts) < 3:
            logger.error(f"❌ Invalid callback data format: {query.data}")
            return
            
        action = data_parts[0]
        callback_chat_id = int(data_parts[1])
        value = '_'.join(data_parts[2:])  # Rejoin in case value has underscores

        # Verify the callback is for the current chat
        current_chat_id = query.message.chat_id
        if callback_chat_id != current_chat_id:
            await query.answer("❌ This button is not for this chat")
            return

        if action == "schedule":
            hour, minute = map(int, value.split(':'))
            await add_or_remove_schedule(callback_chat_id, hour, minute, context)
            
            # Safely delete the message before showing new schedule
            if await safe_delete_message(query.message):
                logger.debug(f"Old schedule message deleted for chat {callback_chat_id}")
            
            # Send new schedule as a fresh message instead of editing
            await safe_send_message(context, callback_chat_id, 
                f"✅ Schedule updated for {hour:02d}:{minute:02d} with 5-minute alert")
            
    except Exception as e:
        logger.error(f"❌ Error in button callback: {e}")
        try:
            await query.answer("❌ An error occurred")
        except:
            pass

# === 17. ENHANCED SCHEDULED JOB (FIXED DUPLICATES & STATS) ===
async def scheduled_job(context: ContextTypes.DEFAULT_TYPE):
    logger.debug("🔄 Running scheduled prediction check for all authenticated chats...")
    
    try:
        pattern = f"{CHAT_STATE_PREFIX}*"
        chat_keys = redis_client.keys(pattern)
        
        for chat_key in chat_keys:
            try:
                chat_id = int(chat_key.replace(CHAT_STATE_PREFIX, ""))
                chat_state = load_chat_state(chat_id)
                
                if not chat_state.get("authenticated", False) or not chat_state.get("is_running", False):
                    continue
                
                logger.debug(f"🔄 Processing scheduled job for chat {chat_id}")
                
                response = get_data_from_source()
                if not response:
                    continue
                
                # ✅ FIX: Get next prediction first and check for duplicates
                next_pred = response.get('size_prediction', {})
                next_issue = next_pred.get('issue')
                next_size = next_pred.get('next_size', 'N/A')
                
                # ✅ FIX: Skip if no new issue or duplicate
                if not next_issue or next_issue == chat_state.get("last_predicted_issue"):
                    logger.debug(f"Skipping duplicate/old prediction for chat {chat_id}: {next_issue}")
                    continue
                
                # Check previous result first if we have history
                size_prediction_history = response.get('size_prediction_history', [])
                if len(size_prediction_history) >= 2:
                    last_completed = size_prediction_history[1]
                    
                    if chat_state["last_predicted_issue"] == last_completed.get('issue'):
                        predicted = last_completed.get('predicted_size')
                        actual = last_completed.get('actual_size')
                        
                        logger.info(f"🎯 Result check for chat {chat_id} - Predicted: {predicted}, Actual: {actual}")

                        # ✅ FIX: Enhanced WIN detection with thread-safe updates
                        if predicted and actual and predicted == actual:  # WIN
                            # ✅ FIX: Thread-safe increment with immediate save
                            with session_lock:
                                chat_state["session_win_count"] += 1
                                chat_state["today_total_wins"] += 1
                                chat_state["current_level"] = 1
                                
                                win_count = chat_state["session_win_count"]
                                
                                # ✅ CRITICAL: Save state immediately after incrementing
                                save_success = save_chat_state(chat_id, chat_state)
                                
                                if not save_success:
                                    logger.error(f"❌ CRITICAL: Failed to save win state for chat {chat_id}")
                                else:
                                    logger.info(f"✅ Win #{win_count} saved for chat {chat_id} (Today: {chat_state['today_total_wins']})")
                            
                            # Send win notification
                            try:
                                win_sticker_id = STICKERS["wins"][win_count - 1] if win_count <= len(STICKERS["wins"]) else None
                                if win_sticker_id:
                                    await safe_send_sticker(context, chat_id, win_sticker_id)
                                    logger.debug(f"🎭 Win sticker {win_count} sent to chat {chat_id}")
                                else: 
                                    await safe_send_message(context, chat_id, f"Win {win_count}")
                                    logger.debug(f"💬 Win message {win_count} sent to chat {chat_id}")
                            except Exception as e:
                                await safe_send_message(context, chat_id, f"Win {win_count}")
                                logger.debug(f"💬 Win message {win_count} sent to chat {chat_id} (sticker failed)")

                            # ✅ FIX: Check target AFTER saving win count
                            if win_count >= chat_state['win_target']:
                                logger.info(f"🎯 Target reached for chat {chat_id}! {win_count}/{chat_state['win_target']}")
                                
                                # Stop session using the enhanced stop function
                                success, message = stop_session(chat_id)
                                final_chat_state = load_chat_state(chat_id)  # Reload fresh state
                                
                                summary = (
                                    f"🎯 **Target Reached & Session Closed** 🎯\n\n"
                                    f"Total Session Wins: **{final_chat_state['session_win_count']}**\n"
                                    f"Max Level Reached: **L{final_chat_state['max_level_session']}**"
                                )
                                await safe_send_message(context, chat_id, summary, parse_mode='Markdown')
                                if STICKERS["stop"]:
                                    await safe_send_sticker(context, chat_id, STICKERS["stop"])
                                
                                continue  # Skip further processing for this chat
                        else:  # LOSS
                            chat_state["current_level"] += 1
                            logger.info(f"❌ LOSS for chat {chat_id}! Level increased to: {chat_state['current_level']}")
                
                # Update max level tracking
                if chat_state['current_level'] > chat_state['max_level_session']:
                    chat_state['max_level_session'] = chat_state['current_level']
                    
                if chat_state['max_level_session'] > chat_state['today_max_level']:
                    chat_state['today_max_level'] = chat_state['max_level_session']

                # ✅ FIX: Send NEW prediction only
                if next_issue and next_size:
                    prediction_text = f"{next_issue[-4:]} {next_size.upper()}\nL{chat_state['current_level']}"
                    await safe_send_message(context, chat_id, prediction_text)
                    
                    # ✅ FIX: Update last predicted issue AFTER sending
                    chat_state["last_predicted_issue"] = next_issue
                    
                    logger.info(f"📤 Sent NEW prediction to chat {chat_id}: {next_issue[-4:]} {next_size.upper()} L{chat_state['current_level']}")
                
                # ✅ FIX: Save state after every update
                save_chat_state(chat_id, chat_state)
                
            except Exception as e:
                logger.error(f"❌ Error processing scheduled job for chat {chat_id}: {e}")

    except Exception as e:
        logger.error(f"❌ Unexpected error in scheduled_job: {e}")

# === 18. ENHANCED SCHEDULE MANAGEMENT WITH ALERTS ===
async def add_or_remove_schedule(chat_id, hour: int, minute: int, context: ContextTypes.DEFAULT_TYPE):
    time_str = f"{hour:02d}:{minute:02d}"
    job_name = f"auto_session_{chat_id}_{time_str.replace(':', '')}"
    alert_job_name = f"alert_{job_name}"
    chat_state = load_chat_state(chat_id)
    tz = pytz.timezone(chat_state['timezone'])
    
    try:
        if time_str in chat_state['schedule']:
            # Remove existing schedule and alert
            jobs = context.job_queue.get_jobs_by_name(job_name)
            for job in jobs:
                job.schedule_removal()
            
            alert_jobs = context.job_queue.get_jobs_by_name(alert_job_name)
            for job in alert_jobs:
                job.schedule_removal()
                
            del chat_state['schedule'][time_str]
            logger.info(f"❌ Removed schedule and alert for {time_str} in chat {chat_id}")
        else:
            # Add new schedule with alert
            scheduled_time = time(hour=hour, minute=minute, tzinfo=tz)
            
            # Schedule 5-minute alert
            alert_time = time(hour=hour, minute=max(0, minute-5) if minute >= 5 else 55, 
                            tzinfo=tz)
            if minute < 5:
                # If alert would be previous hour, adjust
                alert_hour = hour - 1 if hour > 0 else 23
                alert_time = time(hour=alert_hour, minute=55, tzinfo=tz)
            
            context.job_queue.run_daily(
                lambda ctx: asyncio.create_task(schedule_session_alert(ctx, chat_id)),
                time=alert_time,
                name=alert_job_name
            )
            
            # Schedule main session start
            context.job_queue.run_daily(
                lambda ctx: asyncio.create_task(auto_session_start(ctx, chat_id)),
                time=scheduled_time,
                name=job_name
            )
            
            chat_state['schedule'][time_str] = job_name
            logger.info(f"✅ Added schedule and alert for {time_str} in chat {chat_id}")
        
        save_chat_state(chat_id, chat_state)
    except Exception as e:
        logger.error(f"❌ Error managing schedule for chat {chat_id}: {e}")

# === 19. NEW CHAT MEMBER HANDLER (ENHANCED) ===
async def new_chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle when bot is added to a new group"""
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Unknown')
    
    # Check if bot was added
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            logger.info(f"🤖 Bot added to new {chat_type}: {chat_id} ({chat_title})")
            
            # Initialize chat state
            load_chat_state(chat_id)
            
            # Check bot permissions
            is_member, is_admin = await check_bot_admin_status(context, chat_id)
            
            welcome_message = (
                f"👋 **Hello {chat_title}!**\n\n"
                f"🤖 I'm your advanced lottery prediction bot!\n"
                f"🆔 **Your Chat ID:** `{chat_id}`\n\n"
            )
            
            if not is_admin and chat_type in ['group', 'supergroup']:
                welcome_message += (
                    f"⚠️ **Important:** I need admin permissions to function properly!\n"
                    f"Please promote me to admin with 'Send Messages' permission.\n\n"
                    f"**How to promote me:**\n"
                    f"1. Go to Group Settings → Administrators\n"
                    f"2. Add Administrator → Select me (@{context.bot.username})\n"
                    f"3. Grant 'Send Messages' permission\n"
                    f"4. Save changes\n\n"
                )
            
            welcome_message += (
                f"🔒 **To get started, please send me the password.**\n"
                f"Password hint: Ask Owner...\n\n"
                f"Once authenticated, you can use:\n"
                f"• `/start` - Start prediction session\n"
                f"• `/status` - Check current status\n"
                f"• `/schedule` - Setup auto sessions with alerts\n"
                f"• `/id` - Show chat ID\n\n"
                f"✨ **Features:**\n"
                f"• Session conflict prevention\n"
                f"• 5-minute alerts before scheduled sessions\n"
                f"• Enhanced statistics tracking\n"
                f"• Fixed win counting system"
            )
            
            result = await safe_send_message(context, chat_id, welcome_message, parse_mode='Markdown')
            if result is None:
                logger.warning(f"⚠️ Could not send welcome message to chat {chat_id} - likely needs admin permissions")
            break

# === 20. MAIN APPLICATION SETUP (ENHANCED) ===
def main():
    logger.info("🚀 Starting Enhanced Multi-Chat Telegram Bot application...")
    
    if not BOT_TOKEN:
        logger.critical("❌ BOT_TOKEN not found in environment variables!")
        print("!!! BOT_TOKEN not found. Please set it in Railway environment variables. !!!")
        sys.exit(1)

    # Enhanced Application builder with correct syntax
    application = (Application.builder()
                  .token(BOT_TOKEN)
                  .connect_timeout(30)
                  .read_timeout(30)
                  .write_timeout(30)
                  .pool_timeout(30)
                  .build())
    
    logger.info("✅ Enhanced Multi-Chat Telegram Application created")
    
    # Add global error handler
    application.add_error_handler(error_handler)
    logger.info("✅ Global error handler registered")
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("id", id_command))  # Works without auth
    application.add_handler(CommandHandler("winlimit", winlimit_command))
    application.add_handler(CommandHandler("schedule", schedule_command))
    application.add_handler(CommandHandler("viewschedule", viewschedule_command))
    application.add_handler(CommandHandler("clearschedule", clearschedule_command))
    application.add_handler(CommandHandler("timezone", timezone_command))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Add message handler for password authentication
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Add new chat member handler
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_member_handler))
    
    logger.info("✅ ALL command handlers registered")

    # Setup job queue and reschedule existing jobs for all chats
    job_queue = application.job_queue
    
    # Restore schedules for all authenticated chats
    try:
        pattern = f"{CHAT_STATE_PREFIX}*"
        chat_keys = redis_client.keys(pattern)
        total_schedules = 0
        total_alerts = 0
        
        for chat_key in chat_keys:
            try:
                chat_id = int(chat_key.replace(CHAT_STATE_PREFIX, ""))
                chat_state = load_chat_state(chat_id)
                
                if not chat_state.get('authenticated', False):
                    continue
                
                tz = pytz.timezone(chat_state['timezone'])
                
                for time_str, job_name in chat_state['schedule'].items():
                    try:
                        hour, minute = map(int, time_str.split(':'))
                        
                        # Restore main session job
                        job_queue.run_daily(
                            lambda ctx, cid=chat_id: asyncio.create_task(auto_session_start(ctx, cid)),
                            time=time(hour=hour, minute=minute, tzinfo=tz),
                            name=job_name
                        )
                        total_schedules += 1
                        
                        # Restore alert job
                        alert_job_name = f"alert_{job_name}"
                        alert_minute = max(0, minute-5) if minute >= 5 else 55
                        alert_hour = hour if minute >= 5 else (hour - 1 if hour > 0 else 23)
                        
                        job_queue.run_daily(
                            lambda ctx, cid=chat_id: asyncio.create_task(schedule_session_alert(ctx, cid)),
                            time=time(hour=alert_hour, minute=alert_minute, tzinfo=tz),
                            name=alert_job_name
                        )
                        total_alerts += 1
                        
                        logger.info(f"📅 Restored schedule and alert: {time_str} for chat {chat_id}")
                    except Exception as e:
                        logger.error(f"❌ Error rescheduling job {job_name} for chat {chat_id}: {e}")
                        
            except Exception as e:
                logger.error(f"❌ Error processing chat schedules for {chat_key}: {e}")
        
        logger.info(f"📅 Rescheduled {total_schedules} sessions and {total_alerts} alerts across all chats")
        
    except Exception as e:
        logger.error(f"❌ Error restoring schedules: {e}")
    
    # Setup main prediction job
    now = datetime.now()
    delay = (60 - now.second + 4) % 60
    if delay == 0: 
        delay = 60
    
    job_queue.run_repeating(
        lambda ctx: asyncio.create_task(scheduled_job(ctx)), 
        interval=60, 
        first=delay
    )
    logger.info(f"⏰ Main prediction job scheduled (starts in {delay}s)")

    # Final startup log
    logger.info("=" * 60)
    logger.info("🤖 ENHANCED BOT READY - FIXED WIN COUNTING & ALL FEATURES")
    logger.info("=" * 60)
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📱 Bot Token: ...{BOT_TOKEN[-6:]}")
    logger.info(f"🔗 Data Source: {'Redis' if not is_running_locally() else 'API (local)'}")
    logger.info(f"💾 Persistence: Multi-Chat Redis-based (survives redeployments)")
    logger.info(f"🔐 Authentication: Password-based per chat")
    logger.info(f"🛡️ Permissions: Enhanced error handling with admin detection")
    logger.info(f"🎭 Stickers: {sticker_count} loaded")
    logger.info(f"🔑 Password: {BOT_PASSWORD}")
    logger.info("✨ Session conflict prevention enabled")
    logger.info("⏰ 5-minute alerts for scheduled sessions")
    logger.info("📊 Enhanced statistics tracking")
    logger.info("🎯 FIXED: Win counting synchronization")
    logger.info("🔄 Fixed duplicate prediction prevention")
    logger.info("🛡️ Enhanced error handling enabled")
    logger.info("🔇 HTTP request logging disabled")
    logger.info("=" * 60)

    try:
        logger.info("🔄 Starting enhanced bot polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.critical(f"💥 Fatal error during polling: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
