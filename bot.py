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
from datetime import datetime, time
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
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
            logger.error(f"Bot was blocked by user: {context.error}")
            return
        
        # Log the full traceback for debugging
        logger.exception("Full traceback:")
        
    except Exception as e:
        logger.error(f"Error in error handler: {e}")

# === 6. SAFE MESSAGE SENDING FUNCTIONS ===
async def safe_send_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, **kwargs):
    """Safely send a message with error handling"""
    try:
        return await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
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
    """Safely reply to a message with error handling"""
    try:
        return await message.reply_text(text, **kwargs)
    except BadRequest as e:
        if "Message to be replied not found" in str(e):
            logger.warning("Original message not found, sending as new message")
            return await safe_send_message(None, message.chat_id, text, **kwargs)
        else:
            logger.warning(f"Failed to reply: {e}")
            return None
    except Exception as e:
        logger.error(f"Unexpected error replying: {e}")
        return None

async def safe_send_sticker(context: ContextTypes.DEFAULT_TYPE, chat_id: int, sticker: str):
    """Safely send a sticker with error handling"""
    try:
        return await context.bot.send_sticker(chat_id=chat_id, sticker=sticker)
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
    except BadRequest as e:
        if "Message can't be deleted" in str(e):
            logger.debug("Message can't be deleted - likely already deleted")
        else:
            logger.warning(f"Failed to delete message: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting message: {e}")
        return False

# Log environment info
if is_railway_environment():
    logger.info(f"🚂 Telegram Bot starting on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
    logger.info(f"🔍 Log level: {LOG_LEVEL}")
    logger.info("📡 Data source: Redis (direct connection)")
else:
    logger.info("🏠 Telegram Bot starting in local environment")
    logger.info(f"🔍 Log level: {LOG_LEVEL}")
    if is_running_locally():
        logger.info("📡 Data source: API (local mode)")
    else:
        logger.info("📡 Data source: Redis (direct connection)")

# === 7. REDIS CONFIGURATION ===
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

# === 8. BOT CONFIGURATION & STICKERS ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = int(os.getenv('CHAT_ID', '0'))
STATE_FILE = os.path.join(DATA_DIR, 'state.json')

logger.info(f"💾 State file: {STATE_FILE}")

if not BOT_TOKEN:
    logger.critical("❌ BOT_TOKEN not found in environment variables!")
    sys.exit(1)
else:
    logger.info(f"✅ BOT_TOKEN loaded (ending with: ...{BOT_TOKEN[-6:]})")

if CHAT_ID == 0:
    logger.warning("⚠️ CHAT_ID not set or invalid!")
else:
    logger.info(f"📱 CHAT_ID configured: {CHAT_ID}")

STICKERS = {
    "start": os.getenv('START_STICKER_ID'),
    "stop": os.getenv('STOP_STICKER_ID'),
    "wins": [os.getenv(f'WIN_{i}_STICKER_ID') for i in range(1, 31)]
}

sticker_count = sum(1 for s in STICKERS["wins"] if s) + (1 if STICKERS["start"] else 0) + (1 if STICKERS["stop"] else 0)
logger.info(f"🎭 Loaded {sticker_count} stickers")

# === 9. STATE MANAGEMENT ===
def load_state():
    """Loads bot state from a JSON file with comprehensive error handling."""
    default_tz = 'Asia/Kolkata'
    logger.debug(f"📂 Loading state from: {STATE_FILE}")
    
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            logger.info(f"✅ State loaded successfully from {STATE_FILE}")
            
            # Ensure all keys are present
            state.setdefault('is_running', False)
            state.setdefault('current_mode', 'continuous')
            state.setdefault('win_target', 15)
            state.setdefault('session_win_count', 0)
            state.setdefault('current_level', 1)
            state.setdefault('max_level_session', 1)
            state.setdefault('last_predicted_issue', None)
            state.setdefault('timezone', default_tz)
            state.setdefault('schedule', {})
            state.setdefault('auto_win_target', 10)
            state.setdefault('today_total_wins', 0)
            state.setdefault('today_date', datetime.now(pytz.timezone(state.get('timezone', default_tz))).strftime('%Y-%m-%d'))
            state.setdefault('today_max_level', 1)
            state.setdefault('today_session_count', 0)
            
            return state
            
    except FileNotFoundError:
        logger.info(f"📄 State file not found, creating default state")
        return create_default_state(default_tz)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in state file: {e}")
        return create_default_state(default_tz)
    except Exception as e:
        logger.error(f"❌ Unexpected error loading state: {e}")
        return create_default_state(default_tz)

def create_default_state(default_tz):
    """Create default state structure"""
    return {
        "is_running": False, "current_mode": "continuous", "win_target": 15,
        "session_win_count": 0, "current_level": 1, "max_level_session": 1,
        "last_predicted_issue": None, "timezone": default_tz, "schedule": {},
        "auto_win_target": 10, "today_total_wins": 0,
        "today_date": datetime.now(pytz.timezone(default_tz)).strftime('%Y-%m-%d'),
        "today_max_level": 1, "today_session_count": 0
    }

def save_state():
    """Saves the current bot state to a JSON file with error handling."""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(bot_state, f, indent=4)
        logger.debug(f"💾 State saved successfully to {STATE_FILE}")
    except Exception as e:
        logger.error(f"❌ Error saving state to {STATE_FILE}: {e}")

bot_state = load_state()

# === 10. COMMAND HANDLERS WITH ERROR HANDLING ===
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"🎬 Start command received from user {update.effective_user.id}")
    
    try:
        if CHAT_ID == 0:
            logger.error("❌ CHAT_ID not configured")
            await safe_reply_text(update.message, "⚠️ **Error:** CHAT_ID not set.")
            return

        # Update daily session count
        bot_state['today_session_count'] += 1
        logger.info(f"📈 Session count incremented to {bot_state['today_session_count']}")
        
        bot_state.update({
            "is_running": True, "current_level": 1,
            "session_win_count": 0, "max_level_session": 1,
            "last_predicted_issue": None
        })
        save_state()

        try:
            logger.info("🌐 Fetching initial data")
            response = get_data_from_source()
            if response and response.get('size_prediction'):
                full_issue_number = response['size_prediction'].get('issue', 'N/A')
                logger.info(f"✅ Data received, issue: {full_issue_number}")
                
                await safe_send_message(context, CHAT_ID, f"▶️ Starting prediction from mixpred.\nIssue: {full_issue_number}")
            else:
                logger.warning("⚠️ No size prediction data available")
                await safe_send_message(context, CHAT_ID, "▶️ Starting prediction from mixpred.\nIssue: N/A")
            
            if STICKERS["start"]:
                await safe_send_sticker(context, CHAT_ID, STICKERS["start"])
                logger.debug("🎭 Start sticker sent")
            
        except Exception as e:
            logger.error(f"❌ Error getting initial data: {e}")
            await safe_reply_text(update.message, "✅ Bot Started (with warnings)")
            
    except Exception as e:
        logger.error(f"❌ Error in start command: {e}")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"⏹️ Stop command received from user {update.effective_user.id}")
    
    try:
        bot_state["is_running"] = False
        
        # Update today's max level at the end of a session
        if bot_state['max_level_session'] > bot_state['today_max_level']:
            bot_state['today_max_level'] = bot_state['max_level_session']
            logger.info(f"🏆 New daily max level: {bot_state['today_max_level']}")
        
        save_state()
        
        summary = (
            f"⏹️ **Session Closed** ⏹️\n\n"
            f"Total Session Wins: **{bot_state['session_win_count']}**\n"
            f"Max Level Reached: **L{bot_state['max_level_session']}**"
        )
        await safe_reply_text(update.message, summary, parse_mode='Markdown')
        
        if STICKERS["stop"]:
            await safe_send_sticker(context, CHAT_ID, STICKERS["stop"])
            logger.debug("🎭 Stop sticker sent")
        
        logger.info(f"📊 Session ended: {bot_state['session_win_count']} wins, L{bot_state['max_level_session']} max")
        
    except Exception as e:
        logger.error(f"❌ Error in stop command: {e}")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"📊 Status command received from user {update.effective_user.id}")
    
    try:
        status_text = "Running" if bot_state["is_running"] else "Stopped"
        tz = pytz.timezone(bot_state['timezone'])
        
        now = datetime.now(tz)
        current_time_str = now.strftime('%I:%M:%S %p %Z')
        
        # Check for a new day and reset daily stats if needed
        current_date = now.strftime('%Y-%m-%d')
        if current_date != bot_state['today_date']:
            logger.info(f"📅 New day detected: {current_date}, resetting daily stats")
            bot_state['today_total_wins'] = 0
            bot_state['today_session_count'] = 0
            bot_state['today_max_level'] = 1
            bot_state['today_date'] = current_date
            save_state()

        message = (
            f"📊 **Bot Status** 📊\n\n"
            f"🕒 Current Time: **{current_time_str}**\n\n"
            f"**State**: {status_text}\n"
            f"**Mode**: `{bot_state['current_mode']}` (Target: {bot_state.get('win_target', 'N/A')})\n\n"
            f"**-- Current Session --**\n"
            f"Session Wins: **{bot_state['session_win_count']}**\n"
            f"Max Level This Session: **L{bot_state['max_level_session']}**\n\n"
            f"**-- Today's Stats --**\n"
            f"Today's Total Wins: **{bot_state['today_total_wins']}**\n"
            f"Today's Session Count: **{bot_state['today_session_count']}**\n"
            f"Today's Max Level: **L{bot_state['today_max_level']}**"
        )
        await safe_reply_text(update.message, message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"❌ Error in status command: {e}")

async def mode_winlimit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = 15
    if context.args:
        try:
            target = int(context.args[0])
            if target <= 0: target = 15
        except (ValueError, IndexError):
            target = 15
    
    bot_state["current_mode"] = "win_limit"
    bot_state["win_target"] = target
    save_state()
    
    logger.info(f"🎯 Mode changed to win-limit with target: {target}")
    await safe_reply_text(update.message, f"⚙️ **Mode changed to: Win-Limit** (Target: {target} wins)")

# === 11. BUTTON CALLBACK WITH ERROR HANDLING ===
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer()
        logger.debug(f"🔘 Button callback: {query.data}")
        
        action, value = query.data.split('_', 1)

        if action == "schedule":
            hour, minute = map(int, value.split(':'))
            await add_or_remove_schedule(hour, minute, context)
            
            # Safely delete the message before showing new schedule
            if await safe_delete_message(query.message):
                logger.debug("Old schedule message deleted")
            
            # Send new schedule as a fresh message instead of editing
            await safe_send_message(context, query.message.chat_id, 
                f"Schedule updated for {hour:02d}:{minute:02d}")
            
    except Exception as e:
        logger.error(f"❌ Error in button callback: {e}")
        try:
            await query.answer("❌ An error occurred")
        except:
            pass

# === 12. SCHEDULED JOB WITH ERROR HANDLING ===
async def scheduled_job(context: ContextTypes.DEFAULT_TYPE):
    if not bot_state["is_running"]:
        return

    logger.debug("🔄 Running scheduled prediction check...")

    try:
        logger.debug("🌐 Fetching data from source")
        response = get_data_from_source()
        
        if not response:
            logger.error("❌ No data received from source")
            return
            
        size_prediction_history = response.get('size_prediction_history', [])
        if len(size_prediction_history) < 2:
            logger.warning("⚠️ Insufficient size prediction history")
            return
            
        last_completed = size_prediction_history[1]
        
        if bot_state["last_predicted_issue"] == last_completed.get('issue'):
            predicted = last_completed.get('predicted_size')
            actual = last_completed.get('actual_size')
            
            logger.info(f"🎯 Result check - Predicted: {predicted}, Actual: {actual}")

            if predicted and actual and predicted == actual:  # WIN
                bot_state["session_win_count"] += 1
                bot_state["today_total_wins"] += 1
                win_count = bot_state["session_win_count"]
                
                logger.info(f"🎉 WIN! Session wins: {win_count}, Today total: {bot_state['today_total_wins']}")
                
                try:
                    win_sticker_id = STICKERS["wins"][win_count - 1]
                    if win_sticker_id:
                        await safe_send_sticker(context, CHAT_ID, win_sticker_id)
                        logger.debug(f"🎭 Win sticker {win_count} sent")
                    else: 
                        raise IndexError
                except (IndexError, KeyError, TypeError):
                    await safe_send_message(context, CHAT_ID, f"Win {win_count}")
                    logger.debug(f"💬 Win message {win_count} sent (no sticker)")
                
                bot_state["current_level"] = 1

                if bot_state['current_mode'] == 'win_limit' and bot_state['session_win_count'] >= bot_state['win_target']:
                    logger.info(f"🎯 Target reached! {bot_state['session_win_count']}/{bot_state['win_target']}")
                    
                    bot_state["is_running"] = False
                    
                    if bot_state['max_level_session'] > bot_state['today_max_level']:
                        bot_state['today_max_level'] = bot_state['max_level_session']
                    save_state()
                    
                    summary = (
                        f"🎯 **Target Reached & Session Closed** 🎯\n\n"
                        f"Total Session Wins: **{bot_state['session_win_count']}**\n"
                        f"Max Level Reached: **L{bot_state['max_level_session']}**"
                    )
                    await safe_send_message(context, CHAT_ID, summary, parse_mode='Markdown')
                    if STICKERS["stop"]:
                        await safe_send_sticker(context, CHAT_ID, STICKERS["stop"])
                    return
            else:  # LOSS
                bot_state["current_level"] += 1
                logger.info(f"❌ LOSS! Level increased to: {bot_state['current_level']}")
        
        if bot_state['current_level'] > bot_state['max_level_session']:
            bot_state['max_level_session'] = bot_state['current_level']
            logger.debug(f"📈 New session max level: {bot_state['max_level_session']}")

        next_pred = response.get('size_prediction', {})
        next_issue = next_pred.get('issue')
        next_size = next_pred.get('next_size', 'N/A')
        
        if next_issue and next_size:
            prediction_text = f"{next_issue[-4:]} {next_size.upper()}\nL{bot_state['current_level']}"
            await safe_send_message(context, CHAT_ID, prediction_text)
            
            bot_state["last_predicted_issue"] = next_issue
            save_state()
            
            logger.info(f"📤 Sent prediction: {next_issue[-4:]} {next_size.upper()} L{bot_state['current_level']}")
        else:
            logger.warning("⚠️ No next prediction data available")

    except KeyError as e:
        logger.error(f"❌ Missing key in data: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error in scheduled_job: {e}")

# === 13. OTHER FUNCTIONS ===
async def add_or_remove_schedule(hour: int, minute: int, context: ContextTypes.DEFAULT_TYPE):
    time_str = f"{hour:02d}:{minute:02d}"
    job_name = f"auto_session_{time_str.replace(':', '')}"
    tz = pytz.timezone(bot_state['timezone'])
    
    try:
        if time_str in bot_state['schedule']:
            # Remove existing schedule
            jobs = context.job_queue.get_jobs_by_name(job_name)
            for job in jobs:
                job.schedule_removal()
            del bot_state['schedule'][time_str]
            logger.info(f"❌ Removed schedule for {time_str}")
        else:
            # Add new schedule
            context.job_queue.run_daily(
                callback=auto_session_start,
                time=time(hour=hour, minute=minute, tzinfo=tz),
                name=job_name
            )
            bot_state['schedule'][time_str] = job_name
            logger.info(f"✅ Added schedule for {time_str}")
        
        save_state()
    except Exception as e:
        logger.error(f"❌ Error managing schedule: {e}")

async def auto_session_start(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🤖 Auto-session starting...")
    
    try:
        # Update daily session count
        bot_state['today_session_count'] += 1
        
        bot_state['current_mode'] = 'win_limit'
        bot_state['win_target'] = bot_state['auto_win_target']
        bot_state.update({
            "is_running": True, "current_level": 1,
            "session_win_count": 0, "max_level_session": 1,
            "last_predicted_issue": None
        })
        save_state()
        
        logger.info(f"🎯 Auto-session started with target: {bot_state['auto_win_target']} wins")
        
        await safe_send_message(context, CHAT_ID, f"🤖 Auto-session started! Target: {bot_state['auto_win_target']} wins.")
        if STICKERS["start"]:
            await safe_send_sticker(context, CHAT_ID, STICKERS["start"])
            
    except Exception as e:
        logger.error(f"❌ Error in auto session start: {e}")

# === 14. MAIN APPLICATION SETUP ===
def main():
    logger.info("🚀 Starting Telegram Bot application...")
    
    if not BOT_TOKEN:
        logger.critical("❌ BOT_TOKEN not found in environment variables!")
        print("!!! BOT_TOKEN not found. Please set it in Railway environment variables. !!!")
        sys.exit(1)

    if CHAT_ID == 0:
        logger.warning("⚠️ CHAT_ID not configured properly!")

    # Enhanced Application builder with increased timeouts
    application = (Application.builder()
                  .token(BOT_TOKEN)
                  .connect_timeout(30)
                  .read_timeout(30)
                  .write_timeout(30)
                  .pool_timeout(30)
                  .build())
    
    logger.info("✅ Telegram Application created with enhanced timeouts")
    
    # Add global error handler
    application.add_error_handler(error_handler)
    logger.info("✅ Global error handler registered")
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("mode_winlimit", mode_winlimit_command))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    logger.info("✅ Command handlers registered")

    # Setup job queue
    job_queue = application.job_queue
    
    # Setup main prediction job
    now = datetime.now()
    delay = (60 - now.second + 2) % 60
    if delay == 0: 
        delay = 60
    
    job_queue.run_repeating(scheduled_job, interval=60, first=delay)
    logger.info(f"⏰ Main prediction job scheduled (starts in {delay}s)")

    # Final startup log
    logger.info("=" * 60)
    logger.info("🤖 TELEGRAM BOT READY WITH ERROR HANDLING")
    logger.info("=" * 60)
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📱 Bot Token: ...{BOT_TOKEN[-6:]}")
    logger.info(f"💬 Chat ID: {CHAT_ID}")
    logger.info(f"🔗 Data Source: {'Redis' if not is_running_locally() else 'API (local)'}")
    logger.info(f"🎭 Stickers: {sticker_count} loaded")
    logger.info(f"🕐 Timezone: {bot_state['timezone']}")
    logger.info("🛡️ Enhanced error handling enabled")
    logger.info("🔇 HTTP request logging disabled")
    logger.info("=" * 60)

    try:
        logger.info("🔄 Starting bot polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.critical(f"💥 Fatal error during polling: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
