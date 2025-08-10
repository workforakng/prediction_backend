# ai_worker.py
import time
import os
import logging
import json
import signal
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import redis
import pytz
import random
import requests
from threading import Event

# Load .env file first
load_dotenv()

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'ai_worker')

# --- Enhanced Logging Setup ---
LOG_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
LOG_PATH = os.path.join(LOG_DIR, "ai_worker.log")
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging with Railway-specific formatting
log_format = "%(asctime)s - AI_WORKER - %(levelname)s - %(message)s"
handlers = [logging.StreamHandler()]  # Always include stream handler for Railway logs

# Add file handler if writable directory exists
if os.access(LOG_DIR, os.W_OK):
    try:
        file_handler = logging.FileHandler(LOG_PATH)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
    except Exception as e:
        print(f"Warning: Could not create file handler: {e}")

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=handlers
)
logger = logging.getLogger("ai_worker")

# Log Railway environment info
if is_railway_environment():
    logger.info(f"🚂 AI Worker running on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {LOG_DIR}")
else:
    logger.info("🏠 AI Worker running in local environment")

# Global shutdown event for graceful shutdown
shutdown_event = Event()

# --- Redis Connection ---
try:
    redis_client = redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,  # This eliminates the need for .decode('utf-8')
        socket_connect_timeout=15,
        socket_timeout=15,
        retry_on_timeout=True
    )
    redis_client.ping()
    logger.info("✅ Successfully connected to Redis from AI worker.")
except redis.exceptions.ConnectionError as e:
    logger.critical(f"❌ Could not connect to Redis from AI worker: {e}. Exiting.")
    sys.exit(1)

# --- Redis Keys ---
REDIS_HISTORY_KEY = "lottery:history"
REDIS_MAIN_PREDICTION_KEY = "latest_prediction_data"
REDIS_AI_FLAG_KEY = "lottery:ai_enabled"
REDIS_AI_PREDICTION_KEY = "lottery:ai_prediction"
REDIS_AI_PREDICTIONS_HISTORY_HASH = "lottery:ai_preds_history"

# Status tracking keys
REDIS_AI_WORKER_STATUS_KEY = "lottery:ai_worker_status"
REDIS_AI_WORKER_HEARTBEAT_KEY = "lottery:ai_worker_heartbeat"

# --- Redis Pub/Sub Channel for AI Trigger ---
REDIS_AI_TRIGGER_CHANNEL = "lottery:ai_trigger"  # MUST MATCH THE ONE IN worker.py

# --- Perplexity AI Configuration ---
PERPLEXITY_AI_MODEL = os.getenv("PERPLEXITY_AI_MODEL", "sonar")

PERPLEXITY_API_KEYS = [
    os.getenv("PERPLEXITY_API_KEY"),
    os.getenv("PERPLEXITY_API_KEY1")
]
PERPLEXITY_API_KEYS = [key for key in PERPLEXITY_API_KEYS if key]

if not PERPLEXITY_API_KEYS:
    logger.critical("❌ No Perplexity AI API keys found. Please set PERPLEXITY_API_KEY and/or PERPLEXITY_API_KEY1 in your .env file.")
    sys.exit(1)

logger.info(f"🔑 Loaded {len(PERPLEXITY_API_KEYS)} Perplexity AI API key(s)")

# --- Status Management Functions ---
def update_ai_worker_status(status, message=None):
    """Update AI worker status in Redis"""
    try:
        status_data = {
            "status": status,
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "environment": "railway" if is_railway_environment() else "local",
            "service_name": get_railway_service_name()
        }
        if message:
            status_data["message"] = message
        
        redis_client.set(REDIS_AI_WORKER_STATUS_KEY, json.dumps(status_data), ex=300)
        logger.debug(f"📊 AI Worker status updated: {status}")
    except Exception as e:
        logger.error(f"Failed to update AI worker status: {e}")

def send_ai_worker_heartbeat():
    """Send heartbeat to Redis"""
    try:
        heartbeat_data = {
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "environment": "railway" if is_railway_environment() else "local",
            "service_name": get_railway_service_name(),
            "ai_enabled": is_ai_enabled(),
            "api_keys_count": len(PERPLEXITY_API_KEYS)
        }
        redis_client.set(REDIS_AI_WORKER_HEARTBEAT_KEY, json.dumps(heartbeat_data), ex=120)
        logger.debug("💗 AI Worker heartbeat sent")
    except Exception as e:
        logger.error(f"Failed to send AI worker heartbeat: {e}")

# --- Helper Function: Check if AI is enabled ---
def is_ai_enabled():
    """Enhanced AI enabled check with proper value validation"""
    try:
        ai_flag_value = redis_client.get(REDIS_AI_FLAG_KEY)
        
        if ai_flag_value is None:
            return False
        
        # Convert to boolean - handle both string and integer values
        if isinstance(ai_flag_value, str):
            return ai_flag_value.lower() in ['1', 'true', 'yes', 'on', 'enabled']
        else:
            return bool(ai_flag_value)
            
    except Exception as e:
        logger.error(f"❌ Error checking AI enabled status: {e}")
        return False

# --- Perplexity AI Prediction Function ---
def get_perplexity_ai_prediction(history_numbers, api_key, current_issue_id, last_issue_info=None):
    """
    Gets a lottery prediction using the Perplexity AI API.
    Enhanced with better timeout and error handling while keeping it simple.
    """
    logger.info(f"🧠 Getting AI prediction for issue: {current_issue_id}")
    logger.info(f"🔍 History length: {len(history_numbers)}, API key: {bool(api_key)}")
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": f"AI-Worker-Railway/{get_railway_service_name()}" if is_railway_environment() else "AI-Worker-Local"
    }

    # --- Constructing the dynamic prompt text ---
    prompt_intro = """Your task is to predict the next single digit (0–9) in a sequence based on the provided historical lottery results.

### Analysis Guidelines:
- Examine recent trends and patterns in the sequence
- Consider numbers that are "due" (haven't appeared recently)  
- Look for cyclical patterns or repetitions
- Use statistical reasoning and pattern recognition
- Remember frequency alone doesn't determine outcomes

### Output Requirement:
**Output ONLY a single digit (0–9) as your final prediction - no explanation, no additional text**

### Input Data:"""
    
    current_3_digits_issue = str(current_issue_id)[-3:] if len(str(current_issue_id)) > 3 else str(current_issue_id)
    
    # Build feedback from last prediction if available
    feedback_text = ""
    if last_issue_info:
        try:
            last_issue_id = str(last_issue_info['issue'])
            last_3_digits_issue = last_issue_id[-3:] if len(last_issue_id) > 3 else last_issue_id
            last_prediction = last_issue_info.get('prediction')
            last_outcome = last_issue_info.get('outcome')

            if last_prediction is not None and last_outcome is not None:
                accuracy_note = "✅ CORRECT" if last_prediction == last_outcome else "❌ INCORRECT"
                feedback_text = f"\nPrevious Prediction Feedback:\nFor issue {last_3_digits_issue}: You predicted {last_prediction}, actual result was {last_outcome} ({accuracy_note})\nUse this feedback to improve your next prediction.\n"
                logger.info(f"🔍 Added feedback: predicted {last_prediction} vs actual {last_outcome}")
        except Exception as e:
            logger.warning(f"⚠️ Error building feedback text: {e}")
            feedback_text = ""
    
    # Format history for better readability
    history_display = ', '.join(map(str, history_numbers[-20:]))  # Show last 20 for context
    full_history_display = ', '.join(map(str, history_numbers))
    
    prompt_text = (
        prompt_intro +
        f"\nCurrent Issue (last 3 digits): {current_3_digits_issue}" +
        feedback_text +
        f"\nRecent History (last 20): {history_display}" +
        f"\nFull Historical Sequence ({len(history_numbers)} numbers): {full_history_display}" +
        "\n\n🔮 Predict the next single digit (0–9):" +
        "\n👉 **Output ONLY the predicted digit with no explanation**"
    )

    # --- LOG THE FINAL PROMPT HERE ---
    logger.info(f"📝 Generated prompt for Perplexity AI (length: {len(prompt_text)} chars)")
    logger.debug(f"📝 Full prompt:\n---\n{prompt_text}\n---")

    data = {
        "model": PERPLEXITY_AI_MODEL,
        "messages": [
            {"role": "system", "content": "You are an expert lottery prediction AI with advanced pattern recognition capabilities."},
            {"role": "user", "content": prompt_text}
        ],
        "temperature": 0.7,
        "max_tokens": 10,
        "top_p": 0.9
    }

    try:
        logger.info(f"🔍 Making API request to Perplexity AI (timeout: 60s)...")
        start_time = time.time()
        
        response = requests.post(
            "https://api.perplexity.ai/chat/completions", 
            headers=headers, 
            json=data, 
            timeout=60  # Increased from 30 to 60 seconds
        )
        
        end_time = time.time()
        logger.info(f"🔍 API response received in {end_time - start_time:.2f} seconds")
        logger.info(f"🔍 API response status: {response.status_code}")
        
        response.raise_for_status()
        
        response_json = response.json()
        logger.info(f"📡 Perplexity AI response structure: {list(response_json.keys())}")
        logger.debug(f"📡 Perplexity AI raw response: {json.dumps(response_json, indent=2)}")

        if 'choices' in response_json and len(response_json['choices']) > 0:
            message_content = response_json['choices'][0].get('message', {}).get('content', '').strip()
            logger.info(f"🎯 AI raw response content: '{message_content}'")
            
            try:
                # Handle multi-character responses by extracting first digit
                prediction_str = ''.join(filter(str.isdigit, message_content))
                logger.info(f"🔍 Extracted digits from response: '{prediction_str}'")
                
                if not prediction_str:
                    logger.warning(f"⚠️ No digits found in AI response: '{message_content}'")
                    return None
                
                predicted_number = int(prediction_str[0])  # Take first digit
                logger.info(f"🔍 First digit extracted: {predicted_number}")
                
                if 0 <= predicted_number <= 9:
                    logger.info(f"✅ Valid AI prediction: {predicted_number}")
                    return predicted_number
                else:
                    logger.warning(f"⚠️ AI returned out-of-range number: {predicted_number}")
                    return None
                    
            except (ValueError, IndexError) as e:
                logger.error(f"❌ Error parsing AI prediction from '{message_content}': {e}")
                return None
        else:
            logger.warning(f"⚠️ Perplexity AI response missing 'choices' or content: {response_json}")
            return None

    except requests.exceptions.HTTPError as http_err:
        status_code = getattr(http_err.response, 'status_code', 'Unknown')
        response_text = getattr(http_err.response, 'text', 'No response text')
        logger.error(f"❌ Perplexity AI HTTP error: {http_err}")
        logger.error(f"❌ Status Code: {status_code}, Response: {response_text}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"❌ Perplexity AI Connection error: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"❌ Perplexity AI Timeout error (60s): {timeout_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        logger.error(f"❌ Perplexity AI Request error: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"❌ Perplexity AI JSON decode error: {json_err}")
        if 'response' in locals():
            logger.error(f"❌ Raw response: {response.text}")
        return None
    except Exception as e:
        logger.error(f"💥 Unexpected error in AI prediction: {e}", exc_info=True)
        return None

# --- Configuration for retries ---
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 60

# --- History Constraints ---
MAX_HISTORY_TO_USE = 55
MIN_HISTORY_REQUIRED = 5

def process_ai_prediction(trigger_issue):
    """
    Encapsulates the core AI prediction logic to be called on trigger.
    Enhanced with better logging while keeping the core logic simple.
    """
    logger.info(f"🚀 AI prediction triggered for issue: {trigger_issue}")
    
    update_ai_worker_status("processing", f"Processing prediction for issue {trigger_issue}")

    if not is_ai_enabled():
        logger.info("⚠️ AI is disabled. Skipping prediction for triggered issue.")
        update_ai_worker_status("disabled", "AI predictions are disabled")
        return

    logger.info("✅ AI is ENABLED. Running prediction cycle...")

    # Get main prediction data (removed .decode('utf-8') since decode_responses=True)
    main_prediction_str = redis_client.get(REDIS_MAIN_PREDICTION_KEY)
    if not main_prediction_str:
        logger.warning(f"⚠️ No main prediction data found in Redis key '{REDIS_MAIN_PREDICTION_KEY}'. Skipping AI prediction.")
        update_ai_worker_status("warning", "No main prediction data available")
        return

    try:
        main_prediction_data = json.loads(main_prediction_str)  # No .decode needed
        actual_target_next_issue = main_prediction_data.get("next_issue")
        
        logger.info(f"🔍 Main prediction data - next_issue: {actual_target_next_issue}")
        
        if not actual_target_next_issue:
            logger.error(f"❌ Main prediction data missing 'next_issue' field. Data: {main_prediction_data}")
            update_ai_worker_status("error", "Invalid main prediction data")
            return

        # Issue alignment logic
        trigger_issue_int = int(trigger_issue)
        target_issue_int = int(actual_target_next_issue)
        
        if target_issue_int > trigger_issue_int:
            logger.warning(f"⚠️ Triggered for issue {trigger_issue} but main prediction targets {actual_target_next_issue}. Using {actual_target_next_issue}.")
            ai_prediction_issue = actual_target_next_issue
        elif target_issue_int < trigger_issue_int:
            logger.error(f"❌ Main prediction ({actual_target_next_issue}) is behind triggered issue ({trigger_issue}). Skipping.")
            update_ai_worker_status("error", "Prediction data misalignment")
            return
        else:
            ai_prediction_issue = actual_target_next_issue

        logger.info(f"🎯 Target issue for AI prediction: {ai_prediction_issue}")

    except json.JSONDecodeError as e:
        logger.error(f"❌ Error parsing main prediction JSON: {e}")
        update_ai_worker_status("error", f"Data parsing error: {str(e)}")
        return

    # Get history data
    raw_history_str = redis_client.get(REDIS_HISTORY_KEY)
    if not raw_history_str:
        logger.warning(f"⚠️ No history found in Redis key '{REDIS_HISTORY_KEY}'. Skipping prediction.")
        update_ai_worker_status("warning", "No history data available")
        return

    try:
        parsed_history = json.loads(raw_history_str)  # No .decode needed
        logger.info(f"📊 History data loaded: {len(parsed_history)} entries")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error parsing history JSON: {e}")
        update_ai_worker_status("error", f"History parsing error: {str(e)}")
        return

    # --- GATHERING DATA FOR THE FEEDBACK PROMPT ---
    last_issue_info = None
    last_prediction_issue = int(ai_prediction_issue) - 1
    
    try:
        last_prediction_data = redis_client.hget(REDIS_AI_PREDICTIONS_HISTORY_HASH, str(last_prediction_issue))
        if last_prediction_data:
            last_prediction_data_json = json.loads(last_prediction_data)  # No .decode needed
            last_prediction = last_prediction_data_json.get('ai_prediction')
            last_outcome = parsed_history.get(str(last_prediction_issue))

            if last_prediction is not None and last_outcome is not None:
                last_issue_info = {
                    "issue": last_prediction_issue,
                    "prediction": last_prediction,
                    "outcome": last_outcome
                }
                logger.info(f"📝 Previous prediction feedback: Issue {last_prediction_issue} - Predicted: {last_prediction}, Actual: {last_outcome}")
    except Exception as e:
        logger.warning(f"⚠️ Could not retrieve previous prediction data: {e}")

    # --- GAP LOGIC (keeping your working version) ---
    available_issues = sorted([int(k) for k in parsed_history.keys()])
    consecutive_history_issues_for_ai = []
    
    current_expected_issue = int(ai_prediction_issue) - 1

    for i in range(len(available_issues) - 1, -1, -1):
        if available_issues[i] == current_expected_issue:
            consecutive_history_issues_for_ai.insert(0, available_issues[i])
            current_expected_issue -= 1
            if len(consecutive_history_issues_for_ai) >= MAX_HISTORY_TO_USE:
                break
        elif available_issues[i] < current_expected_issue:
            logger.info(f"📋 Gap found in history before issue {current_expected_issue + 1}. Stopping consecutive history collection.")
            break
    
    logger.info(f"🔍 Consecutive history issues for AI: {len(consecutive_history_issues_for_ai)} entries")
    
    if len(consecutive_history_issues_for_ai) < MIN_HISTORY_REQUIRED:
        logger.warning(f"⚠️ Insufficient consecutive history: Required {MIN_HISTORY_REQUIRED}, Found {len(consecutive_history_issues_for_ai)}")
        update_ai_worker_status("warning", f"Insufficient history: {len(consecutive_history_issues_for_ai)}")
        return

    if consecutive_history_issues_for_ai and int(ai_prediction_issue) != consecutive_history_issues_for_ai[-1] + 1:
        logger.warning(f"⚠️ Target issue ({ai_prediction_issue}) doesn't follow last consecutive history issue ({consecutive_history_issues_for_ai[-1]})")
        update_ai_worker_status("warning", "History alignment issue")
        return

    history_numbers_for_ai = [parsed_history[str(issue)] for issue in consecutive_history_issues_for_ai]
    logger.info(f"📊 Numbers being fed to AI (last {len(history_numbers_for_ai)} consecutive entries): {history_numbers_for_ai[-10:]}")

    # --- AI PREDICTION WITH RETRY LOGIC ---
    ai_prediction = None
    backoff_time = INITIAL_BACKOFF_SECONDS
    used_api_keys = []
    
    for attempt in range(MAX_RETRIES):
        # Check for shutdown
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during AI prediction attempts")
            return
            
        # Select API key (rotate to avoid rate limits)
        available_keys = [key for key in PERPLEXITY_API_KEYS if key not in used_api_keys]
        if not available_keys:
            available_keys = PERPLEXITY_API_KEYS
            used_api_keys.clear()
            
        current_api_key = random.choice(available_keys)
        used_api_keys.append(current_api_key)
        
        logger.info(f"🔑 Attempt {attempt + 1}/{MAX_RETRIES} using API key ending in ...{current_api_key[-4:]}...")
        
        try:
            ai_prediction = get_perplexity_ai_prediction(
                history_numbers_for_ai, 
                api_key=current_api_key, 
                current_issue_id=ai_prediction_issue, 
                last_issue_info=last_issue_info
            )
            
            if ai_prediction is not None:
                logger.info(f"🧠 AI Prediction successful for issue {ai_prediction_issue}: {ai_prediction}")
                break
            else:
                logger.warning(f"⚠️ Attempt {attempt + 1} returned None prediction")
        except Exception as api_e:
            logger.error(f"❌ Attempt {attempt + 1} failed: {api_e}")

        # Exponential backoff before retry
        if attempt < MAX_RETRIES - 1:
            sleep_for = backoff_time + random.uniform(0, backoff_time * 0.2)
            logger.info(f"⏳ Sleeping {sleep_for:.2f}s before retry...")
            time.sleep(sleep_for)
            backoff_time = min(backoff_time * 2, MAX_BACKOFF_SECONDS)
    
    # Handle prediction result
    if ai_prediction is None:
        logger.error(f"❌ All {MAX_RETRIES} attempts failed for AI prediction")
        update_ai_worker_status("prediction_failed", f"All {MAX_RETRIES} attempts failed")
        category = None
    else:
        category = "small" if ai_prediction <= 4 else "big"
        logger.info(f"🎯 Final AI prediction: {ai_prediction} (category: {category})")

    # --- SAVE PREDICTION DATA ---
    prediction_data = {
        "issue": ai_prediction_issue,
        "ai_prediction": ai_prediction,
        "ai_predicted_category": category,
        "timestamp": datetime.now(pytz.utc).isoformat(),
        "environment": "railway" if is_railway_environment() else "local",
        "attempts_used": attempt + 1 if ai_prediction is not None else MAX_RETRIES
    }
    
    try:
        redis_client.set(REDIS_AI_PREDICTION_KEY, json.dumps(prediction_data))
        logger.info(f"💾 AI prediction data saved successfully for issue {ai_prediction_issue}")

        redis_client.hset(REDIS_AI_PREDICTIONS_HISTORY_HASH, str(ai_prediction_issue), json.dumps({
            "ai_prediction": prediction_data["ai_prediction"],
            "ai_predicted_category": prediction_data["ai_predicted_category"],
            "timestamp": prediction_data["timestamp"]
        }))
        logger.info(f"💾 AI prediction history saved for issue {ai_prediction_issue}")
        
        update_ai_worker_status("prediction_complete", f"Prediction {ai_prediction} saved for issue {ai_prediction_issue}")
        
    except Exception as save_error:
        logger.error(f"❌ Error saving AI prediction data: {save_error}")
        update_ai_worker_status("save_error", f"Failed to save prediction: {str(save_error)}")

def ai_listener_loop():
    """
    Main loop for the AI worker, listening for triggers from the main worker.
    Enhanced with better error handling and monitoring.
    """
    logger.info("🚀 AI Worker started. Listening for triggers on Redis Pub/Sub channel...")
    update_ai_worker_status("starting", "AI Worker initializing")
    
    pubsub = redis_client.pubsub()
    pubsub.subscribe(REDIS_AI_TRIGGER_CHANNEL)
    logger.info(f"📡 Subscribed to channel: {REDIS_AI_TRIGGER_CHANNEL}")
    
    last_heartbeat = time.time()
    last_ai_status_check = time.time()
    reconnection_attempts = 0
    max_reconnection_attempts = 5
    
    update_ai_worker_status("listening", "Listening for prediction triggers")

    while not shutdown_event.is_set():
        try:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            
            # Send periodic heartbeat
            current_time = time.time()
            if current_time - last_heartbeat >= 30:  # Every 30 seconds
                send_ai_worker_heartbeat()
                last_heartbeat = current_time
            
            # Check AI enabled status periodically
            if current_time - last_ai_status_check >= 10:  # Every 10 seconds
                current_ai_status = is_ai_enabled()
                logger.info(f"🤖 AI Status Check: {'ENABLED' if current_ai_status else 'DISABLED'}")
                last_ai_status_check = current_time
                
                if not current_ai_status:
                    update_ai_worker_status("disabled", "AI predictions disabled")
                    time.sleep(5)  # Sleep when disabled
                    continue
                else:
                    update_ai_worker_status("listening", "AI enabled - listening for triggers")

            # Process message if received
            if message and message['type'] == 'message':
                try:
                    logger.info(f"📨 ===== AI TRIGGER RECEIVED =====")
                    logger.info(f"📨 Raw message: {message}")
                    logger.info(f"📨 Message data: {message['data']}")
                    
                    data = json.loads(message['data'])  # No .decode needed
                    trigger_issue = data.get('issue')
                    trigger_type = data.get('trigger_type', 'unknown')
                    
                    logger.info(f"📨 Parsed trigger - Type: {trigger_type}, Issue: {trigger_issue}")
                    
                    if trigger_issue:
                        logger.info(f"📨 Received AI trigger: {trigger_type} for issue {trigger_issue}")
                        process_ai_prediction(trigger_issue)
                        reconnection_attempts = 0  # Reset on successful processing
                    else:
                        logger.warning(f"⚠️ Malformed trigger message (missing 'issue'): {data}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to decode pub/sub message: {message['data']}. Error: {e}")
                except Exception as e:
                    logger.error(f"❌ Error processing pub/sub message: {e}", exc_info=True)

        except redis.exceptions.ConnectionError as e:
            reconnection_attempts += 1
            logger.error(f"❌ Redis Pub/Sub connection lost (attempt {reconnection_attempts}): {e}")
            update_ai_worker_status("connection_error", f"Redis connection lost (attempt {reconnection_attempts})")
            
            if reconnection_attempts >= max_reconnection_attempts:
                logger.critical(f"💥 Max reconnection attempts ({max_reconnection_attempts}) reached. Exiting.")
                break
                
            # Attempt to reconnect
            retry_delay = min(5 * reconnection_attempts, 30)  # Max 30s delay
            logger.info(f"⏳ Attempting to reconnect in {retry_delay} seconds...")
            time.sleep(retry_delay)
            
            try:
                redis_client.ping()
                pubsub = redis_client.pubsub()
                pubsub.subscribe(REDIS_AI_TRIGGER_CHANNEL)
                logger.info("✅ Successfully reconnected to Redis Pub/Sub")
                update_ai_worker_status("reconnected", "Redis connection restored")
                reconnection_attempts = 0
            except Exception as reconnect_e:
                logger.error(f"❌ Failed to reconnect: {reconnect_e}")
                
        except Exception as e:
            logger.error(f"💥 Unexpected error in AI listener loop: {e}", exc_info=True)
            update_ai_worker_status("loop_error", f"Listener error: {str(e)}")
            time.sleep(5)

# --- Graceful Shutdown Handling ---
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    signal_name = signal.Signals(signum).name
    logger.info(f"🛑 Received {signal_name} signal. Initiating graceful shutdown...")
    update_ai_worker_status("shutting_down", f"Received {signal_name} signal")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 AI WORKER STARTING")
    logger.info("=" * 60)
    
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📁 Data directory: {LOG_DIR}")
    logger.info(f"🔗 Redis: Connected")
    logger.info(f"🔑 API Keys: {len(PERPLEXITY_API_KEYS)} configured")
    logger.info(f"🤖 AI Model: {PERPLEXITY_AI_MODEL}")
    logger.info(f"📡 Trigger Channel: {REDIS_AI_TRIGGER_CHANNEL}")
    
    # Test AI status on startup
    ai_status = is_ai_enabled()
    logger.info(f"🎯 AI Status: {'ENABLED' if ai_status else 'DISABLED'}")
    
    try:
        # Start the main listener loop
        ai_listener_loop()
    except Exception as e:
        logger.critical(f"💥 Fatal error in main execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("👋 AI Worker terminated")
        update_ai_worker_status("stopped", "AI Worker stopped")
