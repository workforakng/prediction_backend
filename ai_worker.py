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
from threading import Event, Lock
import traceback

# Load .env file first
load_dotenv()

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'ai_worker')

# --- Enhanced Logging Setup with Railway Support ---
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.join(DATA_DIR, "ai_worker.log")

# Configure logging with Railway-specific formatting
log_format = "%(asctime)s - AI_WORKER - %(levelname)s - %(message)s"
handlers = [logging.StreamHandler()]  # Always include stream handler for Railway logs

# Add file handler if writable directory exists
if os.access(DATA_DIR, os.W_OK):
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
    logger.info(f"📁 Data directory: {DATA_DIR}")
else:
    logger.info("🏠 AI Worker running in local environment")

# Global shutdown event for graceful shutdown
shutdown_event = Event()
redis_lock = Lock()

# --- Enhanced Redis Connection with Railway Support ---
redis_client = None

def initialize_redis():
    """Initialize Redis connection with retry logic"""
    global redis_client
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            redis_client = redis.from_url(
                redis_url, 
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
                retry_on_timeout=True,
                health_check_interval=30
            )
            redis_client.ping()
            logger.info(f"✅ Successfully connected to Redis (attempt {attempt + 1})")
            logger.info(f"🔗 Redis URL: {redis_url[:25]}...")
            return True
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Redis connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.critical(f"💥 Failed to connect to Redis after {max_retries} attempts")
                return False
        except Exception as e:
            logger.error(f"❌ Unexpected Redis error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.critical(f"💥 Unexpected Redis error after {max_retries} attempts")
                return False
    return False

# Initialize Redis connection
if not initialize_redis():
    logger.critical("❌ Could not initialize Redis connection. Exiting.")
    sys.exit(1)

# --- Redis Keys ---
REDIS_HISTORY_KEY = "lottery:history"
REDIS_MAIN_PREDICTION_KEY = "latest_prediction_data"
REDIS_AI_FLAG_KEY = "lottery:ai_enabled"
REDIS_AI_PREDICTION_KEY = "lottery:ai_prediction"
REDIS_AI_PREDICTIONS_HISTORY_HASH = "lottery:ai_preds_history"
REDIS_AI_BIG_SMALL_ACCURACY_KEY = "lottery:ai_big_small_accuracy"

# Status tracking keys
REDIS_AI_WORKER_STATUS_KEY = "lottery:ai_worker_status"
REDIS_AI_WORKER_HEARTBEAT_KEY = "lottery:ai_worker_heartbeat"

# --- Redis Pub/Sub Channel for AI Trigger ---
REDIS_AI_TRIGGER_CHANNEL = "lottery:ai_trigger"  # MUST MATCH THE ONE IN worker.py

# --- Enhanced Perplexity AI Configuration ---
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

# --- Enhanced Configuration ---
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 60
MAX_HISTORY_TO_USE = 55
MIN_HISTORY_REQUIRED = 5
HEARTBEAT_INTERVAL = 30  # seconds

# --- Status Management Functions ---
def update_ai_worker_status(status, message=None):
    """Update AI worker status in Redis"""
    try:
        if redis_client:
            with redis_lock:
                status_data = {
                    "status": status,
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "service_name": get_railway_service_name()
                }
                if message:
                    status_data["message"] = message
                
                redis_client.set(REDIS_AI_WORKER_STATUS_KEY, json.dumps(status_data), ex=300)  # 5 min expiry
                logger.debug(f"📊 AI Worker status updated: {status}")
    except Exception as e:
        logger.error(f"Failed to update AI worker status: {e}")

def send_ai_worker_heartbeat():
    """Send heartbeat to Redis"""
    try:
        if redis_client:
            with redis_lock:
                heartbeat_data = {
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "service_name": get_railway_service_name(),
                    "ai_enabled": is_ai_enabled(),
                    "api_keys_count": len(PERPLEXITY_API_KEYS)
                }
                redis_client.set(REDIS_AI_WORKER_HEARTBEAT_KEY, json.dumps(heartbeat_data), ex=120)  # 2 min expiry
                logger.debug("💗 AI Worker heartbeat sent")
    except Exception as e:
        logger.error(f"Failed to send AI worker heartbeat: {e}")

# --- Enhanced Helper Functions ---
def is_ai_enabled():
    """Enhanced AI enabled check with proper value validation"""
    try:
        if not redis_client:
            logger.warning("⚠️ Redis client not available for AI enabled check")
            return False
        
        with redis_lock:
            # Get the actual value instead of just checking existence
            ai_flag_value = redis_client.get(REDIS_AI_FLAG_KEY)
        
        if ai_flag_value is None:
            logger.debug(f"🤖 AI flag key '{REDIS_AI_FLAG_KEY}' does not exist")
            return False
        
        # Convert to boolean - handle both string and integer values
        enabled = False
        if isinstance(ai_flag_value, str):
            enabled = ai_flag_value.lower() in ['1', 'true', 'yes', 'on', 'enabled']
        elif isinstance(ai_flag_value, (int, float)):
            enabled = bool(ai_flag_value)
        else:
            # Try to convert to string and check
            enabled = str(ai_flag_value).lower() in ['1', 'true', 'yes', 'on', 'enabled']
        
        logger.debug(f"🤖 AI flag value: '{ai_flag_value}' -> enabled: {enabled}")
        return enabled
        
    except Exception as e:
        logger.error(f"❌ Error checking AI enabled status: {e}")
        return False

def debug_redis_data():
    """Debug function to check Redis data availability"""
    try:
        logger.info("🔍 ===== DEBUGGING REDIS DATA =====")
        
        with redis_lock:
            # Check main prediction data
            main_pred = redis_client.get(REDIS_MAIN_PREDICTION_KEY)
            logger.info(f"🔍 Main prediction data ({REDIS_MAIN_PREDICTION_KEY}):")
            if main_pred:
                logger.info(f"    📄 Raw data length: {len(main_pred)}")
                try:
                    main_pred_json = json.loads(main_pred)
                    logger.info(f"    📋 Next issue: {main_pred_json.get('next_issue')}")
                    logger.info(f"    📋 Keys: {list(main_pred_json.keys())}")
                except json.JSONDecodeError as e:
                    logger.error(f"    ❌ JSON decode error: {e}")
            else:
                logger.warning(f"    ❌ No data found for key {REDIS_MAIN_PREDICTION_KEY}")
            
            # Check history data
            history = redis_client.get(REDIS_HISTORY_KEY)
            if history:
                try:
                    history_data = json.loads(history)
                    logger.info(f"🔍 History data ({REDIS_HISTORY_KEY}):")
                    logger.info(f"    📊 Total entries: {len(history_data)}")
                    if history_data:
                        sorted_keys = sorted(history_data.keys(), key=int)
                        logger.info(f"    📋 Last 5 keys: {sorted_keys[-5:]}")
                        logger.info(f"    📋 Last 5 values: {[history_data[k] for k in sorted_keys[-5:]]}")
                except json.JSONDecodeError as e:
                    logger.error(f"🔍 History JSON decode error: {e}")
            else:
                logger.warning(f"🔍 History data ({REDIS_HISTORY_KEY}): ❌ None")
            
            # Check AI enabled flag
            ai_flag = redis_client.get(REDIS_AI_FLAG_KEY)
            logger.info(f"🔍 AI enabled flag ({REDIS_AI_FLAG_KEY}): {ai_flag}")
            
            # Check all relevant Redis keys
            all_keys = redis_client.keys("lottery:*")
            logger.info(f"🔍 All lottery keys in Redis: {len(all_keys)} keys")
            
        logger.info("🔍 ===== END REDIS DEBUG =====")
        
    except Exception as e:
        logger.error(f"❌ Debug Redis data error: {e}", exc_info=True)

def validate_api_response(response_data):
    """Validate Perplexity AI response structure"""
    try:
        if not isinstance(response_data, dict):
            logger.warning("⚠️ API response is not a dictionary")
            return False
        
        if 'choices' not in response_data:
            logger.warning("⚠️ API response missing 'choices' field")
            return False
        
        choices = response_data['choices']
        if not isinstance(choices, list) or len(choices) == 0:
            logger.warning("⚠️ API response 'choices' is empty or not a list")
            return False
        
        first_choice = choices[0]
        if not isinstance(first_choice, dict):
            logger.warning("⚠️ First choice is not a dictionary")
            return False
        
        if 'message' not in first_choice:
            logger.warning("⚠️ First choice missing 'message' field")
            return False
        
        message = first_choice['message']
        if not isinstance(message, dict) or 'content' not in message:
            logger.warning("⚠️ Message missing 'content' field")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error validating API response: {e}")
        return False

# --- Enhanced Perplexity AI Prediction Function ---
def get_perplexity_ai_prediction(history_numbers, api_key, current_issue_id, last_issue_info=None):
    """Enhanced AI prediction with comprehensive error handling and detailed logging"""
    try:
        logger.info(f"🧠 ===== STARTING AI PREDICTION =====")
        logger.info(f"🧠 Getting AI prediction for issue: {current_issue_id}")
        logger.info(f"🔍 History length provided: {len(history_numbers) if history_numbers else 0}")
        logger.info(f"🔍 API key provided: {bool(api_key)}")
        logger.info(f"🔍 Last issue info provided: {bool(last_issue_info)}")
        
        update_ai_worker_status("predicting", f"Getting prediction for issue {current_issue_id}")
        
        if not history_numbers:
            logger.error("❌ No history numbers provided for prediction")
            return None
        
        if not api_key:
            logger.error("❌ No API key provided")
            return None
        
        # Validate input parameters
        if len(history_numbers) < MIN_HISTORY_REQUIRED:
            logger.warning(f"⚠️ Insufficient history for prediction: {len(history_numbers)} < {MIN_HISTORY_REQUIRED}")
            return None
        
        logger.info("🔍 Building API request headers...")
        
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"AI-Worker-Railway/{get_railway_service_name()}" if is_railway_environment() else "AI-Worker-Local"
        }

        logger.info("🔍 Constructing prompt...")

        # --- Enhanced Prompt Construction ---
        prompt_intro = f"""
Your task is to predict the next single digit (0–9) in a sequence based on the provided historical lottery results.

### Analysis Guidelines:
- Examine recent trends and patterns in the sequence
- Consider numbers that are "due" (haven't appeared recently)
- Look for cyclical patterns or repetitions
- Use statistical reasoning and pattern recognition
- Remember frequency alone doesn't determine outcomes

### Critical Thinking:
- Analyze the last 10-15 numbers for immediate patterns
- Consider mathematical sequences or progressions
- Look for alternating patterns or clusters
- Factor in randomness while seeking logical patterns

### Output Requirement:
**Output ONLY a single digit (0–9) as your final prediction - no explanation, no additional text**

### Input Data:
"""
        
        # Format current issue identifier
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
                    feedback_text = f"\n### Previous Prediction Feedback:\nFor issue {last_3_digits_issue}: You predicted {last_prediction}, actual result was {last_outcome} ({accuracy_note})\nUse this feedback to improve your next prediction.\n"
                    logger.info(f"🔍 Added feedback for previous prediction: {last_prediction} vs {last_outcome}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error building feedback text: {e}")
                feedback_text = ""
        
        # Format history for better readability
        history_display = ', '.join(map(str, history_numbers[-20:]))  # Show last 20 for context
        full_history_display = ', '.join(map(str, history_numbers))
        
        logger.info(f"🔍 Recent history (last 10): {', '.join(map(str, history_numbers[-10:]))}")
        
        # Construct final prompt
        prompt_text = (
            prompt_intro +
            f"Current Issue (last 3 digits): {current_3_digits_issue}\n" +
            feedback_text +
            f"\nRecent History (last 20): {history_display}\n" +
            f"Full Historical Sequence ({len(history_numbers)} numbers): {full_history_display}\n" +
            "\n🔮 **Predict the next single digit (0–9):**\n" +
            "👉 **Output ONLY the predicted digit with no explanation**"
        )

        # Log the prompt for debugging (truncated)
        logger.info(f"📝 Generated prompt for Perplexity AI (issue {current_issue_id}):")
        logger.info(f"📝 Prompt length: {len(prompt_text)} characters")

        # Prepare API request data
        data = {
            "model": PERPLEXITY_AI_MODEL,
            "messages": [
                {
                    "role": "system", 
                    "content": "You are an expert lottery prediction AI with advanced pattern recognition capabilities. You analyze sequences and provide single-digit predictions based on mathematical and statistical analysis."
                },
                {
                    "role": "user", 
                    "content": prompt_text
                }
            ],
            "temperature": 0.7,
            "max_tokens": 10,
            "top_p": 0.9
        }

        logger.info(f"🔍 Making API request to Perplexity AI...")
        logger.info(f"🔍 Request timeout: 30 seconds")
        
        # Make API request with enhanced error handling
        start_time = time.time()
        response = requests.post(
            "https://api.perplexity.ai/chat/completions", 
            headers=headers, 
            json=data, 
            timeout=30
        )
        end_time = time.time()
        
        logger.info(f"🔍 API response received in {end_time - start_time:.2f} seconds")
        logger.info(f"🔍 API response status: {response.status_code}")
        
        # Check response status
        response.raise_for_status()
        
        # Parse response
        response_json = response.json()
        logger.info(f"📡 Perplexity AI response structure: {list(response_json.keys())}")

        # Validate response structure
        if not validate_api_response(response_json):
            logger.error("❌ Invalid API response structure")
            return None
        
        # Extract prediction
        message_content = response_json['choices'][0]['message']['content'].strip()
        logger.info(f"🎯 AI raw response content: '{message_content}'")
        
        # Parse and validate prediction
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
                logger.info(f"✅ Valid AI prediction extracted: {predicted_number}")
                logger.info(f"🧠 ===== AI PREDICTION COMPLETE =====")
                return predicted_number
            else:
                logger.warning(f"⚠️ AI returned out-of-range number: {predicted_number}")
                return None
                
        except (ValueError, IndexError) as e:
            logger.error(f"❌ Error parsing AI prediction from '{message_content}': {e}")
            return None

    except requests.exceptions.HTTPError as http_err:
        status_code = getattr(http_err.response, 'status_code', 'Unknown')
        response_text = getattr(http_err.response, 'text', 'No response text')
        logger.error(f"❌ Perplexity AI HTTP error: {http_err}")
        logger.error(f"❌ Status Code: {status_code}")
        logger.error(f"❌ Response Text: {response_text}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"❌ Perplexity AI Connection error: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"❌ Perplexity AI Timeout error: {timeout_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        logger.error(f"❌ Perplexity AI Request error: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"❌ Perplexity AI JSON decode error: {json_err}")
        return None
    except Exception as e:
        logger.error(f"💥 Unexpected error in AI prediction: {e}", exc_info=True)
        return None

def calculate_ai_big_small_accuracy():
    """Calculate accuracy for AI big/small predictions"""
    try:
        logger.debug("📊 Calculating AI big/small accuracy...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for accuracy calculation")
            return
        
        with redis_lock:
            # Get AI prediction history
            ai_history = redis_client.hgetall(REDIS_AI_PREDICTIONS_HISTORY_HASH)
            # Get actual results
            actual_history_raw = redis_client.get(REDIS_HISTORY_KEY)
        
        if not ai_history or not actual_history_raw:
            logger.warning("⚠️ Insufficient data for AI accuracy calculation")
            return
        
        try:
            actual_history = json.loads(actual_history_raw)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing actual history: {e}")
            return
        
        total_predictions = 0
        correct_predictions = 0
        big_correct = 0
        small_correct = 0
        big_total = 0
        small_total = 0
        
        for issue_str, prediction_data_str in ai_history.items():
            try:
                prediction_data = json.loads(prediction_data_str)
                ai_prediction = prediction_data.get('ai_prediction')
                actual_result = actual_history.get(issue_str)
                
                if ai_prediction is not None and actual_result is not None:
                    total_predictions += 1
                    
                    # Determine categories
                    ai_category = "small" if ai_prediction <= 4 else "big"
                    actual_category = "small" if actual_result <= 4 else "big"
                    
                    if ai_category == "big":
                        big_total += 1
                        if ai_category == actual_category:
                            big_correct += 1
                            correct_predictions += 1
                    else:
                        small_total += 1
                        if ai_category == actual_category:
                            small_correct += 1
                            correct_predictions += 1
                            
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"⚠️ Error processing AI prediction for issue {issue_str}: {e}")
                continue
        
        # Calculate accuracy percentages
        overall_accuracy = (correct_predictions / total_predictions * 100) if total_predictions > 0 else 0
        big_accuracy = (big_correct / big_total * 100) if big_total > 0 else 0
        small_accuracy = (small_correct / small_total * 100) if small_total > 0 else 0
        
        accuracy_data = {
            "total_predictions": total_predictions,
            "correct_predictions": correct_predictions,
            "overall_accuracy_percentage": round(overall_accuracy, 2),
            "big_predictions": {
                "total": big_total,
                "correct": big_correct,
                "accuracy_percentage": round(big_accuracy, 2)
            },
            "small_predictions": {
                "total": small_total,
                "correct": small_correct,
                "accuracy_percentage": round(small_accuracy, 2)
            },
            "timestamp": datetime.now(pytz.utc).isoformat()
        }
        
        with redis_lock:
            redis_client.set(REDIS_AI_BIG_SMALL_ACCURACY_KEY, json.dumps(accuracy_data))
        
        logger.info(f"📊 AI accuracy updated: {correct_predictions}/{total_predictions} ({overall_accuracy:.2f}%)")
        
    except Exception as e:
        logger.error(f"❌ Error calculating AI accuracy: {e}")

def process_ai_prediction(trigger_issue):
    """Enhanced AI prediction processing with comprehensive error handling"""
    try:
        logger.info(f"🚀 ===== AI PREDICTION PROCESSING START =====")
        logger.info(f"🚀 AI prediction triggered for issue: {trigger_issue}")
        
        update_ai_worker_status("processing", f"Processing prediction for issue {trigger_issue}")
        send_ai_worker_heartbeat()

        logger.info("🔍 Step 1: Checking if AI is enabled...")
        
        # Check if AI is enabled
        if not is_ai_enabled():
            logger.info("⚠️ AI is disabled. Skipping prediction for triggered issue.")
            update_ai_worker_status("disabled", "AI predictions are disabled")
            return

        logger.info("✅ AI is ENABLED. Running prediction cycle...")
        logger.info("🔍 Step 2: Checking for shutdown signal...")

        # Check for shutdown signal
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during prediction processing")
            return

        logger.info("🔍 Step 3: Running Redis data debug check...")
        
        # Debug Redis data first
        debug_redis_data()

        logger.info("🔍 Step 4: Getting main prediction data from Redis...")

        # Get main prediction data
        with redis_lock:
            main_prediction_str = redis_client.get(REDIS_MAIN_PREDICTION_KEY)
        
        logger.info(f"🔍 Step 4 Result: main_prediction_str exists = {main_prediction_str is not None}")
        
        if not main_prediction_str:
            logger.warning(f"⚠️ No main prediction data found in Redis key '{REDIS_MAIN_PREDICTION_KEY}'. Skipping AI prediction.")
            update_ai_worker_status("warning", "No main prediction data available")
            return

        logger.info("🔍 Step 5: Parsing main prediction data...")
        
        try:
            main_prediction_data = json.loads(main_prediction_str)
            actual_target_next_issue = main_prediction_data.get("next_issue")
            
            logger.info(f"🔍 Step 5 Result: target issue = {actual_target_next_issue}")
            
            if not actual_target_next_issue:
                logger.error(f"❌ Main prediction data missing 'next_issue' field.")
                update_ai_worker_status("error", "Invalid main prediction data")
                return

            logger.info("🔍 Step 6: Validating issue alignment...")

            # Validate issue alignment
            trigger_issue_int = int(trigger_issue)
            target_issue_int = int(actual_target_next_issue)
            
            logger.info(f"🔍 Step 6: trigger_issue_int = {trigger_issue_int}, target_issue_int = {target_issue_int}")
            
            if target_issue_int > trigger_issue_int:
                logger.warning(f"⚠️ Triggered for issue {trigger_issue} but main prediction targets {actual_target_next_issue}. Using {actual_target_next_issue}.")
                ai_prediction_issue = actual_target_next_issue
            elif target_issue_int < trigger_issue_int:
                logger.error(f"❌ Main prediction ({actual_target_next_issue}) is behind triggered issue ({trigger_issue}). Skipping.")
                update_ai_worker_status("error", "Prediction data misalignment")
                return
            else:
                ai_prediction_issue = actual_target_next_issue

            logger.info(f"🎯 Final target issue for AI prediction: {ai_prediction_issue}")

        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"❌ Error parsing main prediction data: {e}", exc_info=True)
            update_ai_worker_status("error", f"Data parsing error: {str(e)}")
            return

        logger.info("🔍 Step 7: Getting history data from Redis...")

        # Get history data
        with redis_lock:
            raw_history_str = redis_client.get(REDIS_HISTORY_KEY)
        
        logger.info(f"🔍 Step 7 Result: history data exists = {raw_history_str is not None}")
        
        if not raw_history_str:
            logger.warning(f"⚠️ No history found in Redis key '{REDIS_HISTORY_KEY}'. Skipping prediction.")
            update_ai_worker_status("warning", "No history data available")
            return

        logger.info("🔍 Step 8: Parsing history data...")

        try:
            parsed_history = json.loads(raw_history_str)
            logger.info(f"🔍 Step 8 Result: parsed history has {len(parsed_history)} entries")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing history data: {e}")
            update_ai_worker_status("error", f"History parsing error: {str(e)}")
            return

        logger.info("🔍 Step 9: Gathering previous prediction feedback...")

        # --- Gather previous prediction feedback ---
        last_issue_info = None
        last_prediction_issue = int(ai_prediction_issue) - 1
        
        try:
            with redis_lock:
                last_prediction_data = redis_client.hget(REDIS_AI_PREDICTIONS_HISTORY_HASH, str(last_prediction_issue))
            
            if last_prediction_data:
                last_prediction_data_json = json.loads(last_prediction_data)
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

        logger.info("🔍 Step 10: Enhanced history validation with gap detection...")

        # --- Enhanced history validation with gap detection ---
        available_issues = sorted([int(k) for k in parsed_history.keys()])
        consecutive_history_issues_for_ai = []
        
        logger.info(f"🔍 Step 10: Available issues count: {len(available_issues)}")
        
        current_expected_issue = int(ai_prediction_issue) - 1
        logger.info(f"🔍 Step 10: Starting from expected issue: {current_expected_issue}")

        for i in range(len(available_issues) - 1, -1, -1):
            if available_issues[i] == current_expected_issue:
                consecutive_history_issues_for_ai.insert(0, available_issues[i])
                current_expected_issue -= 1
                if len(consecutive_history_issues_for_ai) >= MAX_HISTORY_TO_USE:
                    break
            elif available_issues[i] < current_expected_issue:
                logger.info(f"📋 Gap detected in history before issue {current_expected_issue + 1}. Stopping consecutive collection.")
                break
        
        logger.info(f"🔍 Step 10 Result: consecutive_history_issues_for_ai count = {len(consecutive_history_issues_for_ai)}")
        
        if len(consecutive_history_issues_for_ai) < MIN_HISTORY_REQUIRED:
            logger.warning(f"⚠️ Insufficient consecutive history: Required {MIN_HISTORY_REQUIRED}, Found {len(consecutive_history_issues_for_ai)}")
            update_ai_worker_status("warning", f"Insufficient history: {len(consecutive_history_issues_for_ai)}")
            return

        if consecutive_history_issues_for_ai and int(ai_prediction_issue) != consecutive_history_issues_for_ai[-1] + 1:
            logger.warning(f"⚠️ Target issue ({ai_prediction_issue}) doesn't follow last consecutive history issue ({consecutive_history_issues_for_ai[-1]})")
            update_ai_worker_status("warning", "History alignment issue")
            return

        history_numbers_for_ai = [parsed_history[str(issue)] for issue in consecutive_history_issues_for_ai]
        logger.info(f"📊 Feeding AI {len(history_numbers_for_ai)} consecutive numbers")
        logger.info(f"📊 Last 10 history numbers: {history_numbers_for_ai[-10:]}")

        logger.info("🔍 Step 11: AI Prediction with retry logic...")

        # --- AI Prediction with retry logic ---
        ai_prediction = None
        backoff_time = INITIAL_BACKOFF_SECONDS
        used_api_keys = []
        
        for attempt in range(MAX_RETRIES):
            if shutdown_event.is_set():
                logger.info("🛑 Shutdown requested during AI prediction attempts")
                return
            
            logger.info(f"🔍 Step 11.{attempt + 1}: Starting attempt {attempt + 1}/{MAX_RETRIES}")
            
            # Select API key (avoid recently used ones if possible)
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
                    logger.info(f"✅ AI Prediction successful for issue {ai_prediction_issue}: {ai_prediction}")
                    break
                else:
                    logger.warning(f"⚠️ Attempt {attempt + 1} returned None prediction")
                    
            except Exception as api_e:
                logger.error(f"❌ Attempt {attempt + 1} failed: {api_e}", exc_info=True)

            # Exponential backoff before retry
            if attempt < MAX_RETRIES - 1:
                sleep_for = backoff_time + random.uniform(0, backoff_time * 0.2)
                logger.info(f"⏳ Sleeping {sleep_for:.2f}s before retry...")
                time.sleep(sleep_for)
                backoff_time = min(backoff_time * 2, MAX_BACKOFF_SECONDS)
        
        logger.info("🔍 Step 12: Handling prediction result...")
        
        # Handle prediction result
        if ai_prediction is None:
            logger.error(f"❌ All {MAX_RETRIES} attempts failed for AI prediction")
            update_ai_worker_status("prediction_failed", f"All {MAX_RETRIES} attempts failed")
            category = None
        else:
            category = "small" if ai_prediction <= 4 else "big"
            logger.info(f"🎯 Final AI prediction: {ai_prediction} (category: {category})")

        logger.info("🔍 Step 13: Saving prediction data...")

        # --- Save prediction data ---
        prediction_data = {
            "issue": ai_prediction_issue,
            "ai_prediction": ai_prediction,
            "ai_predicted_category": category,
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "environment": "railway" if is_railway_environment() else "local",
            "attempts_used": attempt + 1 if ai_prediction is not None else MAX_RETRIES,
            "history_length": len(history_numbers_for_ai)
        }
        
        try:
            with redis_lock:
                # Save current prediction
                redis_client.set(REDIS_AI_PREDICTION_KEY, json.dumps(prediction_data))
                
                # Save to history
                history_entry = {
                    "ai_prediction": ai_prediction,
                    "ai_predicted_category": category,
                    "timestamp": prediction_data["timestamp"]
                }
                redis_client.hset(REDIS_AI_PREDICTIONS_HISTORY_HASH, str(ai_prediction_issue), json.dumps(history_entry))
            
            logger.info(f"💾 AI prediction data saved successfully for issue {ai_prediction_issue}")
            
            logger.info("🔍 Step 14: Updating accuracy calculations...")
            
            # Update accuracy calculations
            calculate_ai_big_small_accuracy()
            
            update_ai_worker_status("prediction_complete", f"Prediction {ai_prediction} saved for issue {ai_prediction_issue}")
            
            logger.info(f"🚀 ===== AI PREDICTION PROCESSING COMPLETE =====")
            
        except Exception as save_error:
            logger.error(f"❌ Error saving AI prediction data: {save_error}", exc_info=True)
            update_ai_worker_status("save_error", f"Failed to save prediction: {str(save_error)}")

    except Exception as e:
        logger.error(f"💥 Unexpected error in AI prediction processing: {e}", exc_info=True)
        update_ai_worker_status("error", f"Processing error: {str(e)}")

def ai_listener_loop():
    """Enhanced main loop with graceful shutdown and comprehensive error handling"""
    logger.info("🚀 AI Worker started. Listening for triggers on Redis Pub/Sub channel...")
    update_ai_worker_status("starting", "AI Worker initializing")
    
    pubsub = None
    last_heartbeat = time.time()
    last_ai_status_check = time.time()
    reconnection_attempts = 0
    max_reconnection_attempts = 10
    
    try:
        # Initialize pub/sub connection
        pubsub = redis_client.pubsub()
        pubsub.subscribe(REDIS_AI_TRIGGER_CHANNEL)
        logger.info(f"📡 Subscribed to channel: {REDIS_AI_TRIGGER_CHANNEL}")
        update_ai_worker_status("listening", "Listening for prediction triggers")

        while not shutdown_event.is_set():
            try:
                # Get message with timeout
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                
                # Send periodic heartbeat
                current_time = time.time()
                if current_time - last_heartbeat >= HEARTBEAT_INTERVAL:
                    send_ai_worker_heartbeat()
                    last_heartbeat = current_time
                
                # Check AI enabled status periodically and log changes
                if current_time - last_ai_status_check >= 10:  # Check every 10 seconds
                    current_ai_status = is_ai_enabled()
                    logger.info(f"🤖 AI Status Check: {'ENABLED' if current_ai_status else 'DISABLED'}")
                    last_ai_status_check = current_time
                    
                    if not current_ai_status:
                        update_ai_worker_status("disabled", "AI predictions disabled")
                    else:
                        update_ai_worker_status("listening", "AI enabled - listening for triggers")

                # Process message if received
                if message and message['type'] == 'message':
                    try:
                        logger.info(f"📨 ===== AI TRIGGER RECEIVED =====")
                        logger.info(f"📨 Raw message: {message}")
                        logger.info(f"📨 Message data: {message['data']}")
                        logger.info(f"📨 Message type: {message['type']}")
                        
                        data = json.loads(message['data'])
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
                retry_delay = min(5 * reconnection_attempts, 60)  # Exponential backoff, max 60s
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
                time.sleep(5)  # Brief pause before continuing

    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
        update_ai_worker_status("interrupted", "Keyboard interrupt")
    except Exception as e:
        logger.critical(f"💥 Critical error in AI listener: {e}", exc_info=True)
        update_ai_worker_status("critical", f"Critical error: {str(e)}")
    finally:
        # Clean up
        if pubsub:
            try:
                pubsub.close()
                logger.info("📡 Pub/Sub connection closed")
            except Exception as e:
                logger.warning(f"⚠️ Error closing pub/sub: {e}")
        
        logger.info("🏁 AI Worker listener loop ended")
        update_ai_worker_status("stopped", "AI Worker stopped")

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
    logger.info(f"📁 Data directory: {DATA_DIR}")
    logger.info(f"🔗 Redis: {'Connected' if redis_client else 'Disconnected'}")
    logger.info(f"🔑 API Keys: {len(PERPLEXITY_API_KEYS)} configured")
    logger.info(f"🤖 AI Model: {PERPLEXITY_AI_MODEL}")
    logger.info(f"📡 Trigger Channel: {REDIS_AI_TRIGGER_CHANNEL}")
    
    # Test AI status on startup with detailed logging
    ai_status = is_ai_enabled()
    logger.info(f"🎯 AI Status: {'ENABLED' if ai_status else 'DISABLED'}")
    
    # Debug the AI flag value
    try:
        with redis_lock:
            ai_flag_raw = redis_client.get(REDIS_AI_FLAG_KEY)
        logger.info(f"🔍 AI Flag Debug - Key: '{REDIS_AI_FLAG_KEY}', Raw Value: '{ai_flag_raw}', Type: {type(ai_flag_raw)}")
    except Exception as e:
        logger.error(f"❌ Error debugging AI flag: {e}")
    
    try:
        # Start the main listener loop
        ai_listener_loop()
    except Exception as e:
        logger.critical(f"💥 Fatal error in main execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("👋 AI Worker terminated")
