# ai_worker.py
import time
import os
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import redis
import pytz
import random
import requests

# Load .env file first
load_dotenv()

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'ai_worker')

# --- Logging Setup ---
LOG_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
LOG_PATH = os.path.join(LOG_DIR, "ai_worker.log")
os.makedirs(LOG_DIR, exist_ok=True)

# Enhanced logging with Railway support
log_format = "%(asctime)s - AI_WORKER - %(levelname)s - %(message)s"
handlers = [logging.StreamHandler()]

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

# Log environment info
if is_railway_environment():
    logger.info(f"🚂 AI Worker running on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {LOG_DIR}")
else:
    logger.info("🏠 AI Worker running in local environment")

# --- Redis Connection with Enhanced Fallback ---
def get_redis_connection():
    """Get Redis connection with fallback logic"""
    # Use your working fallback logic
    redis_url = os.getenv("REDIS_URL_UPSTASH") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    logger.info(f"🔗 Using Redis: {redis_url[:25]}...")
    return redis_url

try:
    redis_url = get_redis_connection()
    redis_client = redis.from_url(
        redis_url,
        decode_responses=True,  # This removes need for .decode('utf-8')
        socket_connect_timeout=15,
        socket_timeout=15,
        retry_on_timeout=True
    )
    redis_client.ping()
    logger.info("✅ Successfully connected to Redis from AI worker.")
except redis.exceptions.ConnectionError as e:
    logger.critical(f"❌ Could not connect to Redis from AI worker: {e}. Exiting.")
    exit(1)

# --- Redis Keys ---
REDIS_HISTORY_KEY = "lottery:history"
REDIS_MAIN_PREDICTION_KEY = "latest_prediction_data"
REDIS_AI_FLAG_KEY = "lottery:ai_enabled"
REDIS_AI_PREDICTION_KEY = "lottery:ai_prediction"
REDIS_AI_PREDICTIONS_HISTORY_HASH = "lottery:ai_preds_history"
REDIS_AI_BIG_SMALL_ACCURACY_KEY = "lottery:ai_big_small_accuracy"

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
    exit(1)

logger.info(f"🔑 Loaded {len(PERPLEXITY_API_KEYS)} Perplexity AI API key(s)")

# --- Helper Function: Check if AI is enabled ---
def is_ai_enabled():
    """Enhanced AI enabled check with proper value validation"""
    try:
        ai_flag_value = redis_client.get(REDIS_AI_FLAG_KEY)
        
        if ai_flag_value is None:
            return False
        
        # Handle both string and integer values
        if isinstance(ai_flag_value, str):
            return ai_flag_value.lower() in ['1', 'true', 'yes', 'on', 'enabled']
        else:
            return bool(ai_flag_value)
    except Exception as e:
        logger.error(f"❌ Error checking AI enabled status: {e}")
        return False

# --- Enhanced AI Accuracy Calculation ---
def calculate_ai_big_small_accuracy():
    """Calculate accuracy for AI big/small predictions"""
    try:
        # Get AI prediction history
        ai_history = redis_client.hgetall(REDIS_AI_PREDICTIONS_HISTORY_HASH)
        actual_history_raw = redis_client.get(REDIS_HISTORY_KEY)
        
        if not ai_history or not actual_history_raw:
            # Create default structure for new systems
            default_accuracy = {
                "total_predictions": 0,
                "correct_predictions": 0,
                "overall_accuracy_percentage": 0.0,
                "big_predictions": {"total": 0, "correct": 0, "accuracy_percentage": 0.0},
                "small_predictions": {"total": 0, "correct": 0, "accuracy_percentage": 0.0},
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "status": "no_data",
                "message": "Collecting data for accuracy calculation..."
            }
            redis_client.set(REDIS_AI_BIG_SMALL_ACCURACY_KEY, json.dumps(default_accuracy))
            return
        
        actual_history = json.loads(actual_history_raw)
        
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
        
        # Determine confidence level
        if total_predictions < 3:
            status = "early_data"
            message = f"Early accuracy data based on {total_predictions} prediction(s)"
        elif total_predictions < 10:
            status = "limited_data"
            message = f"Limited accuracy data based on {total_predictions} predictions"
        else:
            status = "reliable_data"
            message = f"Reliable accuracy based on {total_predictions} predictions"
        
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
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "status": status,
            "message": message
        }
        
        redis_client.set(REDIS_AI_BIG_SMALL_ACCURACY_KEY, json.dumps(accuracy_data))
        logger.info(f"📊 AI accuracy updated: {correct_predictions}/{total_predictions} ({overall_accuracy:.2f}%)")
        
    except Exception as e:
        logger.error(f"❌ Error calculating AI accuracy: {e}")

# --- Perplexity AI Prediction Function (Your Working Version) ---
def get_perplexity_ai_prediction(history_numbers, api_key, current_issue_id, last_issue_info=None):
    """Gets a lottery prediction using the Perplexity AI API."""
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # --- Constructing the dynamic prompt text ---
    prompt_intro = f"""
Your task is to predict the next single digit (0–9) in a sequence based on the provided historical lottery results.
### Remember that it doesn't always depend on frequency of same number repeatations.
### Use critical thinking.
 Examine the provided historical sequence of single-digit numbers.
  - Analyze recent trends: find numbers that are "due" (haven't appeared in a while).

- **Conclusion**: Output *one single digit* (0–9) as your final prediction.

### Input:
"""
    
    current_3_digits_issue = str(current_issue_id)[-3:] if len(str(current_issue_id)) > 3 else str(current_issue_id)
    
    if last_issue_info:
        last_issue_id = str(last_issue_info['issue'])
        last_3_digits_issue = last_issue_id[-3:] if len(last_issue_id) > 3 else last_issue_id
        
        last_prediction = last_issue_info.get('prediction')
        last_outcome = last_issue_info.get('outcome')

        feedback = ""
        if last_prediction is not None and last_outcome is not None:
            feedback = f"You predicted for issue no {last_3_digits_issue} was {last_prediction}, but outcome came {last_outcome}, predict for next round."
        
        prompt_text = (
            prompt_intro +
            f"The current issue number's last 3 digits is: {current_3_digits_issue}.\n" +
            feedback +
            f"\nHistorical Sequence: {', '.join(map(str, history_numbers))}\n" +
            "\n🔮 Based on this sequence, what is the next single-digit number?\n" +
            "\n👉 **Only output the predicted digit (0–9)** with no explanation or additional text."
        )
    else:
        prompt_text = (
            prompt_intro +
            f"The current issue number's last 3 digits is: {current_3_digits_issue}.\n" +
            f"\nHistorical Sequence: {', '.join(map(str, history_numbers))}\n" +
            "\n🔮 Based on this sequence, what is the most probable and interesting next single-digit winning number?\n" +
            "\n👉 **Only output the predicted digit (0–9)** with no explanation or additional text."
        )

    # --- LOG THE FINAL PROMPT HERE ---
    logger.info(f"Generated prompt for Perplexity AI:\n---\n{prompt_text}\n---")

    data = {
        "model": PERPLEXITY_AI_MODEL,
        "messages": [
            {"role": "system", "content": "You are an expert lottery prediction AI."},
            {"role": "user", "content": prompt_text}
        ],
        "temperature": 0.7,
        "max_tokens": 5
    }

    try:
        response = requests.post("https://api.perplexity.ai/chat/completions", headers=headers, json=data, timeout=30)
        response.raise_for_status()
        
        response_json = response.json()
        logger.debug(f"Perplexity AI raw response: {json.dumps(response_json, indent=2)}")

        if 'choices' in response_json and len(response_json['choices']) > 0:
            message_content = response_json['choices'][0].get('message', {}).get('content', '').strip()
            
            try:
                predicted_number = int(message_content)
                if 0 <= predicted_number <= 9:
                    return predicted_number
                else:
                    logger.warning(f"Perplexity AI returned out-of-range number: {predicted_number}. Expected 0-9. Content: '{message_content}'")
                    return None
            except ValueError:
                logger.error(f"Perplexity AI returned non-integer prediction: '{message_content}'. Expected a single digit.")
                return None
        else:
            logger.warning(f"Perplexity AI response missing 'choices' or content: {response_json}")
            return None

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Perplexity AI HTTP error: {http_err}. Status Code: {response.status_code}. Response: {response.text}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Perplexity AI Connection error: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Perplexity AI Timeout error: {timeout_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Perplexity AI Request error: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"Perplexity AI JSON decode error: {json_err}. Raw response: {response.text}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while getting Perplexity AI prediction: {e}", exc_info=True)
        return None

# --- Configuration for retries ---
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 60

# --- History Constraints ---
MAX_HISTORY_TO_USE = 55
MIN_HISTORY_REQUIRED = 5 

def process_ai_prediction(trigger_issue):
    """Encapsulates the core AI prediction logic (Your Working Version)"""
    logger.info(f"🚀 AI prediction triggered for issue: {trigger_issue}")

    if not is_ai_enabled():
        logger.info("⚠️ AI is disabled. Skipping prediction for triggered issue.")
        return

    logger.info("✅ AI is ENABLED. Running prediction cycle...")

    main_prediction_str = redis_client.get(REDIS_MAIN_PREDICTION_KEY)
    if not main_prediction_str:
        logger.warning(f"No main prediction data found in Redis key '{REDIS_MAIN_PREDICTION_KEY}'. Skipping AI prediction this cycle.")
        return

    try:
        # No need for .decode('utf-8') since decode_responses=True
        main_prediction_data = json.loads(main_prediction_str)
        actual_target_next_issue = main_prediction_data.get("next_issue")
        
        if not actual_target_next_issue:
            logger.error(f"Main prediction data missing 'next_issue' field. Data: {main_prediction_data}. Skipping AI prediction.")
            return

        if int(actual_target_next_issue) > int(trigger_issue):
            logger.warning(f"Triggered for issue {trigger_issue} but main prediction already targets {actual_target_next_issue}. Predicting for {actual_target_next_issue}.")
            ai_prediction_issue = actual_target_next_issue
        elif int(actual_target_next_issue) < int(trigger_issue):
            logger.error(f"Main prediction data ({actual_target_next_issue}) is *behind* the triggered issue ({trigger_issue}). Skipping prediction to prevent invalid state.")
            return
        else:
            ai_prediction_issue = actual_target_next_issue

        logger.info(f"🎯 Target issue for AI prediction: {ai_prediction_issue}")

    except json.JSONDecodeError as e:
        logger.error(f"Error parsing main prediction JSON: {e}. Skipping AI prediction this cycle.")
        return

    raw_history_str = redis_client.get(REDIS_HISTORY_KEY)
    
    if not raw_history_str:
        logger.warning(f"No history found in Redis key '{REDIS_HISTORY_KEY}'. Skipping prediction this cycle.")
        return

    try:
        parsed_history = json.loads(raw_history_str)  # No .decode needed
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing history JSON: {e}. Skipping prediction this cycle.")
        return

    # --- GATHERING DATA FOR THE NEW PROMPT ---
    last_issue_info = None
    last_prediction_issue = int(ai_prediction_issue) - 1
    
    last_prediction_data = redis_client.hget(REDIS_AI_PREDICTIONS_HISTORY_HASH, str(last_prediction_issue))
    if last_prediction_data:
        try:
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
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse AI history for issue {last_prediction_issue}. Error: {e}")
            last_issue_info = None

    # --- START OF GAP LOGIC (Your Working Version) ---
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
            logger.info(f"Gap found in history before issue {current_expected_issue + 1} (missing {current_expected_issue}). Stopping consecutive history collection.")
            break
    
    if len(consecutive_history_issues_for_ai) < MIN_HISTORY_REQUIRED:
        logger.warning(f"Insufficient recent consecutive history for AI prediction for issue {ai_prediction_issue}. "
                       f"Required: {MIN_HISTORY_REQUIRED}, Found: {len(consecutive_history_issues_for_ai)}.")
        return

    if consecutive_history_issues_for_ai and int(ai_prediction_issue) != consecutive_history_issues_for_ai[-1] + 1:
        logger.warning(f"Target prediction issue ({ai_prediction_issue}) does not immediately follow "
                       f"the last available consecutive history issue ({consecutive_history_issues_for_ai[-1]}). "
                       f"Skipping prediction to prevent using misaligned history.")
        return

    history_numbers_for_ai = [parsed_history[str(issue)] for issue in consecutive_history_issues_for_ai]
    logger.info(f"📊 Numbers being fed to AI (last {len(history_numbers_for_ai)} consecutive entries): {history_numbers_for_ai[-10:]}")
    # --- END OF GAP LOGIC ---

    ai_prediction = None
    backoff_time = INITIAL_BACKOFF_SECONDS
    
    for attempt in range(MAX_RETRIES):
        current_api_key = random.choice(PERPLEXITY_API_KEYS)
        logger.info(f"🔑 Attempt {attempt + 1}/{MAX_RETRIES} to get AI prediction for issue {ai_prediction_issue} using key ending in {current_api_key[-4:]}...")
        
        try:
            ai_prediction = get_perplexity_ai_prediction(history_numbers_for_ai, api_key=current_api_key, current_issue_id=ai_prediction_issue, last_issue_info=last_issue_info)
            if ai_prediction is not None:
                logger.info(f"🧠 AI Prediction for issue {ai_prediction_issue}: {ai_prediction}")
                break
            else:
                logger.warning(f"Attempt {attempt + 1} returned None prediction. Retrying...")
        except Exception as api_e:
            logger.error(f"Attempt {attempt + 1} failed during prediction function call: {api_e}")

        if attempt < MAX_RETRIES - 1:
            sleep_for = backoff_time + random.uniform(0, backoff_time * 0.2)
            logger.info(f"Sleeping for {sleep_for:.2f} seconds before next retry...")
            time.sleep(sleep_for)
            backoff_time = min(backoff_time * 2, MAX_BACKOFF_SECONDS)
        else:
            logger.error(f"All {MAX_RETRIES} attempts failed for AI prediction. Setting prediction to None.")
    
    if ai_prediction is None:
        logger.warning(f"AI prediction for issue {ai_prediction_issue} is None after all retries.")
        category = None
    else:
        category = "small" if ai_prediction <= 4 else "big"

    prediction_data = {
        "issue": ai_prediction_issue,
        "ai_prediction": ai_prediction,
        "ai_predicted_category": category,
        "timestamp": datetime.now(pytz.utc).isoformat(),
        "environment": "railway" if is_railway_environment() else "local"
    }
    redis_client.set(REDIS_AI_PREDICTION_KEY, json.dumps(prediction_data))
    logger.info(f"💾 Successfully saved AI prediction for issue {ai_prediction_issue} to Redis key '{REDIS_AI_PREDICTION_KEY}'.")

    redis_client.hset(REDIS_AI_PREDICTIONS_HISTORY_HASH, str(ai_prediction_issue), json.dumps({
        "ai_prediction": prediction_data["ai_prediction"],
        "ai_predicted_category": prediction_data["ai_predicted_category"],
        "timestamp": prediction_data["timestamp"]
    }))
    logger.info(f"💾 Saved AI prediction history for issue {ai_prediction_issue} to {REDIS_AI_PREDICTIONS_HISTORY_HASH}")

    # Calculate accuracy after each prediction
    calculate_ai_big_small_accuracy()

def ai_listener_loop():
    """Main loop for the AI worker (Your Working Version)"""
    logger.info("🚀 AI Worker started. Listening for triggers on Redis Pub/Sub channel...")
    logger.info(f"📡 Channel: {REDIS_AI_TRIGGER_CHANNEL}")
    
    pubsub = redis_client.pubsub()
    pubsub.subscribe(REDIS_AI_TRIGGER_CHANNEL)
    logger.info(f"📡 Successfully subscribed to {REDIS_AI_TRIGGER_CHANNEL}")

    while True:
        try:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            
            # Simple AI status check (no complex locking)
            if not is_ai_enabled():
                time.sleep(5)  # Sleep when disabled
                continue

            if message and message['type'] == 'message':
                try:
                    logger.info(f"📨 ===== AI TRIGGER RECEIVED =====")
                    logger.info(f"📨 Raw message: {message}")
                    
                    # No .decode needed since decode_responses=True
                    data = json.loads(message['data'])
                    trigger_issue = data.get('issue')
                    trigger_type = data.get('trigger_type', 'unknown')
                    
                    logger.info(f"📨 Parsed trigger - Type: {trigger_type}, Issue: {trigger_issue}")
                    
                    if trigger_issue:
                        logger.info(f"📨 Processing AI trigger for issue: {trigger_issue}")
                        process_ai_prediction(trigger_issue)
                    else:
                        logger.warning(f"⚠️ Malformed trigger message (missing 'issue'): {data}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to decode message from Pub/Sub: {message['data']}. Error: {e}")
                except Exception as e:
                    logger.error(f"❌ Error processing Pub/Sub message: {e}", exc_info=True)

        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Redis Pub/Sub connection lost: {e}. Attempting to reconnect in 5 seconds...")
            time.sleep(5)
            try:
                redis_client.ping()
                pubsub = redis_client.pubsub()
                pubsub.subscribe(REDIS_AI_TRIGGER_CHANNEL)
                logger.info("✅ Successfully reconnected to Redis Pub/Sub.")
            except Exception as reconnect_e:
                logger.critical(f"💥 Failed to reconnect to Redis Pub/Sub: {reconnect_e}. Exiting.")
                exit(1)
        except Exception as e:
            logger.error(f"💥 Unexpected error in AI listener loop: {e}", exc_info=True)
            time.sleep(5)

# --- Startup Information ---
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
    
    # Initialize accuracy calculation on startup
    try:
        calculate_ai_big_small_accuracy()
    except Exception as e:
        logger.warning(f"⚠️ Could not initialize accuracy on startup: {e}")
    
    # Start the main listener loop
    ai_listener_loop()
