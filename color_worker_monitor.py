# color_worker_monitor.py

import os
import json
import logging
import redis
import schedule
import time
import signal
import sys
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from threading import Event, Lock
import pytz

# --- Load environment variables
load_dotenv()

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'color_worker_monitor')

# --- Enhanced Logging Setup with Railway Support ---
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.join(DATA_DIR, "color_worker_monitor.log")

# Configure logging with Railway-specific formatting
log_format = '%(asctime)s - COLOR_WORKER_MONITOR - %(levelname)s - %(message)s'
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
logger = logging.getLogger("color_worker_monitor")

# Log Railway environment info
if is_railway_environment():
    logger.info(f"🚂 Color Worker Monitor running on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
else:
    logger.info("🏠 Color Worker Monitor running in local environment")

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
REDIS_COLOR_PREDICTION_KEY = "lottery:color_prediction"
REDIS_COLOR_PREDICTION_LOG_KEY = "lottery:color_prediction_log"
REDIS_COLOR_PREDICTION_HISTORY_KEY = "lottery:color_prediction_history"
REDIS_COLOR_ACCURACY_KEY = "lottery:color_accuracy"
REDIS_COLOR_RULE_HISTORY_KEY = "lottery:color_rule_history"
REDIS_COLOR_STREAKS_KEY = "lottery:color_streaks"
REDIS_CONTROLLED_STREAKS_KEY = "lottery:controlled_streaks"
REDIS_RESET_POINT = "lottery:streak_reset_point"

# Size prediction keys
REDIS_SIZE_PREDICTION_KEY = "lottery:size_prediction"
REDIS_SIZE_PREDICTION_LOG_KEY = "lottery:size_prediction_log"
REDIS_SIZE_PREDICTION_HISTORY_KEY = "lottery:size_prediction_history"
REDIS_SIZE_ACCURACY_KEY = "lottery:size_accuracy"
REDIS_SIZE_STREAKS_KEY = "lottery:size_streaks"
REDIS_CONTROLLED_SIZE_STREAKS_KEY = "lottery:controlled_size_streaks"

# Status tracking keys
REDIS_COLOR_WORKER_STATUS_KEY = "lottery:color_worker_status"
REDIS_COLOR_WORKER_HEARTBEAT_KEY = "lottery:color_worker_heartbeat"

# --- Enhanced Configuration ---
COLOR_MAP = {
    0: 'R', 1: 'G', 2: 'R', 3: 'G', 4: 'R',
    5: 'G', 6: 'R', 7: 'G', 8: 'R', 9: 'G'
}
SIZE_MAP = {
    0: 'S', 1: 'S', 2: 'S', 3: 'S', 4: 'S',
    5: 'B', 6: 'B', 7: 'B', 8: 'B', 9: 'B'
}
MAX_PATTERN_LENGTH = 6
MIN_OCCURRENCES = 11
MAX_ALLOWED_LOSS_STREAK = 3
TRAINING_WINDOW_SIZE = 1000
EMERGENCY_LOSS_STREAK = 5

# Global variables with thread safety
rules = {}
size_rules = {}
current_loss = 0
current_size_loss = 0
rules_lock = Lock()

# --- Status Management Functions ---
def update_color_worker_status(status, message=None):
    """Update color worker status in Redis"""
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
                
                redis_client.set(REDIS_COLOR_WORKER_STATUS_KEY, json.dumps(status_data), ex=300)  # 5 min expiry
                logger.debug(f"📊 Color Worker status updated: {status}")
    except Exception as e:
        logger.error(f"Failed to update color worker status: {e}")

def send_color_worker_heartbeat():
    """Send heartbeat to Redis"""
    try:
        if redis_client:
            with redis_lock:
                heartbeat_data = {
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "service_name": get_railway_service_name(),
                    "current_color_loss": current_loss,
                    "current_size_loss": current_size_loss,
                    "color_rules_count": len(rules),
                    "size_rules_count": len(size_rules)
                }
                redis_client.set(REDIS_COLOR_WORKER_HEARTBEAT_KEY, json.dumps(heartbeat_data), ex=120)  # 2 min expiry
                logger.debug("💗 Color Worker heartbeat sent")
    except Exception as e:
        logger.error(f"Failed to send color worker heartbeat: {e}")

# --- Enhanced Data Access Functions ---
def decode_history():
    """Enhanced history decoding with error handling"""
    try:
        if not redis_client:
            logger.error("❌ Redis client not available for history decode")
            return {}
        
        with redis_lock:
            raw = redis_client.get(REDIS_HISTORY_KEY)
        
        if not raw:
            logger.debug("📭 No history data found in Redis")
            return {}
        
        # Handle both bytes and string data
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8')
        
        history = json.loads(raw)
        logger.debug(f"📥 Decoded {len(history)} history entries")
        return history
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error for history: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Error decoding history: {e}")
        return {}

def get_color_sequence(history):
    """Enhanced color sequence extraction with validation"""
    try:
        if not history:
            logger.debug("📭 No history provided for color sequence")
            return []
        
        sequence = []
        for k in sorted(history.keys(), key=int):
            value = history[k]
            if value is not None and 0 <= value <= 9:
                sequence.append((k, COLOR_MAP[value]))
            else:
                logger.warning(f"⚠️ Invalid value {value} for issue {k}")
        
        logger.debug(f"🎨 Generated color sequence with {len(sequence)} entries")
        return sequence
        
    except Exception as e:
        logger.error(f"❌ Error generating color sequence: {e}")
        return []

def get_size_sequence(history):
    """Enhanced size sequence extraction with validation"""
    try:
        if not history:
            logger.debug("📭 No history provided for size sequence")
            return []
        
        sequence = []
        for k in sorted(history.keys(), key=int):
            value = history[k]
            if value is not None and 0 <= value <= 9:
                sequence.append((k, SIZE_MAP[value]))
            else:
                logger.warning(f"⚠️ Invalid value {value} for issue {k}")
        
        logger.debug(f"📏 Generated size sequence with {len(sequence)} entries")
        return sequence
        
    except Exception as e:
        logger.error(f"❌ Error generating size sequence: {e}")
        return []

# --- Enhanced Pattern Learning Functions ---
def learn_patterns(colors):
    """Enhanced pattern learning with validation"""
    try:
        if not colors or len(colors) < MAX_PATTERN_LENGTH:
            logger.warning(f"⚠️ Insufficient data for pattern learning: {len(colors) if colors else 0} entries")
            return defaultdict(lambda: defaultdict(int))
        
        stats = defaultdict(lambda: defaultdict(int))
        patterns_found = 0
        
        for i in range(MAX_PATTERN_LENGTH, len(colors)):
            for l in range(2, MAX_PATTERN_LENGTH + 1):
                try:
                    pattern = ''.join([c for _, c in colors[i - l:i]])
                    next_c = colors[i][1]
                    
                    if pattern and next_c:
                        stats[pattern][next_c] += 1
                        patterns_found += 1
                        
                except (IndexError, TypeError) as e:
                    logger.warning(f"⚠️ Error processing pattern at index {i}, length {l}: {e}")
                    continue
        
        logger.debug(f"🧠 Learned {patterns_found} pattern occurrences")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error in pattern learning: {e}")
        return defaultdict(lambda: defaultdict(int))

def generate_rules(stats):
    """Enhanced rule generation with validation"""
    try:
        if not stats:
            logger.warning("⚠️ No statistics provided for rule generation")
            return {}
        
        ruleset = {}
        rules_generated = 0
        
        for k, outcomes in stats.items():
            try:
                total = sum(outcomes.values())
                if total >= MIN_OCCURRENCES:
                    best = max(outcomes.items(), key=lambda x: x[1])
                    accuracy = round((best[1] / total) * 100, 2)
                    
                    ruleset[k] = {
                        "predict": best[0],
                        "correct": best[1],
                        "total": total,
                        "accuracy": accuracy
                    }
                    rules_generated += 1
                    
            except (ValueError, ZeroDivisionError) as e:
                logger.warning(f"⚠️ Error generating rule for pattern {k}: {e}")
                continue
        
        logger.debug(f"📜 Generated {rules_generated} rules from {len(stats)} patterns")
        return ruleset
        
    except Exception as e:
        logger.error(f"❌ Error generating rules: {e}")
        return {}

def retrain_rules(colors):
    """Enhanced rule retraining with comprehensive error handling"""
    try:
        logger.info(f"🧠 Retraining rules with {len(colors)} color entries...")
        update_color_worker_status("retraining", f"Retraining with {len(colors)} entries")
        
        if not colors:
            logger.error("❌ No colors provided for retraining")
            return {}
        
        stats = learn_patterns(colors)
        rules = generate_rules(stats)
        
        logger.info(f"✅ Retraining complete: {len(rules)} rules generated")
        return rules
        
    except Exception as e:
        logger.error(f"❌ Error during rule retraining: {e}")
        return {}

# --- Enhanced Prediction Functions ---
def predict_next_color(seq, rulebook):
    """Enhanced color prediction with validation"""
    try:
        if not seq:
            logger.warning("⚠️ Empty sequence for color prediction")
            return 'R', "Fallback", 0.0, True
        
        if not rulebook:
            logger.warning("⚠️ Empty rulebook for color prediction")
            return 'R', "Fallback", 0.0, True
        
        # Try patterns from longest to shortest
        for l in range(min(MAX_PATTERN_LENGTH, len(seq)), 1, -1):
            try:
                sub = ''.join(seq[-l:])
                if sub in rulebook:
                    rule = rulebook[sub]
                    prediction = rule["predict"]
                    accuracy = rule["accuracy"]
                    logger.debug(f"🎯 Color prediction using rule '{sub}': {prediction} ({accuracy}%)")
                    return prediction, sub, accuracy, False
            except (IndexError, KeyError) as e:
                logger.warning(f"⚠️ Error checking pattern length {l}: {e}")
                continue
        
        logger.debug("🎯 Using fallback color prediction: Red")
        return 'R', "Fallback", 0.0, True
        
    except Exception as e:
        logger.error(f"❌ Error in color prediction: {e}")
        return 'R', "Fallback", 0.0, True

def predict_next_size(seq, rulebook):
    """Enhanced size prediction with validation"""
    try:
        if not seq:
            logger.warning("⚠️ Empty sequence for size prediction")
            return 'S', "Fallback", 0.0, True
        
        if not rulebook:
            logger.warning("⚠️ Empty rulebook for size prediction")
            return 'S', "Fallback", 0.0, True
        
        # Try patterns from longest to shortest
        for l in range(min(MAX_PATTERN_LENGTH, len(seq)), 1, -1):
            try:
                sub = ''.join(seq[-l:])
                if sub in rulebook:
                    rule = rulebook[sub]
                    prediction = rule["predict"]
                    accuracy = rule["accuracy"]
                    logger.debug(f"📏 Size prediction using rule '{sub}': {prediction} ({accuracy}%)")
                    return prediction, sub, accuracy, False
            except (IndexError, KeyError) as e:
                logger.warning(f"⚠️ Error checking pattern length {l}: {e}")
                continue
        
        logger.debug("📏 Using fallback size prediction: Small")
        return 'S', "Fallback", 0.0, True
        
    except Exception as e:
        logger.error(f"❌ Error in size prediction: {e}")
        return 'S', "Fallback", 0.0, True

# --- Enhanced Logging Functions ---
def log_color_prediction(pred):
    """Enhanced color prediction logging with error handling"""
    try:
        if not pred or not pred.get("issue"):
            logger.error("❌ Invalid prediction data for color logging")
            return False
        
        if not redis_client:
            logger.error("❌ Redis client not available for color prediction logging")
            return False
        
        with redis_lock:
            # Store current prediction
            redis_client.set(REDIS_COLOR_PREDICTION_KEY, json.dumps(pred))
            # Store in prediction log
            redis_client.hset(REDIS_COLOR_PREDICTION_LOG_KEY, pred["issue"], json.dumps(pred))
        
        logger.debug(f"💾 Color prediction logged for issue {pred['issue']}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error logging color prediction: {e}")
        return False

def log_size_prediction(pred):
    """Enhanced size prediction logging with error handling"""
    try:
        if not pred or not pred.get("issue"):
            logger.error("❌ Invalid prediction data for size logging")
            return False
        
        if not redis_client:
            logger.error("❌ Redis client not available for size prediction logging")
            return False
        
        with redis_lock:
            # Store current prediction
            redis_client.set(REDIS_SIZE_PREDICTION_KEY, json.dumps(pred))
            # Store in prediction log
            redis_client.hset(REDIS_SIZE_PREDICTION_LOG_KEY, pred["issue"], json.dumps(pred))
        
        logger.debug(f"💾 Size prediction logged for issue {pred['issue']}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error logging size prediction: {e}")
        return False

# --- Enhanced History Update Functions ---
def update_prediction_history():
    """Enhanced prediction history update with error handling"""
    try:
        if not redis_client:
            logger.error("❌ Redis client not available for history update")
            return
        
        with redis_lock:
            log = redis_client.hgetall(REDIS_COLOR_PREDICTION_LOG_KEY)
        
        history = decode_history()
        if not log:
            logger.debug("📭 No prediction log found for history update")
            return
        
        # Process issues in descending order
        issues = sorted([k.decode() if isinstance(k, bytes) else k for k in log.keys()], 
                       key=lambda x: int(x), reverse=True)
        
        history_table = []
        processed_count = 0
        
        for i in issues[:15]:  # Limit to last 15
            try:
                pred_raw = log.get(i.encode() if isinstance(i, str) else i)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw.decode() if isinstance(pred_raw, bytes) else pred_raw)
                actual_num = history.get(i)
                
                actual_color = None
                if actual_num is not None:
                    actual_color = "Red" if COLOR_MAP.get(actual_num) == "R" else "Green"
                
                history_table.append({
                    "issue": i,
                    "predicted_color": pred.get("next_color"),
                    "actual_color": actual_color,
                    "prediction_time": pred.get("last_updated"),
                    "rule_name": pred.get("rule_name")
                })
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing history for issue {i}: {e}")
                continue
        
        with redis_lock:
            redis_client.set(REDIS_COLOR_PREDICTION_HISTORY_KEY, json.dumps(history_table))
        
        logger.debug(f"📋 Updated color prediction history with {processed_count} entries")
        
    except Exception as e:
        logger.error(f"❌ Error updating prediction history: {e}")

def update_size_prediction_history():
    """Enhanced size prediction history update with error handling"""
    try:
        if not redis_client:
            logger.error("❌ Redis client not available for size history update")
            return
        
        with redis_lock:
            log = redis_client.hgetall(REDIS_SIZE_PREDICTION_LOG_KEY)
        
        history = decode_history()
        if not log:
            logger.debug("📭 No size prediction log found for history update")
            return
        
        # Process issues in descending order
        issues = sorted([k.decode() if isinstance(k, bytes) else k for k in log.keys()], 
                       key=lambda x: int(x), reverse=True)
        
        history_table = []
        processed_count = 0
        
        for i in issues[:15]:  # Limit to last 15
            try:
                pred_raw = log.get(i.encode() if isinstance(i, str) else i)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw.decode() if isinstance(pred_raw, bytes) else pred_raw)
                actual_num = history.get(i)
                
                actual_size = None
                if actual_num is not None:
                    actual_size = "Big" if SIZE_MAP.get(actual_num) == "B" else "Small"
                
                history_table.append({
                    "issue": i,
                    "predicted_size": pred.get("next_size"),
                    "actual_size": actual_size,
                    "prediction_time": pred.get("last_updated"),
                    "rule_name": pred.get("rule_name")
                })
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing size history for issue {i}: {e}")
                continue
        
        with redis_lock:
            redis_client.set(REDIS_SIZE_PREDICTION_HISTORY_KEY, json.dumps(history_table))
        
        logger.debug(f"📋 Updated size prediction history with {processed_count} entries")
        
    except Exception as e:
        logger.error(f"❌ Error updating size prediction history: {e}")

# --- Enhanced Accuracy Calculation Functions ---
def update_accuracy():
    """Enhanced accuracy calculation with comprehensive error handling"""
    try:
        logger.debug("📊 Updating color accuracy statistics...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for accuracy update")
            return
        
        with redis_lock:
            log = redis_client.hgetall(REDIS_COLOR_PREDICTION_LOG_KEY)
            reset_point = redis_client.get(REDIS_RESET_POINT)
        
        history = decode_history()
        reset_from = int(reset_point.decode()) if reset_point else None
        
        total = correct = 0
        per_rule = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # Process issues in chronological order
        sorted_issues = sorted([k.decode() if isinstance(k, bytes) else k for k in log.keys()], key=int)
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                v = log.get(issue.encode() if isinstance(issue, str) else issue)
                if not v:
                    continue
                
                pred = json.loads(v.decode() if isinstance(v, bytes) else v)
                actual_value = history.get(issue)
                
                if actual_value is None:
                    continue
                
                actual_color = "Red" if COLOR_MAP[actual_value] == "R" else "Green"
                predicted_color = pred.get("next_color")
                rule = pred.get("rule_name", "Unknown")
                
                per_rule[rule]["total"] += 1
                total += 1
                
                if predicted_color == actual_color:
                    per_rule[rule]["correct"] += 1
                    correct += 1
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing accuracy for issue {issue}: {e}")
                continue
        
        # Build accuracy data
        accuracy_data = {
            "total_predictions": total,
            "correct_predictions": correct,
            "accuracy_percentage": round((correct / total) * 100, 2) if total else 0.0,
            "per_rule": {
                rule: {
                    "accuracy_pct": round((v["correct"] / v["total"]) * 100, 2) if v["total"] else 0.0,
                    "total": v["total"],
                    "correct": v["correct"]
                } for rule, v in per_rule.items()
            },
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "reset_from_issue": reset_from
        }
        
        with redis_lock:
            redis_client.set(REDIS_COLOR_ACCURACY_KEY, json.dumps(accuracy_data))
        
        logger.info(f"📊 Color accuracy updated: {correct}/{total} ({accuracy_data['accuracy_percentage']}%)")
        
    except Exception as e:
        logger.error(f"❌ Error updating color accuracy: {e}")

def update_size_accuracy():
    """Enhanced size accuracy calculation with comprehensive error handling"""
    try:
        logger.debug("📊 Updating size accuracy statistics...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for size accuracy update")
            return
        
        with redis_lock:
            log = redis_client.hgetall(REDIS_SIZE_PREDICTION_LOG_KEY)
            reset_point = redis_client.get(REDIS_RESET_POINT)
        
        history = decode_history()
        reset_from = int(reset_point.decode()) if reset_point else None
        
        total = correct = 0
        per_rule = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # Process issues in chronological order
        sorted_issues = sorted([k.decode() if isinstance(k, bytes) else k for k in log.keys()], key=int)
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                v = log.get(issue.encode() if isinstance(issue, str) else issue)
                if not v:
                    continue
                
                pred = json.loads(v.decode() if isinstance(v, bytes) else v)
                actual_value = history.get(issue)
                
                if actual_value is None:
                    continue
                
                actual_size = "Big" if SIZE_MAP[actual_value] == "B" else "Small"
                predicted_size = pred.get("next_size")
                rule = pred.get("rule_name", "Unknown")
                
                per_rule[rule]["total"] += 1
                total += 1
                
                if predicted_size == actual_size:
                    per_rule[rule]["correct"] += 1
                    correct += 1
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing size accuracy for issue {issue}: {e}")
                continue
        
        # Build accuracy data
        accuracy_data = {
            "total_predictions": total,
            "correct_predictions": correct,
            "accuracy_percentage": round((correct / total) * 100, 2) if total else 0.0,
            "per_rule": {
                rule: {
                    "accuracy_pct": round((v["correct"] / v["total"]) * 100, 2) if v["total"] else 0.0,
                    "total": v["total"],
                    "correct": v["correct"]
                } for rule, v in per_rule.items()
            },
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "reset_from_issue": reset_from
        }
        
        with redis_lock:
            redis_client.set(REDIS_SIZE_ACCURACY_KEY, json.dumps(accuracy_data))
        
        logger.info(f"📊 Size accuracy updated: {correct}/{total} ({accuracy_data['accuracy_percentage']}%)")
        
    except Exception as e:
        logger.error(f"❌ Error updating size accuracy: {e}")

# --- Enhanced Streak Calculation Functions ---
def calculate_color_streaks():
    """Enhanced color streak calculation with comprehensive error handling"""
    try:
        logger.debug("📈 Calculating color streaks...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for color streak calculation")
            return
        
        with redis_lock:
            pred_log = redis_client.hgetall(REDIS_COLOR_PREDICTION_LOG_KEY)
            history_raw = redis_client.get(REDIS_HISTORY_KEY)
            reset_point = redis_client.get(REDIS_RESET_POINT)
        
        if not pred_log or not history_raw:
            logger.warning("⚠️ Missing prediction log or history for color streaks")
            return
        
        reset_from = int(reset_point.decode()) if reset_point else None
        history = json.loads(history_raw.decode() if isinstance(history_raw, bytes) else history_raw)
        
        # Process issues in chronological order
        sorted_issues = sorted([k.decode() if isinstance(k, bytes) else k for k in pred_log.keys()], key=int)
        
        win = lose = max_win = max_lose = 0
        wd = defaultdict(int)  # win distribution
        ld = defaultdict(int)  # loss distribution
        
        for issue in sorted_issues:
            try:
                if reset_from and int(issue) < reset_from:
                    continue
                
                raw = pred_log.get(issue.encode() if isinstance(issue, str) else issue)
                if not raw:
                    continue
                
                pred = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                actual_val = history.get(issue)
                
                if actual_val is None:
                    continue
                
                actual_color = "Red" if COLOR_MAP[actual_val] == 'R' else "Green"
                predicted = pred.get("next_color")
                
                if actual_color == predicted:
                    win += 1
                    if lose > 0:
                        ld[lose] += 1
                        lose = 0
                    max_win = max(max_win, win)
                else:
                    lose += 1
                    if win > 0:
                        wd[win] += 1
                        win = 0
                    max_lose = max(max_lose, lose)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing color streak for issue {issue}: {e}")
                continue
        
        # Handle final streak
        current_win = win if win > 0 else 0
        current_lose = lose if lose > 0 else 0
        
        if win > 0:
            wd[win] += 1
        elif lose > 0:
            ld[lose] += 1
        
        result = {
            "current_win_streak": current_win,
            "current_lose_streak": current_lose,
            "max_win_streak": max_win,
            "max_lose_streak": max_lose,
            "win_streak_distribution": dict(wd),
            "lose_streak_distribution": dict(ld),
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "reset_from_issue": reset_from
        }
        
        with redis_lock:
            redis_client.set(REDIS_COLOR_STREAKS_KEY, json.dumps(result))
            redis_client.set(REDIS_CONTROLLED_STREAKS_KEY, json.dumps(result))
        
        logger.info(f"📈 Color streaks updated from reset point {reset_from}: win={current_win}, lose={current_lose}")
        
    except Exception as e:
        logger.error(f"❌ Error calculating color streaks: {e}")

def calculate_size_streaks():
    """Enhanced size streak calculation with comprehensive error handling"""
    try:
        logger.debug("📈 Calculating size streaks...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for size streak calculation")
            return
        
        with redis_lock:
            pred_log = redis_client.hgetall(REDIS_SIZE_PREDICTION_LOG_KEY)
            history_raw = redis_client.get(REDIS_HISTORY_KEY)
            reset_point = redis_client.get(REDIS_RESET_POINT)
        
        if not pred_log or not history_raw:
            logger.warning("⚠️ Missing size prediction log or history for size streaks")
            return
        
        reset_from = int(reset_point.decode()) if reset_point else None
        history = json.loads(history_raw.decode() if isinstance(history_raw, bytes) else history_raw)
        
        # Process issues in chronological order
        sorted_issues = sorted([k.decode() if isinstance(k, bytes) else k for k in pred_log.keys()], key=int)
        
        win = lose = max_win = max_lose = 0
        wd = defaultdict(int)  # win distribution
        ld = defaultdict(int)  # loss distribution
        
        for issue in sorted_issues:
            try:
                if reset_from and int(issue) < reset_from:
                    continue
                
                raw = pred_log.get(issue.encode() if isinstance(issue, str) else issue)
                if not raw:
                    continue
                
                pred = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                actual_val = history.get(issue)
                
                if actual_val is None:
                    continue
                
                actual_size = "Big" if SIZE_MAP[actual_val] == 'B' else "Small"
                predicted = pred.get("next_size")
                
                if actual_size == predicted:
                    win += 1
                    if lose > 0:
                        ld[lose] += 1
                        lose = 0
                    max_win = max(max_win, win)
                else:
                    lose += 1
                    if win > 0:
                        wd[win] += 1
                        win = 0
                    max_lose = max(max_lose, lose)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing size streak for issue {issue}: {e}")
                continue
        
        # Handle final streak
        current_win = win if win > 0 else 0
        current_lose = lose if lose > 0 else 0
        
        if win > 0:
            wd[win] += 1
        elif lose > 0:
            ld[lose] += 1
        
        result = {
            "current_win_streak": current_win,
            "current_lose_streak": current_lose,
            "max_win_streak": max_win,
            "max_lose_streak": max_lose,
            "win_streak_distribution": dict(wd),
            "lose_streak_distribution": dict(ld),
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "reset_from_issue": reset_from
        }
        
        with redis_lock:
            redis_client.set(REDIS_SIZE_STREAKS_KEY, json.dumps(result))
            redis_client.set(REDIS_CONTROLLED_SIZE_STREAKS_KEY, json.dumps(result))
        
        logger.info(f"📈 Size streaks updated from reset point {reset_from}: win={current_win}, lose={current_lose}")
        
    except Exception as e:
        logger.error(f"❌ Error calculating size streaks: {e}")

# --- Enhanced Main Prediction Functions ---
def run_color_prediction_and_monitor():
    """Enhanced color prediction and monitoring with comprehensive error handling"""
    global rules, current_loss
    
    try:
        logger.info("🔁 Running color prediction cycle...")
        update_color_worker_status("color_prediction", "Running color prediction cycle")
        send_color_worker_heartbeat()
        
        # Check for shutdown signal
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during color prediction")
            return
        
        history = decode_history()
        if not history:
            logger.warning("⚠️ History not found. Skipping color cycle.")
            update_color_worker_status("warning", "No history data available")
            return

        sequence_raw = get_color_sequence(history)
        if not sequence_raw:
            logger.warning("⚠️ Color sequence is empty. Skipping color cycle.")
            update_color_worker_status("warning", "Empty color sequence")
            return
        
        # --- Step 1: Evaluate the PREVIOUS prediction ---
        last_issue_key = max(history.keys(), key=int)
        actual_value = history.get(str(last_issue_key))

        if actual_value is not None:
            actual_color = "Red" if COLOR_MAP[actual_value] == 'R' else "Green"
            
            # Fetch the prediction that was made for this now-completed issue
            with redis_lock:
                last_prediction_raw = redis_client.hget(REDIS_COLOR_PREDICTION_LOG_KEY, str(last_issue_key))
            
            if last_prediction_raw:
                try:
                    last_prediction_data = json.loads(last_prediction_raw.decode() if isinstance(last_prediction_raw, bytes) else last_prediction_raw)
                    last_predicted_color = last_prediction_data.get("next_color")

                    if last_predicted_color == actual_color:
                        current_loss = 0
                        logger.info(f"✅ Correct color prediction for issue #{last_issue_key} ('{actual_color}'). Loss streak reset.")
                    else:
                        current_loss += 1
                        logger.warning(f"❌ Color mismatch for issue #{last_issue_key}: expected '{actual_color}', but predicted '{last_predicted_color}'. Loss streak is now {current_loss}.")
                        
                        # Check for retraining conditions
                        with redis_lock:
                            streak_data_raw = redis_client.get(REDIS_COLOR_STREAKS_KEY)
                        
                        current_streak = json.loads(streak_data_raw.decode() if streak_data_raw else "{}") if streak_data_raw else {}
                        current_lose_streak = current_streak.get("current_lose_streak", 0)

                        if current_loss >= MAX_ALLOWED_LOSS_STREAK or current_lose_streak >= EMERGENCY_LOSS_STREAK:
                            logger.warning(f"🚨 Triggering color rule retraining...")
                            update_color_worker_status("retraining", f"Loss streak: {current_loss}, Current streak: {current_lose_streak}")
                            
                            if current_lose_streak >= EMERGENCY_LOSS_STREAK:
                                logger.warning(f"🚨 Emergency color retrain triggered: current_lose_streak = {current_lose_streak}")
                                train_data = sequence_raw[-300:]
                            else:
                                logger.warning(f"📉 Regular color retrain triggered: current_loss = {current_loss}")
                                train_data = sequence_raw[-TRAINING_WINDOW_SIZE:]

                            with rules_lock:
                                rules_new = retrain_rules(train_data)
                                
                                if rules_new:
                                    logger.info(f"🧠 Color model retrained at {datetime.now().isoformat()} with {len(rules_new)} rules.")
                                    logger.info("📊 Top 5 color patterns after retraining:")
                                    top_rules = sorted(rules_new.items(), key=lambda item: item[1]['total'], reverse=True)[:5]
                                    for rule_pattern, info in top_rules:
                                        logger.info(f"🔍 {rule_pattern} → predict '{info['predict']}' | Accuracy: {info['accuracy']}% | Total: {info['total']}")
                                    
                                    rules.clear()
                                    rules.update(rules_new)
                                    current_loss = 0  # Reset after retraining
                                    logger.info("✅ Color rules updated successfully")
                                else:
                                    logger.error("❌ Color rule retraining failed")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing previous color prediction: {e}")

        # Check for shutdown again
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during color evaluation")
            return

        # --- Step 2: Make and log the NEW prediction for the NEXT issue ---
        try:
            sequence = [c for _, c in sequence_raw]
            next_issue_num = int(last_issue_key) + 1
            
            with rules_lock:
                predicted_code, rule_pattern, confidence, fallback = predict_next_color(sequence, rules)
            
            predicted_color_for_next_issue = "Red" if predicted_code == 'R' else "Green"

            prediction = {
                "issue": str(next_issue_num),
                "observed_sequence": ''.join(sequence[-15:]),
                "next_color": predicted_color_for_next_issue,
                "rule_name": rule_pattern,
                "rule_score": confidence,
                "last_updated": datetime.now(pytz.utc).isoformat()
            }
            
            if log_color_prediction(prediction):
                logger.info(f"✅ New color prediction logged for issue #{prediction['issue']} → '{prediction['next_color']}' via rule '{prediction['rule_name']}'.")
            else:
                logger.error("❌ Failed to log color prediction")
            
        except Exception as e:
            logger.error(f"❌ Error making new color prediction: {e}")

        # --- Step 3: Update all summary stats ---
        try:
            update_prediction_history()
            update_accuracy()
            calculate_color_streaks()
            update_color_worker_status("color_complete", "Color prediction cycle completed")
        except Exception as e:
            logger.error(f"❌ Error updating color statistics: {e}")
            update_color_worker_status("stats_error", f"Stats update error: {str(e)}")
        
    except Exception as e:
        logger.error(f"💥 Error in color prediction cycle: {e}", exc_info=True)
        update_color_worker_status("error", f"Color prediction error: {str(e)}")

def run_size_prediction_and_monitor():
    """Enhanced size prediction and monitoring with comprehensive error handling"""
    global size_rules, current_size_loss

    try:
        logger.info("🔁 Running size prediction cycle...")
        update_color_worker_status("size_prediction", "Running size prediction cycle")
        send_color_worker_heartbeat()
        
        # Check for shutdown signal
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during size prediction")
            return
        
        history = decode_history()
        if not history:
            logger.warning("⚠️ History not found for size. Skipping size cycle.")
            update_color_worker_status("warning", "No history data for size")
            return

        sequence_raw = get_size_sequence(history)
        if not sequence_raw:
            logger.warning("⚠️ Size sequence is empty. Skipping size cycle.")
            update_color_worker_status("warning", "Empty size sequence")
            return
        
        # --- Step 1: Evaluate the PREVIOUS size prediction ---
        last_issue_key = max(history.keys(), key=int)
        actual_value = history.get(str(last_issue_key))

        if actual_value is not None:
            actual_size = "Big" if SIZE_MAP[actual_value] == 'B' else "Small"
            
            # Fetch the prediction that was made for this now-completed issue
            with redis_lock:
                last_prediction_raw = redis_client.hget(REDIS_SIZE_PREDICTION_LOG_KEY, str(last_issue_key))
            
            if last_prediction_raw:
                try:
                    last_prediction_data = json.loads(last_prediction_raw.decode() if isinstance(last_prediction_raw, bytes) else last_prediction_raw)
                    last_predicted_size = last_prediction_data.get("next_size")

                    if last_predicted_size == actual_size:
                        current_size_loss = 0
                        logger.info(f"✅ Correct size prediction for issue #{last_issue_key} ('{actual_size}'). Loss streak reset.")
                    else:
                        current_size_loss += 1
                        logger.warning(f"❌ Size mismatch for issue #{last_issue_key}: expected '{actual_size}', but predicted '{last_predicted_size}'. Loss streak is now {current_size_loss}.")
                        
                        # Check for retraining conditions
                        with redis_lock:
                            streak_data_raw = redis_client.get(REDIS_SIZE_STREAKS_KEY)
                        
                        current_streak = json.loads(streak_data_raw.decode() if streak_data_raw else "{}") if streak_data_raw else {}
                        current_lose_streak = current_streak.get("current_lose_streak", 0)

                        if current_size_loss >= MAX_ALLOWED_LOSS_STREAK or current_lose_streak >= EMERGENCY_LOSS_STREAK:
                            logger.warning(f"🚨 Triggering size rule retraining...")
                            update_color_worker_status("retraining", f"Size loss streak: {current_size_loss}, Current streak: {current_lose_streak}")
                            
                            if current_lose_streak >= EMERGENCY_LOSS_STREAK:
                                logger.warning(f"🚨 Emergency size retrain triggered: current_lose_streak = {current_lose_streak}")
                                train_data = sequence_raw[-300:]
                            else:
                                logger.warning(f"📉 Regular size retrain triggered: current_size_loss = {current_size_loss}")
                                train_data = sequence_raw[-TRAINING_WINDOW_SIZE:]

                            with rules_lock:
                                rules_new = retrain_rules(train_data)
                                
                                if rules_new:
                                    logger.info(f"🧠 Size model retrained at {datetime.now().isoformat()} with {len(rules_new)} rules.")
                                    logger.info("📊 Top 5 size patterns after retraining:")
                                    top_rules = sorted(rules_new.items(), key=lambda item: item[1]['total'], reverse=True)[:5]
                                    for rule_pattern, info in top_rules:
                                        logger.info(f"🔍 {rule_pattern} → predict '{info['predict']}' | Accuracy: {info['accuracy']}% | Total: {info['total']}")
                                    
                                    size_rules.clear()
                                    size_rules.update(rules_new)
                                    current_size_loss = 0  # Reset after retraining
                                    logger.info("✅ Size rules updated successfully")
                                else:
                                    logger.error("❌ Size rule retraining failed")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing previous size prediction: {e}")

        # Check for shutdown again
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during size evaluation")
            return

        # --- Step 2: Make and log the NEW size prediction for the NEXT issue ---
        try:
            sequence = [c for _, c in sequence_raw]
            next_issue_num = int(last_issue_key) + 1
            
            with rules_lock:
                predicted_code, rule_pattern, confidence, fallback = predict_next_size(sequence, size_rules)
            
            predicted_size_for_next_issue = "Big" if predicted_code == 'B' else "Small"

            prediction = {
                "issue": str(next_issue_num),
                "observed_sequence": ''.join(sequence[-15:]),
                "next_size": predicted_size_for_next_issue,
                "rule_name": rule_pattern,
                "rule_score": confidence,
                "last_updated": datetime.now(pytz.utc).isoformat()
            }
            
            if log_size_prediction(prediction):
                logger.info(f"✅ New size prediction logged for issue #{prediction['issue']} → '{prediction['next_size']}' via rule '{prediction['rule_name']}'.")
            else:
                logger.error("❌ Failed to log size prediction")
            
        except Exception as e:
            logger.error(f"❌ Error making new size prediction: {e}")

        # --- Step 3: Update all summary stats ---
        try:
            update_size_prediction_history()
            update_size_accuracy()
            calculate_size_streaks()
            update_color_worker_status("size_complete", "Size prediction cycle completed")
        except Exception as e:
            logger.error(f"❌ Error updating size statistics: {e}")
            update_color_worker_status("stats_error", f"Size stats update error: {str(e)}")
        
    except Exception as e:
        logger.error(f"💥 Error in size prediction cycle: {e}", exc_info=True)
        update_color_worker_status("error", f"Size prediction error: {str(e)}")

def run_dual_prediction_cycle():
    """Enhanced dual prediction cycle with error handling and graceful shutdown"""
    try:
        logger.info("🚀 Starting dual prediction cycle...")
        update_color_worker_status("running", "Starting dual prediction cycle")
        
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested before dual cycle start")
            return
        
        # Run color prediction
        run_color_prediction_and_monitor()
        
        # Check for shutdown between predictions
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested after color prediction")
            return
        
        # Run size prediction
        run_size_prediction_and_monitor()
        
        logger.info("✅ Dual prediction cycle completed successfully")
        update_color_worker_status("idle", "Dual prediction cycle completed")
        send_color_worker_heartbeat()
        
    except Exception as e:
        logger.error(f"💥 Error in dual prediction cycle: {e}", exc_info=True)
        update_color_worker_status("error", f"Dual cycle error: {str(e)}")

# --- Graceful Shutdown Handling ---
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    signal_name = signal.Signals(signum).name
    logger.info(f"🛑 Received {signal_name} signal. Initiating graceful shutdown...")
    update_color_worker_status("shutting_down", f"Received {signal_name} signal")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 COLOR & SIZE WORKER MONITOR STARTING")
    logger.info("=" * 60)
    
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
    logger.info(f"🔗 Redis: {'Connected' if redis_client else 'Disconnected'}")
    
    try:
        # Initialize models
        logger.info("🔧 Initializing prediction models...")
        update_color_worker_status("initializing", "Loading initial models")
        
        history = decode_history()
        if history:
            color_sequence = get_color_sequence(history)
            size_sequence = get_size_sequence(history)
            
            with rules_lock:
                if color_sequence:
                    rules = retrain_rules(color_sequence[-TRAINING_WINDOW_SIZE:])
                    logger.info(f"🎨 Color model initialized with {len(rules)} rules")
                else:
                    logger.warning("⚠️ No color sequence data for initial training")
                
                if size_sequence:
                    size_rules = retrain_rules(size_sequence[-TRAINING_WINDOW_SIZE:])
                    logger.info(f"📏 Size model initialized with {len(size_rules)} rules")
                else:
                    logger.warning("⚠️ No size sequence data for initial training")
        else:
            logger.warning("⚠️ No history found on startup. Starting with empty rulesets.")
        
        # Log top rules
        if rules:
            logger.info("🔍 Top 5 initial color rules:")
            top_color_rules = sorted(rules.items(), key=lambda x: x[1]['total'], reverse=True)[:5]
            for pattern, info in top_color_rules:
                logger.info(f"  {pattern} → {info['predict']} ({info['accuracy']}%, {info['total']} uses)")
        
        if size_rules:
            logger.info("🔍 Top 5 initial size rules:")
            top_size_rules = sorted(size_rules.items(), key=lambda x: x[1]['total'], reverse=True)[:5]
            for pattern, info in top_size_rules:
                logger.info(f"  {pattern} → {info['predict']} ({info['accuracy']}%, {info['total']} uses)")
        
        # Schedule the job
        schedule.every().minute.at(":01").do(run_dual_prediction_cycle)
        logger.info("🕐 Scheduled to run dual prediction cycle every minute at :01 seconds")
        
        update_color_worker_status("ready", "Worker initialized and scheduled")
        
        # Main loop
        logger.info("🔄 Starting main worker loop...")
        last_heartbeat = time.time()
        heartbeat_interval = 30  # seconds
        
        while not shutdown_event.is_set():
            try:
                # Run pending scheduled jobs
                schedule.run_pending()
                
                # Send periodic heartbeat
                current_time = time.time()
                if current_time - last_heartbeat >= heartbeat_interval:
                    send_color_worker_heartbeat()
                    last_heartbeat = current_time
                
                # Sleep briefly to prevent busy waiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"💥 Error in main loop: {e}", exc_info=True)
                update_color_worker_status("loop_error", f"Main loop error: {str(e)}")
                time.sleep(5)  # Wait before retrying
        
        logger.info("🏁 Worker shutdown complete")
        update_color_worker_status("stopped", "Worker gracefully stopped")
        
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
        update_color_worker_status("interrupted", "Keyboard interrupt")
    except Exception as e:
        logger.critical(f"💥 Critical error in worker: {e}", exc_info=True)
        update_color_worker_status("critical", f"Critical error: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("👋 Color Worker Monitor terminated")
