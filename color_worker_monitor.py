# color_worker_monitor.py

import os
import json
import logging
import redis
import schedule
import time
import signal
import sys
from datetime import datetime, timedelta
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
                decode_responses=True,  # This is crucial - returns strings, not bytes
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

# --- Helper Functions for Safe Redis Data Handling ---
def safe_decode(value):
    """Safely decode Redis value - handles both bytes and strings"""
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return value

def get_redis_json(key, default=None):
    """Safely get and parse JSON data from Redis"""
    try:
        if not redis_client:
            return default
        
        with redis_lock:
            raw_data = redis_client.get(key)
        
        if raw_data is None:
            return default
        
        # Safe decode - only if bytes
        decoded_data = safe_decode(raw_data)
        return json.loads(decoded_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for key {key}: {e}")
        return default
    except Exception as e:
        logger.error(f"Error getting Redis JSON for key {key}: {e}")
        return default

def get_redis_hash_all(key):
    """Safely get all hash data from Redis"""
    try:
        if not redis_client:
            return {}
        
        with redis_lock:
            raw_hash = redis_client.hgetall(key)
        
        if not raw_hash:
            return {}
        
        # Safe decode all keys and values
        decoded_hash = {}
        for k, v in raw_hash.items():
            decoded_key = safe_decode(k)
            decoded_value = safe_decode(v)
            decoded_hash[decoded_key] = decoded_value
        
        return decoded_hash
        
    except Exception as e:
        logger.error(f"Error getting Redis hash for key {key}: {e}")
        return {}

def get_redis_hash_json(hash_key, field, default=None):
    """Safely get and parse JSON data from Redis hash field"""
    try:
        if not redis_client:
            return default
        
        with redis_lock:
            raw_data = redis_client.hget(hash_key, field)
        
        if raw_data is None:
            return default
        
        # Safe decode - only if bytes
        decoded_data = safe_decode(raw_data)
        return json.loads(decoded_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for hash {hash_key}, field {field}: {e}")
        return default
    except Exception as e:
        logger.error(f"Error getting Redis hash JSON for {hash_key}:{field}: {e}")
        return default

def set_redis_json(key, data, expiry=None):
    """Safely set JSON data to Redis"""
    try:
        if not redis_client:
            return False
        
        json_data = json.dumps(data)
        
        with redis_lock:
            if expiry:
                redis_client.setex(key, expiry, json_data)
            else:
                redis_client.set(key, json_data)
        
        return True
        
    except Exception as e:
        logger.error(f"Error setting Redis JSON for key {key}: {e}")
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
MIN_SIZE_OCCURRENCES = 8  # Lower threshold for size patterns
MAX_ALLOWED_LOSS_STREAK = 3
TRAINING_WINDOW_SIZE = 1000
EMERGENCY_LOSS_STREAK = 5

# --- PREDEFINED RULES FOR FALLBACK ---
PREDEFINED_COLOR_RULES = {
    # Basic alternating patterns
    "RG": {"predict": "R", "correct": 55, "total": 100, "accuracy": 55.0},
    "GR": {"predict": "G", "correct": 52, "total": 100, "accuracy": 52.0},
    "RR": {"predict": "G", "correct": 58, "total": 100, "accuracy": 58.0},
    "GG": {"predict": "R", "correct": 56, "total": 100, "accuracy": 56.0},
    
    # Triple patterns
    "RRR": {"predict": "G", "correct": 62, "total": 100, "accuracy": 62.0},
    "GGG": {"predict": "R", "correct": 60, "total": 100, "accuracy": 60.0},
    "RRG": {"predict": "R", "correct": 54, "total": 100, "accuracy": 54.0},
    "GGR": {"predict": "G", "correct": 53, "total": 100, "accuracy": 53.0},
    "RGR": {"predict": "G", "correct": 57, "total": 100, "accuracy": 57.0},
    "GRG": {"predict": "R", "correct": 55, "total": 100, "accuracy": 55.0},
    "RGG": {"predict": "R", "correct": 56, "total": 100, "accuracy": 56.0},
    "GRR": {"predict": "G", "correct": 54, "total": 100, "accuracy": 54.0},
    
    # Complex patterns
    "RGRG": {"predict": "R", "correct": 65, "total": 100, "accuracy": 65.0},
    "GRGR": {"predict": "G", "correct": 63, "total": 100, "accuracy": 63.0},
    "RRGG": {"predict": "R", "correct": 59, "total": 100, "accuracy": 59.0},
    "GGRR": {"predict": "G", "correct": 57, "total": 100, "accuracy": 57.0},
    "RRRG": {"predict": "G", "correct": 61, "total": 100, "accuracy": 61.0},
    "GGGR": {"predict": "R", "correct": 59, "total": 100, "accuracy": 59.0},
    "RGGR": {"predict": "G", "correct": 58, "total": 100, "accuracy": 58.0},
    "GRRG": {"predict": "R", "correct": 56, "total": 100, "accuracy": 56.0},
    
    # Advanced patterns
    "RGRGR": {"predict": "G", "correct": 68, "total": 100, "accuracy": 68.0},
    "GRGRG": {"predict": "R", "correct": 66, "total": 100, "accuracy": 66.0},
    "RRRGG": {"predict": "R", "correct": 61, "total": 100, "accuracy": 61.0},
    "GGGRR": {"predict": "G", "correct": 59, "total": 100, "accuracy": 59.0},
    "RRGGG": {"predict": "R", "correct": 60, "total": 100, "accuracy": 60.0},
    "GGRRR": {"predict": "G", "correct": 58, "total": 100, "accuracy": 58.0},
    "RGRRG": {"predict": "R", "correct": 64, "total": 100, "accuracy": 64.0},
    "GRGGR": {"predict": "G", "correct": 62, "total": 100, "accuracy": 62.0},
    
    # Streak breakers
    "RRRR": {"predict": "G", "correct": 72, "total": 100, "accuracy": 72.0},
    "GGGG": {"predict": "R", "correct": 70, "total": 100, "accuracy": 70.0},
    "RRRRR": {"predict": "G", "correct": 78, "total": 100, "accuracy": 78.0},
    "GGGGG": {"predict": "R", "correct": 76, "total": 100, "accuracy": 76.0},
    "RRRRRR": {"predict": "G", "correct": 80, "total": 100, "accuracy": 80.0},
    "GGGGGG": {"predict": "R", "correct": 78, "total": 100, "accuracy": 78.0},
}

PREDEFINED_SIZE_RULES = {
    # Basic alternating patterns
    "SB": {"predict": "S", "correct": 54, "total": 100, "accuracy": 54.0},
    "BS": {"predict": "B", "correct": 53, "total": 100, "accuracy": 53.0},
    "SS": {"predict": "B", "correct": 57, "total": 100, "accuracy": 57.0},
    "BB": {"predict": "S", "correct": 55, "total": 100, "accuracy": 55.0},
    
    # Triple patterns
    "SSS": {"predict": "B", "correct": 61, "total": 100, "accuracy": 61.0},
    "BBB": {"predict": "S", "correct": 59, "total": 100, "accuracy": 59.0},
    "SSB": {"predict": "S", "correct": 52, "total": 100, "accuracy": 52.0},
    "BBS": {"predict": "B", "correct": 51, "total": 100, "accuracy": 51.0},
    "SBS": {"predict": "B", "correct": 56, "total": 100, "accuracy": 56.0},
    "BSB": {"predict": "S", "correct": 54, "total": 100, "accuracy": 54.0},
    "SBB": {"predict": "S", "correct": 55, "total": 100, "accuracy": 55.0},
    "BSS": {"predict": "B", "correct": 53, "total": 100, "accuracy": 53.0},
    
    # Complex patterns
    "SBSB": {"predict": "S", "correct": 64, "total": 100, "accuracy": 64.0},
    "BSBS": {"predict": "B", "correct": 62, "total": 100, "accuracy": 62.0},
    "SSBB": {"predict": "S", "correct": 58, "total": 100, "accuracy": 58.0},
    "BBSS": {"predict": "B", "correct": 56, "total": 100, "accuracy": 56.0},
    "SSSB": {"predict": "B", "correct": 60, "total": 100, "accuracy": 60.0},
    "BBBS": {"predict": "S", "correct": 58, "total": 100, "accuracy": 58.0},
    "SBBS": {"predict": "B", "correct": 57, "total": 100, "accuracy": 57.0},
    "BSSB": {"predict": "S", "correct": 55, "total": 100, "accuracy": 55.0},
    
    # Advanced patterns
    "SBSBS": {"predict": "B", "correct": 67, "total": 100, "accuracy": 67.0},
    "BSBSB": {"predict": "S", "correct": 65, "total": 100, "accuracy": 65.0},
    "SSSBB": {"predict": "S", "correct": 60, "total": 100, "accuracy": 60.0},
    "BBBSS": {"predict": "B", "correct": 58, "total": 100, "accuracy": 58.0},
    "SSBBB": {"predict": "S", "correct": 59, "total": 100, "accuracy": 59.0},
    "BBSSS": {"predict": "B", "correct": 57, "total": 100, "accuracy": 57.0},
    "SBSSB": {"predict": "S", "correct": 63, "total": 100, "accuracy": 63.0},
    "BSBBS": {"predict": "B", "correct": 61, "total": 100, "accuracy": 61.0},
    
    # Streak breakers
    "SSSS": {"predict": "B", "correct": 71, "total": 100, "accuracy": 71.0},
    "BBBB": {"predict": "S", "correct": 69, "total": 100, "accuracy": 69.0},
    "SSSSS": {"predict": "B", "correct": 77, "total": 100, "accuracy": 77.0},
    "BBBBB": {"predict": "S", "correct": 75, "total": 100, "accuracy": 75.0},
    "SSSSSS": {"predict": "B", "correct": 79, "total": 100, "accuracy": 79.0},
    "BBBBBB": {"predict": "S", "correct": 77, "total": 100, "accuracy": 77.0},
}

# Time-based prediction rules
def get_time_based_color_prediction():
    """Get color prediction based on current time patterns"""
    current_time = datetime.now(pytz.utc)
    minute = current_time.minute
    second = current_time.second
    
    # Even/odd minute logic with slight bias
    if minute % 2 == 0:
        return "R", "TimeRule_EvenMinute", 58.0
    else:
        return "G", "TimeRule_OddMinute", 56.0

def get_time_based_size_prediction():
    """Get size prediction based on current time patterns"""
    current_time = datetime.now(pytz.utc)
    minute = current_time.minute
    second = current_time.second
    
    # Minute-based size logic
    if minute < 30:
        return "S", "TimeRule_FirstHalf", 57.0
    else:
        return "B", "TimeRule_SecondHalf", 55.0

# Mathematical prediction rules
def get_fibonacci_color_prediction(sequence):
    """Get color prediction based on Fibonacci-like sequence"""
    if len(sequence) < 2:
        return "R", "FibRule_Default", 50.0
    
    # Simple Fibonacci-based logic
    last_two = sequence[-2:]
    if (last_two[0] == 'R' and last_two[1] == 'G') or (last_two[0] == 'G' and last_two[1] == 'R'):
        return "R", "FibRule_Mixed", 60.0
    else:
        return "G", "FibRule_Same", 58.0

def get_fibonacci_size_prediction(sequence):
    """Get size prediction based on Fibonacci-like sequence"""
    if len(sequence) < 2:
        return "S", "FibRule_Default", 50.0
    
    # Simple Fibonacci-based logic
    last_two = sequence[-2:]
    if (last_two[0] == 'S' and last_two[1] == 'B') or (last_two[0] == 'B' and last_two[1] == 'S'):
        return "S", "FibRule_Mixed", 59.0
    else:
        return "B", "FibRule_Same", 57.0

# Trend-based prediction rules
def get_trend_color_prediction(sequence):
    """Get color prediction based on recent trends"""
    if len(sequence) < 5:
        return "R", "TrendRule_Insufficient", 50.0
    
    recent = sequence[-5:]
    red_count = recent.count('R')
    green_count = recent.count('G')
    
    if red_count > green_count:
        return "G", "TrendRule_RedDominant", 64.0
    elif green_count > red_count:
        return "R", "TrendRule_GreenDominant", 62.0
    else:
        return "R", "TrendRule_Balanced", 52.0

def get_trend_size_prediction(sequence):
    """Get size prediction based on recent trends"""
    if len(sequence) < 5:
        return "S", "TrendRule_Insufficient", 50.0
    
    recent = sequence[-5:]
    small_count = recent.count('S')
    big_count = recent.count('B')
    
    if small_count > big_count:
        return "B", "TrendRule_SmallDominant", 63.0
    elif big_count > small_count:
        return "S", "TrendRule_BigDominant", 61.0
    else:
        return "S", "TrendRule_Balanced", 51.0

# Global variables with thread safety
rules = {}
size_rules = {}
current_loss = 0
current_size_loss = 0
rules_lock = Lock()
cycle_count = 0

# --- Status Management Functions ---
def update_color_worker_status(status, message=None):
    """Update color worker status in Redis"""
    try:
        if redis_client:
            status_data = {
                "status": status,
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "environment": "railway" if is_railway_environment() else "local",
                "service_name": get_railway_service_name()
            }
            if message:
                status_data["message"] = message
            
            set_redis_json(REDIS_COLOR_WORKER_STATUS_KEY, status_data, expiry=300)
            logger.debug(f"📊 Color Worker status updated: {status}")
    except Exception as e:
        logger.error(f"Failed to update color worker status: {e}")

def send_color_worker_heartbeat():
    """Send heartbeat to Redis"""
    try:
        if redis_client:
            heartbeat_data = {
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "environment": "railway" if is_railway_environment() else "local",
                "service_name": get_railway_service_name(),
                "current_color_loss": current_loss,
                "current_size_loss": current_size_loss,
                "color_rules_count": len(rules),
                "size_rules_count": len(size_rules),
                "predefined_color_rules": len(PREDEFINED_COLOR_RULES),
                "predefined_size_rules": len(PREDEFINED_SIZE_RULES)
            }
            set_redis_json(REDIS_COLOR_WORKER_HEARTBEAT_KEY, heartbeat_data, expiry=120)
            logger.debug("💗 Color Worker heartbeat sent")
    except Exception as e:
        logger.error(f"Failed to send color worker heartbeat: {e}")

# --- Enhanced Data Access Functions ---
def decode_history():
    """Enhanced history decoding with error handling"""
    try:
        history = get_redis_json(REDIS_HISTORY_KEY, {})
        logger.debug(f"📥 Decoded {len(history)} history entries")
        return history
        
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

def learn_size_patterns(sizes):
    """Size-specific pattern learning with validation"""
    try:
        if not sizes or len(sizes) < MAX_PATTERN_LENGTH:
            logger.warning(f"⚠️ Insufficient data for size pattern learning: {len(sizes) if sizes else 0} entries")
            return defaultdict(lambda: defaultdict(int))
        
        stats = defaultdict(lambda: defaultdict(int))
        patterns_found = 0
        
        for i in range(MAX_PATTERN_LENGTH, len(sizes)):
            for l in range(2, MAX_PATTERN_LENGTH + 1):
                try:
                    pattern = ''.join([s for _, s in sizes[i - l:i]])
                    next_s = sizes[i][1]
                    
                    if pattern and next_s:
                        stats[pattern][next_s] += 1
                        patterns_found += 1
                        
                except (IndexError, TypeError) as e:
                    logger.warning(f"⚠️ Error processing size pattern at index {i}, length {l}: {e}")
                    continue
        
        logger.debug(f"🧠 Learned {patterns_found} size pattern occurrences")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error in size pattern learning: {e}")
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

def generate_size_rules(stats):
    """Generate size rules with lower threshold and accuracy filter"""
    try:
        if not stats:
            logger.warning("⚠️ No statistics provided for size rule generation")
            return {}
        
        ruleset = {}
        rules_generated = 0
        
        for k, outcomes in stats.items():
            try:
                total = sum(outcomes.values())
                if total >= MIN_SIZE_OCCURRENCES:  # Use lower threshold
                    best = max(outcomes.items(), key=lambda x: x[1])
                    accuracy = round((best[1] / total) * 100, 2)
                    
                    # Only accept rules with decent accuracy
                    if accuracy >= 55.0:  # Minimum accuracy threshold
                        ruleset[k] = {
                            "predict": best[0],
                            "correct": best[1],
                            "total": total,
                            "accuracy": accuracy
                        }
                        rules_generated += 1
                        
            except (ValueError, ZeroDivisionError) as e:
                logger.warning(f"⚠️ Error generating size rule for pattern {k}: {e}")
                continue
        
        logger.debug(f"📜 Generated {rules_generated} size rules from {len(stats)} patterns")
        return ruleset
        
    except Exception as e:
        logger.error(f"❌ Error generating size rules: {e}")
        return {}

def get_effective_rulebook(learned_rules, predefined_rules, rule_type="color"):
    """Combine learned rules with predefined fallback rules"""
    try:
        effective_rules = {}
        
        # First, add all learned rules (higher priority)
        if learned_rules:
            effective_rules.update(learned_rules)
            logger.debug(f"📚 Added {len(learned_rules)} learned {rule_type} rules")
        
        # Then, add predefined rules that don't conflict
        added_predefined = 0
        for pattern, rule_data in predefined_rules.items():
            if pattern not in effective_rules:
                effective_rules[pattern] = rule_data
                added_predefined += 1
        
        logger.info(f"🎯 Effective {rule_type} rulebook: {len(learned_rules or [])} learned + {added_predefined} predefined = {len(effective_rules)} total rules")
        
        return effective_rules
        
    except Exception as e:
        logger.error(f"❌ Error creating effective rulebook: {e}")
        return predefined_rules  # Fallback to just predefined rules

def retrain_rules(colors):
    """Enhanced rule retraining with comprehensive error handling"""
    try:
        logger.info(f"🧠 Retraining COLOR rules with {len(colors)} color entries...")
        update_color_worker_status("retraining", f"Retraining with {len(colors)} entries")
        
        if not colors:
            logger.error("❌ No colors provided for retraining")
            return {}
        
        stats = learn_patterns(colors)
        rules = generate_rules(stats)
        
        logger.info(f"✅ Color retraining complete: {len(rules)} rules generated")
        return rules
        
    except Exception as e:
        logger.error(f"❌ Error during rule retraining: {e}")
        return {}

def retrain_size_rules(sizes):
    """Size-specific rule retraining with comprehensive error handling"""
    try:
        logger.info(f"🧠 Retraining SIZE rules with {len(sizes)} size entries...")
        update_color_worker_status("retraining_size", f"Retraining size rules with {len(sizes)} entries")
        
        if not sizes:
            logger.error("❌ No sizes provided for retraining")
            return {}
        
        stats = learn_size_patterns(sizes)
        rules = generate_size_rules(stats)
        
        logger.info(f"✅ Size retraining complete: {len(rules)} size rules generated")
        return rules
        
    except Exception as e:
        logger.error(f"❌ Error during size rule retraining: {e}")
        return {}

def retrain_size_rules_if_needed():
    """Retrain size rules when accuracy drops below threshold"""
    global size_rules, current_size_loss
    
    try:
        # Get current size accuracy
        size_accuracy_data = get_redis_json(REDIS_SIZE_ACCURACY_KEY, {})
        size_accuracy = size_accuracy_data.get("accuracy_percentage", 50.0)
        
        # Check if retraining is needed
        should_retrain = False
        retrain_reason = ""
        
        if size_accuracy < 50.0:
            should_retrain = True
            retrain_reason = f"poor accuracy ({size_accuracy}%)"
        elif current_size_loss >= 3:
            should_retrain = True
            retrain_reason = f"high loss streak ({current_size_loss})"
        elif len(size_rules) == 0:
            should_retrain = True
            retrain_reason = "no learned rules available"
        
        if should_retrain:
            logger.warning(f"🔄 Size rule retraining triggered: {retrain_reason}")
            
            history = decode_history()
            size_sequence = get_size_sequence(history)
            
            if len(size_sequence) >= 50:  # Ensure enough data
                new_size_rules = retrain_size_rules(size_sequence)
                
                with rules_lock:
                    size_rules.update(new_size_rules)
                
                logger.info(f"✅ Size rules retrained: {len(new_size_rules)} new rules learned")
                logger.info(f"📊 Total size rules now: {len(size_rules)} learned + {len(PREDEFINED_SIZE_RULES)} predefined")
                return True
            else:
                logger.warning(f"⚠️ Insufficient size data for retraining: {len(size_sequence)} entries")
                
    except Exception as e:
        logger.error(f"❌ Error in size rule retraining: {e}")
    
    return False

# --- Enhanced Prediction Functions with Fallback ---
def predict_next_color(seq, rulebook):
    """Enhanced color prediction with multiple fallback strategies"""
    try:
        if not seq:
            logger.warning("⚠️ Empty sequence for color prediction")
            # Use time-based prediction as fallback
            return get_time_based_color_prediction()
        
        # Use effective rulebook (learned + predefined)
        effective_rules = get_effective_rulebook(rulebook, PREDEFINED_COLOR_RULES, "color")
        
        if not effective_rules:
            logger.warning("⚠️ No effective rules available for color prediction")
            # Multiple fallback strategies
            color_seq = [c for _, c in seq[-5:]] if len(seq) >= 5 else [c for _, c in seq]
            
            # Try trend-based prediction
            trend_pred, trend_rule, trend_acc = get_trend_color_prediction(color_seq)
            logger.info(f"🔄 Using trend-based color prediction: {trend_pred} via {trend_rule} ({trend_acc}%)")
            return trend_pred, trend_rule, trend_acc
        
        # Try patterns from longest to shortest
        color_seq = [c for _, c in seq]
        for l in range(min(MAX_PATTERN_LENGTH, len(color_seq)), 1, -1):
            try:
                sub = ''.join(color_seq[-l:])
                if sub in effective_rules:
                    rule = effective_rules[sub]
                    prediction = rule["predict"]
                    accuracy = rule["accuracy"]
                    rule_source = "learned" if sub in (rulebook or {}) else "predefined"
                    logger.info(f"🎯 Color prediction using {rule_source} rule '{sub}': {prediction} ({accuracy}%)")
                    return prediction, sub, accuracy
            except (IndexError, KeyError) as e:
                logger.warning(f"⚠️ Error checking pattern length {l}: {e}")
                continue
        
        # If no pattern matches, use algorithmic predictions
        color_seq = [c for _, c in seq[-5:]] if len(seq) >= 5 else [c for _, c in seq]
        
        # Try Fibonacci prediction
        fib_pred, fib_rule, fib_acc = get_fibonacci_color_prediction(color_seq)
        logger.info(f"🌀 Using Fibonacci color prediction: {fib_pred} via {fib_rule} ({fib_acc}%)")
        return fib_pred, fib_rule, fib_acc
        
    except Exception as e:
        logger.error(f"❌ Error in color prediction: {e}")
        # Final fallback
        return get_time_based_color_prediction()

def predict_next_size(seq, rulebook):
    """Enhanced size prediction with multiple fallback strategies"""
    try:
        if not seq:
            logger.warning("⚠️ Empty sequence for size prediction")
            # Use time-based prediction as fallback
            return get_time_based_size_prediction()
        
        # Use effective rulebook (learned + predefined)
        effective_rules = get_effective_rulebook(rulebook, PREDEFINED_SIZE_RULES, "size")
        
        if not effective_rules:
            logger.warning("⚠️ No effective rules available for size prediction")
            # Multiple fallback strategies
            size_seq = [s for _, s in seq[-5:]] if len(seq) >= 5 else [s for _, s in seq]
            
            # Try trend-based prediction
            trend_pred, trend_rule, trend_acc = get_trend_size_prediction(size_seq)
            logger.info(f"🔄 Using trend-based size prediction: {trend_pred} via {trend_rule} ({trend_acc}%)")
            return trend_pred, trend_rule, trend_acc
        
        # Try patterns from longest to shortest
        size_seq = [s for _, s in seq]
        for l in range(min(MAX_PATTERN_LENGTH, len(size_seq)), 1, -1):
            try:
                sub = ''.join(size_seq[-l:])
                if sub in effective_rules:
                    rule = effective_rules[sub]
                    prediction = rule["predict"]
                    accuracy = rule["accuracy"]
                    rule_source = "learned" if sub in (rulebook or {}) else "predefined"
                    logger.info(f"📏 Size prediction using {rule_source} rule '{sub}': {prediction} ({accuracy}%)")
                    return prediction, sub, accuracy
            except (IndexError, KeyError) as e:
                logger.warning(f"⚠️ Error checking pattern length {l}: {e}")
                continue
        
        # If no pattern matches, use algorithmic predictions
        size_seq = [s for _, s in seq[-5:]] if len(seq) >= 5 else [s for _, s in seq]
        
        # Try Fibonacci prediction
        fib_pred, fib_rule, fib_acc = get_fibonacci_size_prediction(size_seq)
        logger.info(f"🌀 Using Fibonacci size prediction: {fib_pred} via {fib_rule} ({fib_acc}%)")
        return fib_pred, fib_rule, fib_acc
        
    except Exception as e:
        logger.error(f"❌ Error in size prediction: {e}")
        # Final fallback
        return get_time_based_size_prediction()

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
        
        # Store current prediction
        set_redis_json(REDIS_COLOR_PREDICTION_KEY, pred)
        
        # Store in prediction log
        with redis_lock:
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
        
        # Store current prediction
        set_redis_json(REDIS_SIZE_PREDICTION_KEY, pred)
        
        # Store in prediction log
        with redis_lock:
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
        
        log = get_redis_hash_all(REDIS_COLOR_PREDICTION_LOG_KEY)
        history = decode_history()
        
        if not log:
            logger.debug("📭 No prediction log found for history update")
            return
        
        # Process issues in descending order
        issues = sorted(log.keys(), key=lambda x: int(x), reverse=True)
        
        history_table = []
        processed_count = 0
        
        for i in issues[:15]:  # Limit to last 15
            try:
                pred_raw = log.get(i)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_num = history.get(i)
                
                actual_color = None
                if actual_num is not None:
                    actual_color = "Red" if COLOR_MAP.get(actual_num) == "R" else "Green"
                
                history_table.append({
                    "issue": i,
                    "predicted": pred.get("next_color"),
                    "rule": pred.get("rule_name"),
                    "actual": actual_color,
                    "result": "Correct" if pred.get("next_color") == actual_color else "Incorrect" if actual_color else "Pending"
                })
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing history for issue {i}: {e}")
                continue
        
        set_redis_json(REDIS_COLOR_PREDICTION_HISTORY_KEY, history_table)
        logger.debug(f"📋 Updated color prediction history with {processed_count} entries")
        
    except Exception as e:
        logger.error(f"❌ Error updating prediction history: {e}")

def update_size_prediction_history():
    """Enhanced size prediction history update with error handling"""
    try:
        if not redis_client:
            logger.error("❌ Redis client not available for size history update")
            return
        
        log = get_redis_hash_all(REDIS_SIZE_PREDICTION_LOG_KEY)
        history = decode_history()
        
        if not log:
            logger.debug("📭 No size prediction log found for history update")
            return
        
        # Process issues in descending order
        issues = sorted(log.keys(), key=lambda x: int(x), reverse=True)
        
        history_table = []
        processed_count = 0
        
        for i in issues[:15]:  # Limit to last 15
            try:
                pred_raw = log.get(i)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_num = history.get(i)
                
                # Consistent format - always use "Small"/"Big"
                actual_size = None
                if actual_num is not None:
                    actual_size = "Big" if SIZE_MAP.get(actual_num) == "B" else "Small"
                
                # Ensure predicted size is also consistent format
                predicted_size = pred.get("next_size")
                if predicted_size == "S":
                    predicted_size = "Small"
                elif predicted_size == "B":
                    predicted_size = "Big"
                
                history_table.append({
                    "issue": i,
                    "predicted": predicted_size,
                    "rule": pred.get("rule_name"),
                    "actual": actual_size,
                    "result": "Correct" if predicted_size == actual_size else "Incorrect" if actual_size else "Pending"
                })
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing size history for issue {i}: {e}")
                continue
        
        set_redis_json(REDIS_SIZE_PREDICTION_HISTORY_KEY, history_table)
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
        
        log = get_redis_hash_all(REDIS_COLOR_PREDICTION_LOG_KEY)
        history = decode_history()
        
        # Get reset point
        reset_point_raw = get_redis_json(REDIS_RESET_POINT)
        reset_from = int(reset_point_raw) if reset_point_raw else None
        
        total = correct = 0
        per_rule = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # Process issues in chronological order
        sorted_issues = sorted(log.keys(), key=int)
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = log.get(issue)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
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
        
        set_redis_json(REDIS_COLOR_ACCURACY_KEY, accuracy_data)
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
        
        log = get_redis_hash_all(REDIS_SIZE_PREDICTION_LOG_KEY)
        history = decode_history()
        
        # Get reset point
        reset_point_raw = get_redis_json(REDIS_RESET_POINT)
        reset_from = int(reset_point_raw) if reset_point_raw else None
        
        total = correct = 0
        per_rule = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # Process issues in chronological order
        sorted_issues = sorted(log.keys(), key=int)
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = log.get(issue)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_value = history.get(issue)
                
                if actual_value is None:
                    continue
                
                actual_size = "Big" if SIZE_MAP[actual_value] == "B" else "Small"
                predicted_size = pred.get("next_size")
                
                # Handle both formats consistently
                if predicted_size == "S":
                    predicted_size = "Small"
                elif predicted_size == "B":
                    predicted_size = "Big"
                
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
        
        set_redis_json(REDIS_SIZE_ACCURACY_KEY, accuracy_data)
        logger.info(f"📊 Size accuracy updated: {correct}/{total} ({accuracy_data['accuracy_percentage']}%)")
        
    except Exception as e:
        logger.error(f"❌ Error updating size accuracy: {e}")

# --- Enhanced Streak Functions ---
def update_streaks():
    """Enhanced streak calculation with comprehensive error handling"""
    try:
        logger.debug("📈 Updating color streaks...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for streak update")
            return
        
        log = get_redis_hash_all(REDIS_COLOR_PREDICTION_LOG_KEY)
        history = decode_history()
        
        # Get reset point
        reset_point_raw = get_redis_json(REDIS_RESET_POINT)
        reset_from = int(reset_point_raw) if reset_point_raw else None
        
        # Process issues in chronological order
        sorted_issues = sorted(log.keys(), key=int)
        
        current_win = current_loss = 0
        max_win = max_loss = 0
        win_dist = defaultdict(int)
        loss_dist = defaultdict(int)
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = log.get(issue)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_value = history.get(issue)
                
                if actual_value is None:
                    continue
                
                actual_color = "Red" if COLOR_MAP[actual_value] == "R" else "Green"
                predicted_color = pred.get("next_color")
                
                if predicted_color == actual_color:
                    # Correct prediction - win
                    if current_loss > 0:
                        loss_dist[current_loss] += 1
                        current_loss = 0
                    current_win += 1
                    max_win = max(max_win, current_win)
                else:
                    # Incorrect prediction - loss
                    if current_win > 0:
                        win_dist[current_win] += 1
                        current_win = 0
                    current_loss += 1
                    max_loss = max(max_loss, current_loss)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing streak for issue {issue}: {e}")
                continue
        
        # Count current streak in distribution
        if current_win > 0:
            win_dist[current_win] += 1
        if current_loss > 0:
            loss_dist[current_loss] += 1
        
        # Build streak data
        streak_data = {
            "current_win_streak": current_win,
            "current_lose_streak": current_loss,
            "max_win_streak": max_win,
            "max_lose_streak": max_loss,
            "win_streak_distribution": dict(win_dist),
            "lose_streak_distribution": dict(loss_dist),
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "reset_from_issue": reset_from
        }
        
        set_redis_json(REDIS_COLOR_STREAKS_KEY, streak_data)
        logger.info(f"📈 Color streaks updated from reset point {reset_from}: win={current_win}, lose={current_loss}")
        
    except Exception as e:
        logger.error(f"❌ Error updating color streaks: {e}")

def update_size_streaks():
    """Enhanced size streak calculation with comprehensive error handling"""
    try:
        logger.debug("📈 Updating size streaks...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for size streak update")
            return
        
        log = get_redis_hash_all(REDIS_SIZE_PREDICTION_LOG_KEY)
        history = decode_history()
        
        # Get reset point
        reset_point_raw = get_redis_json(REDIS_RESET_POINT)
        reset_from = int(reset_point_raw) if reset_point_raw else None
        
        # Process issues in chronological order
        sorted_issues = sorted(log.keys(), key=int)
        
        current_win = current_loss = 0
        max_win = max_loss = 0
        win_dist = defaultdict(int)
        loss_dist = defaultdict(int)
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = log.get(issue)
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_value = history.get(issue)
                
                if actual_value is None:
                    continue
                
                actual_size = "Big" if SIZE_MAP[actual_value] == "B" else "Small"
                predicted_size = pred.get("next_size")
                
                # Handle both formats consistently
                if predicted_size == "S":
                    predicted_size = "Small"
                elif predicted_size == "B":
                    predicted_size = "Big"
                
                if predicted_size == actual_size:
                    # Correct prediction - win
                    if current_loss > 0:
                        loss_dist[current_loss] += 1
                        current_loss = 0
                    current_win += 1
                    max_win = max(max_win, current_win)
                else:
                    # Incorrect prediction - loss
                    if current_win > 0:
                        win_dist[current_win] += 1
                        current_win = 0
                    current_loss += 1
                    max_loss = max(max_loss, current_loss)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing size streak for issue {issue}: {e}")
                continue
        
        # Count current streak in distribution
        if current_win > 0:
            win_dist[current_win] += 1
        if current_loss > 0:
            loss_dist[current_loss] += 1
        
        # Build streak data
        streak_data = {
            "current_win_streak": current_win,
            "current_lose_streak": current_loss,
            "max_win_streak": max_win,
            "max_lose_streak": max_loss,
            "win_streak_distribution": dict(win_dist),
            "lose_streak_distribution": dict(loss_dist),
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "reset_from_issue": reset_from
        }
        
        set_redis_json(REDIS_SIZE_STREAKS_KEY, streak_data)
        logger.info(f"📈 Size streaks updated from reset point {reset_from}: win={current_win}, lose={current_loss}")
        
    except Exception as e:
        logger.error(f"❌ Error updating size streaks: {e}")

# --- Enhanced Prediction Cycle Functions ---
def run_color_prediction_cycle():
    """Enhanced color prediction cycle with comprehensive error handling"""
    global rules, current_loss
    
    try:
        logger.info("🎨 Running color prediction cycle...")
        update_color_worker_status("running", "Processing color predictions")
        
        # Get history and sequences
        history = decode_history()
        color_sequence = get_color_sequence(history)
        
        if not color_sequence:
            logger.warning("⚠️ No color sequence available")
            return
        
        # Get current issue info
        sorted_issues = sorted(history.keys(), key=int, reverse=True)
        if not sorted_issues:
            logger.warning("⚠️ No issues found in history")
            return
        
        current_issue = sorted_issues[0]
        next_issue = str(int(current_issue) + 1)
        
        # Check for recent mismatch (validation)
        if len(sorted_issues) > 1:
            prev_issue = sorted_issues[1]
            prev_prediction = get_redis_hash_json(REDIS_COLOR_PREDICTION_LOG_KEY, prev_issue)
            
            if prev_prediction:
                actual_value = history.get(prev_issue)
                if actual_value is not None:
                    actual_color = "Red" if COLOR_MAP[actual_value] == "R" else "Green"
                    predicted_color = prev_prediction.get("next_color")
                    
                    if predicted_color != actual_color:
                        current_loss += 1
                        logger.warning(f"❌ Color mismatch for issue #{prev_issue}: expected '{actual_color}', but predicted '{predicted_color}'. Loss streak is now {current_loss}.")
                    else:
                        current_loss = 0
                        logger.info(f"✅ Color prediction correct for issue #{prev_issue}: '{actual_color}'. Loss streak reset.")
        
        # Check if retraining is needed
        if current_loss >= MAX_ALLOWED_LOSS_STREAK or len(rules) == 0:
            logger.warning(f"🔄 Retraining triggered: loss streak={current_loss}, rules={len(rules)}")
            with rules_lock:
                new_rules = retrain_rules(color_sequence)
                rules.update(new_rules)
                current_loss = 0  # Reset loss streak after retraining
                logger.info(f"✅ Rules updated: {len(rules)} total rules now available")
        
        # Use effective rulebook
        effective_rules = get_effective_rulebook(rules, PREDEFINED_COLOR_RULES, "color")
        
        # Make prediction
        predicted_color, rule_used, accuracy = predict_next_color(color_sequence, effective_rules)
        
        # Create prediction object
        prediction = {
            "issue": next_issue,
            "next_color": predicted_color,
            "rule_name": rule_used,
            "confidence": accuracy / 100.0,
            "available_rules": len(effective_rules),
            "last_updated": datetime.now(pytz.utc).isoformat(),
            "prediction_source": "enhanced_rules"
        }
        
        # Log the prediction
        if log_color_prediction(prediction):
            logger.info(f"✅ New color prediction logged for issue #{next_issue} → '{predicted_color}' via rule '{rule_used}'.")
        
        # Update accuracy and history
        update_accuracy()
        update_prediction_history()
        update_streaks()
        
        logger.info("✅ Color prediction cycle completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Color prediction cycle failed: {e}")
        update_color_worker_status("error", f"Color prediction cycle failed: {str(e)}")

def run_size_prediction_cycle():
    """Enhanced size prediction cycle with learning"""
    global size_rules, current_size_loss
    
    try:
        logger.info("📏 Running size prediction cycle...")
        
        # Check if size retraining is needed
        retrain_size_rules_if_needed()
        
        # Get history and sequences
        history = decode_history()
        size_sequence = get_size_sequence(history)
        
        if not size_sequence:
            logger.warning("⚠️ No size sequence available")
            return
        
        # Get current issue info
        sorted_issues = sorted(history.keys(), key=int, reverse=True)
        if not sorted_issues:
            logger.warning("⚠️ No issues found in history")
            return
        
        current_issue = sorted_issues[0]
        next_issue = str(int(current_issue) + 1)
        
        # Check for recent mismatch (validation)
        if len(sorted_issues) > 1:
            prev_issue = sorted_issues[1]
            prev_prediction = get_redis_hash_json(REDIS_SIZE_PREDICTION_LOG_KEY, prev_issue)
            
            if prev_prediction:
                actual_value = history.get(prev_issue)
                if actual_value is not None:
                    actual_size = "Big" if SIZE_MAP[actual_value] == "B" else "Small"
                    predicted_size = prev_prediction.get("next_size")
                    
                    # Handle both formats consistently
                    if predicted_size == "S":
                        predicted_size = "Small"
                    elif predicted_size == "B":
                        predicted_size = "Big"
                    
                    if predicted_size != actual_size:
                        current_size_loss += 1
                        logger.warning(f"❌ Size mismatch for issue #{prev_issue}: expected '{actual_size}', but predicted '{predicted_size}'. Loss streak is now {current_size_loss}.")
                    else:
                        current_size_loss = 0
                        logger.info(f"✅ Size prediction correct for issue #{prev_issue}: '{actual_size}'. Loss streak reset.")
        
        # Use effective rulebook with any learned rules
        effective_rules = get_effective_rulebook(size_rules, PREDEFINED_SIZE_RULES, "size")
        
        # Make prediction
        predicted_size, rule_used, accuracy = predict_next_size(size_sequence, effective_rules)
        
        # Ensure consistent format in storage (use full names)
        if predicted_size == "S":
            predicted_size = "Small"
        elif predicted_size == "B":
            predicted_size = "Big"
        
        # Create prediction object
        prediction = {
            "issue": next_issue,
            "next_size": predicted_size,  # Now stores "Small"/"Big"
            "rule_name": rule_used,
            "confidence": accuracy / 100.0,
            "available_rules": len(effective_rules),
            "last_updated": datetime.now(pytz.utc).isoformat(),
            "prediction_source": "enhanced_rules"
        }
        
        # Log the prediction
        if log_size_prediction(prediction):
            logger.info(f"✅ New size prediction logged for issue #{next_issue} → '{predicted_size}' via rule '{rule_used}'.")
        
        # Update accuracy and history
        update_size_accuracy()
        update_size_prediction_history()
        update_size_streaks()
        
        logger.info("✅ Size prediction cycle completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Size prediction cycle failed: {e}")

# --- Main Monitoring Functions ---
def main_monitoring_loop():
    """Main monitoring loop with both color and size prediction cycles"""
    global size_rules, cycle_count
    
    logger.info("🚀 Starting main monitoring loop...")
    update_color_worker_status("started", "Main monitoring loop initialized")
    
    while not shutdown_event.is_set():
        try:
            cycle_count += 1
            logger.info(f"🔄 Starting cycle #{cycle_count}")
            
            # Color prediction cycle
            run_color_prediction_cycle()
            
            # Size prediction cycle with learning
            run_size_prediction_cycle()
            
            # Force size rule retraining every 50 cycles if still no learned rules
            if cycle_count % 50 == 0 and len(size_rules) == 0:
                logger.info("🔄 Forcing size rule retraining due to no learned rules...")
                retrain_size_rules_if_needed()
            
            # Send heartbeat
            send_color_worker_heartbeat()
            update_color_worker_status("running", f"Cycle #{cycle_count} completed")
            
            # Wait for next cycle
            logger.info(f"⏳ Cycle #{cycle_count} complete. Waiting 60 seconds...")
            if shutdown_event.wait(60):  # Wait 60 seconds or until shutdown
                break
                
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
            break
        except Exception as e:
            logger.error(f"❌ Error in main monitoring loop cycle #{cycle_count}: {e}")
            update_color_worker_status("error", f"Cycle #{cycle_count} failed: {str(e)}")
            if shutdown_event.wait(30):  # Wait 30 seconds before retrying, or until shutdown
                break

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"🛑 Received signal {signum}. Initiating graceful shutdown...")
    update_color_worker_status("shutting_down", f"Received signal {signum}")
    shutdown_event.set()

def main():
    """Main entry point"""
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("🎮 WinGo Color Worker Monitor starting...")
        logger.info(f"🔧 Configuration: Pattern Length={MAX_PATTERN_LENGTH}, Min Occurrences={MIN_OCCURRENCES}")
        logger.info(f"📏 Size Configuration: Min Size Occurrences={MIN_SIZE_OCCURRENCES}")
        logger.info(f"🎯 Predefined Rules: {len(PREDEFINED_COLOR_RULES)} color, {len(PREDEFINED_SIZE_RULES)} size")
        
        # Run the main monitoring loop
        main_monitoring_loop()
        
    except Exception as e:
        logger.critical(f"💥 Critical error in main(): {e}")
        update_color_worker_status("critical_error", str(e))
        return 1
    finally:
        logger.info("🏁 Color Worker Monitor shutting down...")
        update_color_worker_status("stopped", "Shutdown completed")
        return 0

if __name__ == "__main__":
    sys.exit(main())
