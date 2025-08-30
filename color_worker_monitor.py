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

def graceful_shutdown():
    """Cleanup resources before exit"""
    try:
        if redis_client:
            logger.info("🧹 Closing Redis connection...")
            redis_client.close()
        logger.info("✅ Graceful shutdown completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

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

# Initialize Redis connection with graceful shutdown
if not initialize_redis():
    logger.critical("❌ Could not initialize Redis connection. Exiting.")
    graceful_shutdown()
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

# ✅ NEW: Redis Keys for Optimized Streak Calculation
REDIS_COLOR_CURRENT_RESET_ISSUE = "lottery:color_current_reset_issue"
REDIS_SIZE_CURRENT_RESET_ISSUE = "lottery:size_current_reset_issue"
REDIS_COLOR_CACHED_STREAKS = "lottery:color_cached_streaks"
REDIS_SIZE_CACHED_STREAKS = "lottery:size_cached_streaks"

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

# ✅ FIXED: Fully Implemented Optimized Streak Calculation Class
class OptimizedStreakCalculator:
    """
    Optimized streak calculator that only recalculates when reset issue changes.
    Uses Redis to persist streak data between calculations.
    """
    def __init__(self, redis_client, calculation_type="color"):
        self.redis_client = redis_client
        self.calculation_type = calculation_type
        self.streaks_lock = Lock()
        
        # Redis keys based on calculation type
        if calculation_type == "color":
            self.reset_key = REDIS_COLOR_CURRENT_RESET_ISSUE
            self.streaks_key = REDIS_COLOR_CACHED_STREAKS
        else:  # size
            self.reset_key = REDIS_SIZE_CURRENT_RESET_ISSUE
            self.streaks_key = REDIS_SIZE_CACHED_STREAKS
    
    def get_or_calculate_streaks(self, reset_issue_no, data_for_calculation=None):
        """
        Get streaks - either from cache (if same reset) or recalculate (if new reset).
        This is the main optimization: O(1) most times, O(n) only on reset change.
        """
        with self.streaks_lock:
            try:
                # Get current stored reset issue
                stored_reset = get_redis_json(self.reset_key)
                current_reset = str(reset_issue_no)
                
                if stored_reset != current_reset:
                    # ✅ NEW RESET DETECTED: Full recalculation needed
                    logger.info(f"🔄 {self.calculation_type.upper()} streak reset changed from {stored_reset} to {current_reset}")
                    logger.info(f"⚡ Performing full {self.calculation_type} streak recalculation...")
                    
                    # Perform heavy computation
                    new_streaks = self._calculate_full_streaks(reset_issue_no, data_for_calculation)
                    
                    # Save new reset issue and calculated streaks
                    set_redis_json(self.reset_key, current_reset)
                    set_redis_json(self.streaks_key, new_streaks)
                    
                    logger.info(f"✅ {self.calculation_type.upper()} streaks fully recalculated and cached")
                    return new_streaks
                
                else:
                    # ✅ SAME RESET: Use cached streaks with incremental update
                    logger.debug(f"⚡ Using cached {self.calculation_type} streaks (reset: {current_reset})")
                    
                    cached_streaks = get_redis_json(self.streaks_key, {})
                    if cached_streaks:
                        # Optionally update incrementally if new data provided
                        if data_for_calculation:
                            updated_streaks = self._update_streaks_incrementally(cached_streaks, data_for_calculation)
                            set_redis_json(self.streaks_key, updated_streaks)
                            logger.debug(f"📈 {self.calculation_type.upper()} streaks updated incrementally")
                            return updated_streaks
                        return cached_streaks
                    else:
                        # Fallback: no cached data, recalculate
                        logger.warning(f"⚠️ No cached {self.calculation_type} streaks found, recalculating...")
                        new_streaks = self._calculate_full_streaks(reset_issue_no, data_for_calculation)
                        set_redis_json(self.streaks_key, new_streaks)
                        return new_streaks
                        
            except Exception as e:
                logger.error(f"❌ Error in optimized {self.calculation_type} streak calculation: {e}")
                # Fallback to full calculation
                return self._calculate_full_streaks(reset_issue_no, data_for_calculation)
    
    def _calculate_full_streaks(self, reset_issue_no, data):
        """
        ✅ FIXED: Heavy computation - Calculate streaks from scratch starting from reset issue.
        This is only called when reset issue changes.
        """
        logger.info(f"🔄 Full {self.calculation_type} streak calculation starting from issue {reset_issue_no}")
        
        try:
            if not data or 'prediction_log' not in data or 'history' not in data:
                logger.warning(f"⚠️ Insufficient data for {self.calculation_type} streak calculation")
                return self._get_empty_streaks_structure(reset_issue_no)
            
            prediction_log = data['prediction_log']
            history = data['history']
            
            # Process issues in chronological order starting from reset point
            sorted_issues = sorted(prediction_log.keys(), key=int)
            
            win = lose = max_win = max_lose = 0
            wd = defaultdict(int)  # win distribution
            ld = defaultdict(int)  # loss distribution
            total_processed = 0
            
            for issue in sorted_issues:
                try:
                    # Skip issues before reset point
                    if int(issue) < reset_issue_no:
                        continue
                    
                    pred_raw = prediction_log.get(issue)
                    if not pred_raw:
                        continue
                    
                    pred = json.loads(pred_raw)
                    actual_val = history.get(issue)
                    
                    if actual_val is None:
                        continue
                    
                    # Determine if prediction was correct based on type
                    if self.calculation_type == "color":
                        actual_result = "Red" if COLOR_MAP[actual_val] == 'R' else "Green"
                        predicted = pred.get("next_color")
                    else:  # size
                        actual_result = "Big" if SIZE_MAP[actual_val] == 'B' else "Small"
                        predicted = pred.get("next_size")
                    
                    total_processed += 1
                    
                    if actual_result == predicted:
                        # Correct prediction
                        win += 1
                        if lose > 0:
                            ld[lose] += 1
                            lose = 0
                        max_win = max(max_win, win)
                    else:
                        # Incorrect prediction
                        lose += 1
                        if win > 0:
                            wd[win] += 1
                            win = 0
                        max_lose = max(max_lose, lose)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error processing {self.calculation_type} streak for issue {issue}: {e}")
                    continue
            
            # Handle final streak
            current_win = win if win > 0 else 0
            current_lose = lose if lose > 0 else 0
            
            if win > 0:
                wd[win] += 1
            elif lose > 0:
                ld[lose] += 1
            
            result = {
                'current_win_streak': current_win,
                'current_lose_streak': current_lose,
                'max_win_streak': max_win,
                'max_lose_streak': max_lose,
                'win_streak_distribution': dict(wd),
                'lose_streak_distribution': dict(ld),
                'reset_issue': reset_issue_no,
                'calculation_timestamp': datetime.now(pytz.utc).isoformat(),
                'total_processed': total_processed
            }
            
            logger.info(f"📊 {self.calculation_type.upper()} streak calculation complete: "
                       f"processed {total_processed} issues, "
                       f"current win={current_win}, lose={current_lose}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in full {self.calculation_type} streak calculation: {e}")
            return self._get_empty_streaks_structure(reset_issue_no, error=str(e))
    
    def _update_streaks_incrementally(self, cached_streaks, new_data):
        """
        ✅ FIXED: Light computation - Update existing streaks with new data.
        This is called for every update when reset issue hasn't changed.
        """
        logger.debug(f"📈 Incremental {self.calculation_type} streak update")
        
        try:
            # Make a copy to avoid modifying original
            updated_streaks = cached_streaks.copy()
            
            if not new_data or 'prediction_log' not in new_data or 'history' not in new_data:
                updated_streaks['last_update'] = datetime.now(pytz.utc).isoformat()
                return updated_streaks
            
            prediction_log = new_data['prediction_log']
            history = new_data['history']
            
            # Find the latest processed issue from cache
            last_processed = updated_streaks.get('last_processed_issue', 0)
            
            # Process only new issues since last update
            sorted_issues = sorted([k for k in prediction_log.keys() if int(k) > last_processed], key=int)
            
            if not sorted_issues:
                updated_streaks['last_update'] = datetime.now(pytz.utc).isoformat()
                return updated_streaks
            
            current_win = updated_streaks.get('current_win_streak', 0)
            current_lose = updated_streaks.get('current_lose_streak', 0)
            max_win = updated_streaks.get('max_win_streak', 0)
            max_lose = updated_streaks.get('max_lose_streak', 0)
            wd = defaultdict(int, updated_streaks.get('win_streak_distribution', {}))
            ld = defaultdict(int, updated_streaks.get('lose_streak_distribution', {}))
            
            for issue in sorted_issues:
                try:
                    pred_raw = prediction_log.get(issue)
                    if not pred_raw:
                        continue
                    
                    pred = json.loads(pred_raw)
                    actual_val = history.get(issue)
                    
                    if actual_val is None:
                        continue
                    
                    # Determine if prediction was correct
                    if self.calculation_type == "color":
                        actual_result = "Red" if COLOR_MAP[actual_val] == 'R' else "Green"
                        predicted = pred.get("next_color")
                    else:  # size
                        actual_result = "Big" if SIZE_MAP[actual_val] == 'B' else "Small"
                        predicted = pred.get("next_size")
                    
                    if actual_result == predicted:
                        # Correct prediction
                        if current_lose > 0:
                            ld[current_lose] += 1
                            current_lose = 0
                        current_win += 1
                        max_win = max(max_win, current_win)
                    else:
                        # Incorrect prediction
                        if current_win > 0:
                            wd[current_win] += 1
                            current_win = 0
                        current_lose += 1
                        max_lose = max(max_lose, current_lose)
                    
                    # Update last processed issue
                    updated_streaks['last_processed_issue'] = int(issue)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error in incremental update for issue {issue}: {e}")
                    continue
            
            # Update streak data
            updated_streaks.update({
                'current_win_streak': current_win,
                'current_lose_streak': current_lose,
                'max_win_streak': max_win,
                'max_lose_streak': max_lose,
                'win_streak_distribution': dict(wd),
                'lose_streak_distribution': dict(ld),
                'last_update': datetime.now(pytz.utc).isoformat(),
                'total_processed': updated_streaks.get('total_processed', 0) + len(sorted_issues)
            })
            
            return updated_streaks
            
        except Exception as e:
            logger.error(f"❌ Error in incremental {self.calculation_type} streak update: {e}")
            return cached_streaks  # Return original on error
    
    def _get_empty_streaks_structure(self, reset_issue_no, error=None):
        """Helper method to return empty streak structure"""
        structure = {
            'win_streak_distribution': {},
            'lose_streak_distribution': {},
            'current_win_streak': 0,
            'current_lose_streak': 0,
            'max_win_streak': 0,
            'max_lose_streak': 0,
            'reset_issue': reset_issue_no,
            'calculation_timestamp': datetime.now(pytz.utc).isoformat(),
            'total_processed': 0
        }
        if error:
            structure['error'] = error
        return structure

# ✅ NEW: Global instances for optimized streak calculation
color_streak_calculator = None
size_streak_calculator = None

def initialize_streak_calculators():
    """Initialize optimized streak calculators"""
    global color_streak_calculator, size_streak_calculator
    
    try:
        color_streak_calculator = OptimizedStreakCalculator(redis_client, "color")
        size_streak_calculator = OptimizedStreakCalculator(redis_client, "size")
        logger.info("✅ Optimized streak calculators initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize streak calculators: {e}")

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
MIN_OCCURRENCES = 8
MIN_SIZE_OCCURRENCES = 8
MAX_ALLOWED_LOSS_STREAK = 1
TRAINING_WINDOW_SIZE = 10000
EMERGENCY_LOSS_STREAK = 7

# ✅ Issue Numbering Fix Function
def calculate_next_issue_number(current_issue_key, max_issues_per_day=1440):
    """
    Calculate next issue with proper day transition handling.
    
    Args:
        current_issue_key (str): Current full issue key (e.g., '20250829100011440')
        max_issues_per_day (int): Maximum issues per day (default: 1440)
    
    Returns:
        str: Next issue key with correct date and issue number
    """
    from datetime import datetime, timedelta
    
    try:
        # Extract parts: YYYYMMDD + 10001 + XXXX
        date_part = current_issue_key[:8]  # YYYYMMDD
        middle_part = current_issue_key[8:13]  # 10001
        issue_num = int(current_issue_key[13:])  # Last 4 digits
        
        # If current issue is 1440 (last of day), move to next day's 0001
        if issue_num >= max_issues_per_day:
            # Parse current date and add 1 day
            current_date = datetime.strptime(date_part, "%Y%m%d").date()
            next_date = current_date + timedelta(days=1)
            next_date_str = next_date.strftime("%Y%m%d")
            
            # Build next issue key for next day starting at 0001
            next_issue_key = f"{next_date_str}{middle_part}0001"
            logger.info(f"📅 Day transition: {current_issue_key} → {next_issue_key}")
            return next_issue_key
        else:
            # Same day, just increment issue number
            next_issue_num = issue_num + 1
            next_issue_key = f"{date_part}{middle_part}{str(next_issue_num).zfill(4)}"
            return next_issue_key
            
    except (ValueError, IndexError) as e:
        logger.error(f"Error calculating next issue from {current_issue_key}: {e}")
        # Fallback to basic increment (this should not happen with valid data)
        return str(int(current_issue_key) + 1)

# ✅ FIXED: Issue Key Validation Function
def validate_issue_key(issue_key):
    """
    Validate that issue key follows correct format and numbering.
    
    Args:
        issue_key (str): Issue key to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        if len(issue_key) < 17:
            return False
        
        date_part = issue_key[:8]
        issue_num = int(issue_key[13:])
        
        # Check date format
        datetime.strptime(date_part, "%Y%m%d")
        
        # Check issue number range
        if issue_num < 1 or issue_num > 1440:
            logger.warning(f"⚠️ Invalid issue number {issue_num} in key {issue_key}")
            return False
        
        return True
        
    except (ValueError, IndexError):
        logger.warning(f"⚠️ Invalid issue key format: {issue_key}")
        return False

# --- 25 Red-Green Pattern Rules ---
RED_GREEN_PATTERNS = {
    "Rule 1": "ABABABABAB",
    "Rule 2": "AABBAABB",
    "Rule 3": "AAABBBAAABBB",
    "Rule 4": "AAAABBBBAAAABBBB",
    "Rule 5": "AABAABAAB",
    "Rule 6": "AAAAAAAABBBBBBBB",
    "Rule 7": "ABBABBABB",
    "Rule 8": "AAABAAABAAAB",
    "Rule 9": "AAABBAAABB",
    "Rule 10": "AAAABBABBBAAAA",
    "Rule 11": "ABBBAABBBAABBB",
    "Rule 12": "ABABBBABBBB",
    "Rule 13": "AABBAAABBBAAAABBBB",
    "Rule 14": "ABBAAABBBB",
    "Rule 15": "AAAABBBAAB",
    "Rule 16": "ABAABBAAABBB",
    "Rule 17": "AABBBAABBBAA",
    "Rule 18": "ABBAAAABBBBBBBB",
    "Rule 19": "ABBBAABBB",
    "Rule 20": "AABBBAABBB",
    "Rule 21": "ABAABAAAB",
    "Rule 22": "AABAABBAABBB",
    "Rule 23": "AAAABAAAAB",
    "Rule 24": "AAAABBAAAABB",
    "Rule 25": "AAAABBBAAAABBB"
}

# --- 25 Small-Big Pattern Rules (for size) ---
# Same patterns, but for Small and Big
SMALL_BIG_PATTERNS = {
    "Rule 1": "ABABABABAB",
    "Rule 2": "AABBAABB",
    "Rule 3": "AAABBBAAABBB",
    "Rule 4": "AAAABBBBAAAABBBB",
    "Rule 5": "AABAABAAB",
    "Rule 6": "AAAAAAAABBBBBBBB",
    "Rule 7": "ABBABBABB",
    "Rule 8": "AAABAAABAAAB",
    "Rule 9": "AAABBAAABB",
    "Rule 10": "AAAABBABBBAAAA",
    "Rule 11": "ABBBAABBBAABBB",
    "Rule 12": "ABABBBABBBB",
    "Rule 13": "AABBAAABBBAAAABBBB",
    "Rule 14": "ABBAAABBBB",
    "Rule 15": "AAAABBBAAB",
    "Rule 16": "ABAABBAAABBB",
    "Rule 17": "AABBBAABBBAA",
    "Rule 18": "ABBAAAABBBBBBBB",
    "Rule 19": "ABBBAABBB",
    "Rule 20": "AABBBAABBB",
    "Rule 21": "ABAABAAAB",
    "Rule 22": "AABAABBAABBB",
    "Rule 23": "AAAABAAAAB",
    "Rule 24": "AAAABBAAAABB",
    "Rule 25": "AAAABBBAAAABBB"
}

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
    #"SSSSSS": {"predict": "B", "correct": 79, "total": 100, "accuracy": 79.0},
    #"BBBBBB": {"predict": "S", "correct": 77, "total": 100, "accuracy": 77.0},
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

# --- Pattern Matching Logic for 25 Rules ---
def ab_to_colors(pattern, a_type, b_type):
    """Converts a pattern of 'A'/'B' into a color/size sequence"""
    return [a_type if c == 'A' else b_type for c in pattern]

def score_rule_match(sequence, rule_pattern):
    """Scores a match between a sequence and a rule pattern"""
    max_match_len = min(len(sequence), len(rule_pattern))
    score = 0
    for i in range(max_match_len):
        if sequence[-max_match_len + i] == rule_pattern[i]:
            score += 1
    return score, max_match_len

# ✅ FIXED: Pattern Matching Function
def infer_next_from_patterns(current_sequence, patterns, type_a, type_b):
    """Infers the next element based on a set of A/B patterns"""
    best_score = -1
    best_rule = None
    best_next_type = None
    best_match_len = 0  # ✅ Fixed: Track best match length

    for rule_name, ab_pattern in patterns.items():
        for a_type, b_type in [(type_a, type_b), (type_b, type_a)]:
            type_pattern = ab_to_colors(ab_pattern, a_type, b_type)
            for offset in range(len(type_pattern)):
                # Ensure a minimum match length of 2
                slice_pattern = type_pattern[offset:offset + len(current_sequence)]
                if len(slice_pattern) < 2:
                    continue

                score, match_len = score_rule_match(current_sequence, slice_pattern)
                
                # Use strict score comparison
                if score > best_score:
                    try:
                        next_char = type_pattern[offset + len(current_sequence)]
                        best_next_type = next_char
                        best_score = score
                        best_rule = f"{rule_name} (A={a_type}, B={b_type})"
                        best_match_len = match_len  # ✅ Fixed: Update best match length
                    except IndexError:
                        continue
                # ✅ FIXED: If scores are equal, prioritize longer match length
                elif score == best_score and match_len > best_match_len:
                     try:
                        next_char = type_pattern[offset + len(current_sequence)]
                        best_next_type = next_char
                        best_score = score
                        best_rule = f"{rule_name} (A={a_type}, B={b_type})"
                        best_match_len = match_len  # ✅ Fixed: Update best match length
                     except IndexError:
                        continue
    
    # Return prediction and a confidence score based on the match score
    if best_score > -1:
        # Simple confidence calculation based on best score
        confidence = (best_score / len(current_sequence)) if len(current_sequence) > 0 else 0.5
        return best_next_type, best_rule, confidence
        
    return None, None, 0.0

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

def generate_rules(stats, min_occurrences=MIN_OCCURRENCES):
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
                if total >= min_occurrences:
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
        logger.info(f"🧠 Retraining color rules with {len(colors)} entries...")
        update_color_worker_status("retraining", f"Retraining color with {len(colors)} entries")
        
        if not colors:
            logger.error("❌ No colors provided for retraining")
            return {}
        
        stats = learn_patterns(colors)
        rules = generate_rules(stats)
        
        logger.info(f"✅ Color retraining complete: {len(rules)} rules generated")
        return rules
        
    except Exception as e:
        logger.error(f"❌ Error during color rule retraining: {e}")
        return {}

def retrain_size_rules(size_sequence):
    """Retrain size pattern rules from history sequence"""
    try:
        logger.info(f"🧠 Retraining SIZE rules with {len(size_sequence)} entries...")
        update_color_worker_status("size_retraining", f"Retraining size with {len(size_sequence)} entries")
        
        if not size_sequence or len(size_sequence) < MAX_PATTERN_LENGTH + 1:
            logger.warning("⚠️ Not enough size sequence data to retrain.")
            return {}
        
        stats = learn_patterns(size_sequence)
        rules = generate_rules(stats, min_occurrences=MIN_SIZE_OCCURRENCES)
        
        logger.info(f"✅ Size retraining complete: {len(rules)} rules generated")
        return rules
        
    except Exception as e:
        logger.error(f"❌ Error during size rule retraining: {e}")
        return {}

def cleanup_corrupted_prediction_logs():
    """Clean up corrupted prediction log data"""
    try:
        logger.info("🧹 Cleaning up corrupted prediction logs...")
        
        # Get all corrupted entries
        color_log = get_redis_hash_all(REDIS_COLOR_PREDICTION_LOG_KEY)
        size_log = get_redis_hash_all(REDIS_SIZE_PREDICTION_LOG_KEY)
        
        # Clean color log
        cleaned_color = 0
        for key, value in color_log.items():
            if value is None or value == "null" or not key.isdigit():
                redis_client.hdel(REDIS_COLOR_PREDICTION_LOG_KEY, key)
                cleaned_color += 1
                logger.debug(f"🗑️ Removed corrupted color entry: {key}")
        
        # Clean size log
        cleaned_size = 0
        for key, value in size_log.items():
            if value is None or value == "null" or not key.isdigit():
                redis_client.hdel(REDIS_SIZE_PREDICTION_LOG_KEY, key)
                cleaned_size += 1
                logger.debug(f"🗑️ Removed corrupted size entry: {key}")
        
        logger.info(f"✅ Cleanup complete: {cleaned_color} color entries, {cleaned_size} size entries removed")
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}")

# ✅ Initialize streak calculators after Redis is connected
initialize_streak_calculators()
# --- Enhanced Prediction Functions with Fallback ---
def predict_next_color(seq, rulebook):
    """Enhanced color prediction with multiple fallback strategies"""
    try:
        # ✅ ADDED: Input validation
        if not seq or not isinstance(seq, list):
            logger.warning("⚠️ Invalid or empty sequence for color prediction")
            return get_time_based_color_prediction()
        
        # ✅ ADDED: Validate sequence content
        try:
            color_seq = []
            for item in seq:
                if isinstance(item, tuple) and len(item) == 2:
                    color_seq.append(item[1])
                else:
                    logger.warning(f"⚠️ Invalid sequence item format: {item}")
                    continue
        except Exception as e:
            logger.error(f"❌ Error processing sequence: {e}")
            return get_time_based_color_prediction()
        
        if not color_seq:
            logger.warning("⚠️ No valid color data after processing sequence")
            return get_time_based_color_prediction()
        
        # Use effective rulebook (learned + predefined)
        effective_rules = get_effective_rulebook(rulebook, PREDEFINED_COLOR_RULES, "color")
        
        # First, try to find a match using the 25 predefined patterns
        if len(color_seq) >= 2:
            try:
                next_color, rule_name, confidence = infer_next_from_patterns(color_seq, RED_GREEN_PATTERNS, 'R', 'G')
                if next_color and confidence > 0:
                    accuracy_pct = round(confidence * 100, 2)
                    logger.info(f"🎯 Color prediction using 25-Rules pattern '{rule_name}': {next_color} ({accuracy_pct}%)")
                    return next_color, rule_name, accuracy_pct
            except Exception as e:
                logger.error(f"❌ Error during 25-Rules pattern inference: {e}")

        # If no pattern matches from the 25 rules, fall back to learned/predefined rules
        for l in range(min(MAX_PATTERN_LENGTH, len(color_seq)), 1, -1):
            try:
                sub = ''.join(color_seq[-l:])
                if sub and sub in effective_rules:
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
        recent_seq = color_seq[-5:] if len(color_seq) >= 5 else color_seq
        
        # Try trend-based prediction first
        if len(recent_seq) >= 3:
            try:
                trend_pred, trend_rule, trend_acc = get_trend_color_prediction(recent_seq)
                if trend_acc > 55.0:  # Only use if reasonably confident
                    logger.info(f"📊 Using trend-based color prediction: {trend_pred} via {trend_rule} ({trend_acc}%)")
                    return trend_pred, trend_rule, trend_acc
            except Exception as e:
                logger.warning(f"⚠️ Error in trend prediction: {e}")
        
        # Try Fibonacci prediction
        try:
            fib_pred, fib_rule, fib_acc = get_fibonacci_color_prediction(recent_seq)
            logger.info(f"🌀 Using Fibonacci color prediction: {fib_pred} via {fib_rule} ({fib_acc}%)")
            return fib_pred, fib_rule, fib_acc
        except Exception as e:
            logger.warning(f"⚠️ Error in Fibonacci prediction: {e}")
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in color prediction: {e}", exc_info=True)
    
    # Final fallback
    logger.info("🕐 Using time-based fallback for color prediction")
    return get_time_based_color_prediction()

def predict_next_size(seq, rulebook):
    """Enhanced size prediction with multiple fallback strategies"""
    try:
        # ✅ ADDED: Input validation
        if not seq or not isinstance(seq, list):
            logger.warning("⚠️ Invalid or empty sequence for size prediction")
            return get_time_based_size_prediction()
        
        # ✅ ADDED: Validate sequence content
        try:
            size_seq = []
            for item in seq:
                if isinstance(item, tuple) and len(item) == 2:
                    size_seq.append(item[1])
                else:
                    logger.warning(f"⚠️ Invalid sequence item format: {item}")
                    continue
        except Exception as e:
            logger.error(f"❌ Error processing sequence: {e}")
            return get_time_based_size_prediction()
        
        if not size_seq:
            logger.warning("⚠️ No valid size data after processing sequence")
            return get_time_based_size_prediction()
        
        # Use effective rulebook (learned + predefined)
        effective_rules = get_effective_rulebook(rulebook, PREDEFINED_SIZE_RULES, "size")

        # First, try to find a match using the 25 predefined patterns for size
        if len(size_seq) >= 2:
            try:
                next_size, rule_name, confidence = infer_next_from_patterns(size_seq, SMALL_BIG_PATTERNS, 'S', 'B')
                if next_size and confidence > 0:
                    accuracy_pct = round(confidence * 100, 2)
                    logger.info(f"🎯 Size prediction using 25-Rules pattern '{rule_name}': {next_size} ({accuracy_pct}%)")
                    return next_size, rule_name, accuracy_pct
            except Exception as e:
                logger.error(f"❌ Error during 25-Rules size pattern inference: {e}")
        
        # Try patterns from longest to shortest
        for l in range(min(MAX_PATTERN_LENGTH, len(size_seq)), 1, -1):
            try:
                sub = ''.join(size_seq[-l:])
                if sub and sub in effective_rules:
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
        recent_seq = size_seq[-5:] if len(size_seq) >= 5 else size_seq
        
        # Try trend-based prediction first
        if len(recent_seq) >= 3:
            try:
                trend_pred, trend_rule, trend_acc = get_trend_size_prediction(recent_seq)
                if trend_acc > 55.0:  # Only use if reasonably confident
                    logger.info(f"📊 Using trend-based size prediction: {trend_pred} via {trend_rule} ({trend_acc}%)")
                    return trend_pred, trend_rule, trend_acc
            except Exception as e:
                logger.warning(f"⚠️ Error in trend prediction: {e}")
        
        # Try Fibonacci prediction
        try:
            fib_pred, fib_rule, fib_acc = get_fibonacci_size_prediction(recent_seq)
            logger.info(f"🌀 Using Fibonacci size prediction: {fib_pred} via {fib_rule} ({fib_acc}%)")
            return fib_pred, fib_rule, fib_acc
        except Exception as e:
            logger.warning(f"⚠️ Error in Fibonacci prediction: {e}")
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in size prediction: {e}", exc_info=True)
    
    # Final fallback
    logger.info("🕐 Using time-based fallback for size prediction")
    return get_time_based_size_prediction()

# --- Enhanced Logging Functions ---
def log_color_prediction(pred):
    """Fixed color prediction logging with robust error handling"""
    try:
        # ✅ Input validation
        if not pred or not isinstance(pred, dict):
            logger.error("❌ Invalid prediction data for color logging")
            return False
            
        if not pred.get("issue"):
            logger.error("❌ Missing issue field in color prediction")
            return False

        # ✅ Ensure all required fields exist
        prediction_data = {
            "issue": str(pred["issue"]),
            "next_color": str(pred.get("next_color", "Red")),
            "rule_name": str(pred.get("rule_name", "Unknown")),
            "confidence": float(pred.get("confidence", 0.5)),
            "score": float(pred.get("score", 50.0)),
            "observed_sequence": str(pred.get("observed_sequence", "N/A")),
            "last_updated": pred.get("last_updated", datetime.now(pytz.utc).isoformat()),
            "prediction_source": str(pred.get("prediction_source", "system")),
            "available_rules": int(pred.get("available_rules", 0)),
            "sequence_length": int(pred.get("sequence_length", 0)),
            "worker_version": str(pred.get("worker_version", "v3.0"))
        }

        # ✅ Validate data before JSON serialization
        try:
            json_test = json.dumps(prediction_data)
            logger.debug(f"✅ JSON validation passed for issue {prediction_data['issue']}")
        except (TypeError, ValueError) as e:
            logger.error(f"❌ JSON serialization failed: {e}")
            logger.error(f"❌ Problematic data: {prediction_data}")
            return False

        # ✅ Store with proper error handling
        with redis_lock:
            try:
                # Store current prediction
                redis_client.set(REDIS_COLOR_PREDICTION_KEY, json.dumps(prediction_data))
                
                # Store in prediction log
                redis_client.hset(
                    REDIS_COLOR_PREDICTION_LOG_KEY, 
                    prediction_data["issue"], 
                    json.dumps(prediction_data)
                )
                
                logger.info(f"✅ Color prediction logged successfully for issue {prediction_data['issue']}")
                return True
                
            except redis.RedisError as e:
                logger.error(f"❌ Redis error during color logging: {e}")
                return False

    except Exception as e:
        logger.error(f"❌ Unexpected error in log_color_prediction: {e}")
        logger.exception("Full traceback:")
        return False

def log_size_prediction(pred):
    """Fixed size prediction logging with robust error handling"""
    try:
        # ✅ Input validation
        if not pred or not isinstance(pred, dict):
            logger.error("❌ Invalid prediction data for size logging")
            return False
            
        if not pred.get("issue"):
            logger.error("❌ Missing issue field in size prediction")
            return False

        # ✅ Ensure all required fields exist
        prediction_data = {
            "issue": str(pred["issue"]),
            "next_size": str(pred.get("next_size", "Small")),
            "rule_name": str(pred.get("rule_name", "Unknown")),
            "confidence": float(pred.get("confidence", 0.5)),
            "score": float(pred.get("score", 50.0)),
            "observed_sequence": str(pred.get("observed_sequence", "N/A")),
            "last_updated": pred.get("last_updated", datetime.now(pytz.utc).isoformat()),
            "prediction_source": str(pred.get("prediction_source", "system")),
            "available_rules": int(pred.get("available_rules", 0)),
            "sequence_length": int(pred.get("sequence_length", 0)),
            "worker_version": str(pred.get("worker_version", "v3.0"))
        }

        # ✅ Validate data before JSON serialization
        try:
            json_test = json.dumps(prediction_data)
            logger.debug(f"✅ JSON validation passed for issue {prediction_data['issue']}")
        except (TypeError, ValueError) as e:
            logger.error(f"❌ JSON serialization failed: {e}")
            logger.error(f"❌ Problematic data: {prediction_data}")
            return False

        # ✅ Store with proper error handling
        with redis_lock:
            try:
                # Store current prediction
                redis_client.set(REDIS_SIZE_PREDICTION_KEY, json.dumps(prediction_data))
                
                # Store in prediction log
                redis_client.hset(
                    REDIS_SIZE_PREDICTION_LOG_KEY, 
                    prediction_data["issue"], 
                    json.dumps(prediction_data)
                )
                
                logger.info(f"✅ Size prediction logged successfully for issue {prediction_data['issue']}")
                return True
                
            except redis.RedisError as e:
                logger.error(f"❌ Redis error during size logging: {e}")
                return False

    except Exception as e:
        logger.error(f"❌ Unexpected error in log_size_prediction: {e}")
        logger.exception("Full traceback:")
        return False


# --- Enhanced History Update Functions ---
def update_prediction_history():
    """Enhanced prediction history update with error handling"""
    try:
        if not redis_client:
            logger.error("❌ Redis client not available for history update")
            return
        
        # ✅ IMPROVED: Batch Redis operations
        with redis_lock:
            pipe = redis_client.pipeline()
            pipe.hgetall(REDIS_COLOR_PREDICTION_LOG_KEY)
            pipe.get(REDIS_HISTORY_KEY)
            results = pipe.execute()
        
        log_data = results[0] if results[0] else {}
        history_raw = results[1]
        
        # Decode history
        try:
            history = json.loads(history_raw) if history_raw else {}
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error decoding history: {e}")
            history = {}
        
        if not log_data:
            logger.debug("📭 No prediction log found for history update")
            return
        
        # ✅ IMPROVED: Process with better error handling
        history_table = []
        processed_count = 0
        
        # Process issues in descending order
        try:
            issues = sorted([k for k in log_data.keys() if k.isdigit()], key=int, reverse=True)
        except Exception as e:
            logger.error(f"❌ Error sorting issues: {e}")
            return
        
        for i in issues[:15]:  # Limit to last 15
            try:
                pred_raw = safe_decode(log_data.get(i))
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_num = history.get(i)
                
                actual_color = None
                if actual_num is not None and isinstance(actual_num, int) and 0 <= actual_num <= 9:
                    actual_color = "Red" if COLOR_MAP.get(actual_num) == "R" else "Green"
                
                history_table.append({
                    "issue": i,
                    "predicted_color": pred.get("next_color"),
                    "actual_color": actual_color,
                    "prediction_time": pred.get("last_updated"),
                    "rule_name": pred.get("rule_name"),
                    "accuracy": pred.get("score", 0)
                })
                processed_count += 1
                
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for issue {i}: {e}")
                continue
            except Exception as e:
                logger.warning(f"⚠️ Error processing history for issue {i}: {e}")
                continue
        
        set_redis_json(REDIS_COLOR_PREDICTION_HISTORY_KEY, history_table)
        logger.debug(f"📋 Updated color prediction history with {processed_count} entries")
        
    except Exception as e:
        logger.error(f"❌ Error updating prediction history: {e}", exc_info=True)

def update_size_prediction_history():
    """Enhanced size prediction history update with error handling"""
    try:
        if not redis_client:
            logger.error("❌ Redis client not available for size history update")
            return
        
        # ✅ IMPROVED: Batch Redis operations
        with redis_lock:
            pipe = redis_client.pipeline()
            pipe.hgetall(REDIS_SIZE_PREDICTION_LOG_KEY)
            pipe.get(REDIS_HISTORY_KEY)
            results = pipe.execute()
        
        log_data = results[0] if results[0] else {}
        history_raw = results[1]
        
        # Decode history
        try:
            history = json.loads(history_raw) if history_raw else {}
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error decoding history: {e}")
            history = {}
        
        if not log_data:
            logger.debug("📭 No size prediction log found for history update")
            return
        
        # ✅ IMPROVED: Process with better error handling
        history_table = []
        processed_count = 0
        
        # Process issues in descending order
        try:
            issues = sorted([k for k in log_data.keys() if k.isdigit()], key=int, reverse=True)
        except Exception as e:
            logger.error(f"❌ Error sorting issues: {e}")
            return
        
        for i in issues[:15]:  # Limit to last 15
            try:
                pred_raw = safe_decode(log_data.get(i))
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_num = history.get(i)
                
                actual_size = None
                if actual_num is not None and isinstance(actual_num, int) and 0 <= actual_num <= 9:
                    actual_size = "Big" if SIZE_MAP.get(actual_num) == "B" else "Small"
                
                history_table.append({
                    "issue": i,
                    "predicted_size": pred.get("next_size"),
                    "actual_size": actual_size,
                    "prediction_time": pred.get("last_updated"),
                    "rule_name": pred.get("rule_name"),
                    "accuracy": pred.get("score", 0)
                })
                processed_count += 1
                
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for issue {i}: {e}")
                continue
            except Exception as e:
                logger.warning(f"⚠️ Error processing size history for issue {i}: {e}")
                continue
        
        set_redis_json(REDIS_SIZE_PREDICTION_HISTORY_KEY, history_table)
        logger.debug(f"📋 Updated size prediction history with {processed_count} entries")
        
    except Exception as e:
        logger.error(f"❌ Error updating size prediction history: {e}", exc_info=True)

# --- Enhanced Accuracy Calculation Functions ---
def update_accuracy():
    """Enhanced accuracy calculation with comprehensive error handling"""
    try:
        logger.debug("📊 Updating color accuracy statistics...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for accuracy update")
            return
        
        # ✅ IMPROVED: Batch Redis operations
        with redis_lock:
            pipe = redis_client.pipeline()
            pipe.hgetall(REDIS_COLOR_PREDICTION_LOG_KEY)
            pipe.get(REDIS_HISTORY_KEY)
            pipe.get(REDIS_RESET_POINT)
            results = pipe.execute()
        
        log_data = results[0] if results[0] else {}
        history_raw = results[1]
        reset_point_raw = results[2]
        
        # Decode data
        try:
            history = json.loads(history_raw) if history_raw else {}
            reset_from = int(json.loads(reset_point_raw)) if reset_point_raw else None
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"❌ Error decoding data: {e}")
            history = {}
            reset_from = None
        
        total = correct = 0
        per_rule = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # ✅ IMPROVED: Process issues with better validation
        try:
            sorted_issues = sorted([k for k in log_data.keys() if k.isdigit()], key=int)
        except Exception as e:
            logger.error(f"❌ Error sorting issues: {e}")
            return
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = safe_decode(log_data.get(issue))
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_value = history.get(issue)
                
                if actual_value is None or not isinstance(actual_value, int) or not (0 <= actual_value <= 9):
                    continue
                
                actual_color = "Red" if COLOR_MAP[actual_value] == "R" else "Green"
                predicted_color = pred.get("next_color")
                rule = pred.get("rule_name", "Unknown")
                
                if not predicted_color or predicted_color not in ["Red", "Green"]:
                    logger.warning(f"⚠️ Invalid predicted color for issue {issue}: {predicted_color}")
                    continue
                
                per_rule[rule]["total"] += 1
                total += 1
                
                if predicted_color == actual_color:
                    per_rule[rule]["correct"] += 1
                    correct += 1
                    
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for issue {issue}: {e}")
                continue
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
            "reset_from_issue": reset_from,
            "calculation_method": "enhanced_batch"
        }
        
        set_redis_json(REDIS_COLOR_ACCURACY_KEY, accuracy_data)
        logger.info(f"📊 Color accuracy updated: {correct}/{total} ({accuracy_data['accuracy_percentage']}%)")
        
    except Exception as e:
        logger.error(f"❌ Error updating color accuracy: {e}", exc_info=True)

def update_size_accuracy():
    """Enhanced size accuracy calculation with comprehensive error handling"""
    try:
        logger.debug("📊 Updating size accuracy statistics...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for size accuracy update")
            return
        
        # ✅ IMPROVED: Batch Redis operations
        with redis_lock:
            pipe = redis_client.pipeline()
            pipe.hgetall(REDIS_SIZE_PREDICTION_LOG_KEY)
            pipe.get(REDIS_HISTORY_KEY)
            pipe.get(REDIS_RESET_POINT)
            results = pipe.execute()
        
        log_data = results[0] if results[0] else {}
        history_raw = results[1]
        reset_point_raw = results[2]
        
        # Decode data
        try:
            history = json.loads(history_raw) if history_raw else {}
            reset_from = int(json.loads(reset_point_raw)) if reset_point_raw else None
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"❌ Error decoding data: {e}")
            history = {}
            reset_from = None
        
        total = correct = 0
        per_rule = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # ✅ IMPROVED: Process issues with better validation
        try:
            sorted_issues = sorted([k for k in log_data.keys() if k.isdigit()], key=int)
        except Exception as e:
            logger.error(f"❌ Error sorting issues: {e}")
            return
        
        for issue in sorted_issues:
            try:
                # Skip if before reset point
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = safe_decode(log_data.get(issue))
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_value = history.get(issue)
                
                if actual_value is None or not isinstance(actual_value, int) or not (0 <= actual_value <= 9):
                    continue
                
                actual_size = "Big" if SIZE_MAP[actual_value] == "B" else "Small"
                predicted_size = pred.get("next_size")
                rule = pred.get("rule_name", "Unknown")
                
                if not predicted_size or predicted_size not in ["Big", "Small"]:
                    logger.warning(f"⚠️ Invalid predicted size for issue {issue}: {predicted_size}")
                    continue
                
                per_rule[rule]["total"] += 1
                total += 1
                
                if predicted_size == actual_size:
                    per_rule[rule]["correct"] += 1
                    correct += 1
                    
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for issue {issue}: {e}")
                continue
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
            "reset_from_issue": reset_from,
            "calculation_method": "enhanced_batch"
        }
        
        set_redis_json(REDIS_SIZE_ACCURACY_KEY, accuracy_data)
        logger.info(f"📊 Size accuracy updated: {correct}/{total} ({accuracy_data['accuracy_percentage']}%)")
        
    except Exception as e:
        logger.error(f"❌ Error updating size accuracy: {e}", exc_info=True)

# ✅ NEW: Optimized Streak Calculation Functions
def calculate_color_streaks():
    """✅ OPTIMIZED: Color streak calculation using cached approach"""
    try:
        logger.debug("📈 Calculating color streaks with optimization...")
        
        if not redis_client or not color_streak_calculator:
            logger.error("❌ Redis client or color streak calculator not available")
            return
        
        # Get reset point
        reset_point_raw = get_redis_json(REDIS_RESET_POINT)
        reset_from = int(reset_point_raw) if reset_point_raw else None
        
        if not reset_from:
            logger.warning("⚠️ No reset point found for color streak calculation")
            return
        
        # ✅ USE OPTIMIZED CALCULATOR: Only recalculates when reset issue changes
        # This is the key optimization - O(1) most times, O(n) only on reset change
        optimized_streaks = color_streak_calculator.get_or_calculate_streaks(
            reset_issue_no=reset_from,
            data_for_calculation={
                'prediction_log': get_redis_hash_all(REDIS_COLOR_PREDICTION_LOG_KEY),
                'history': decode_history(),
                'calculation_type': 'color'
            }
        )
        
        # Store results in original keys for compatibility
        set_redis_json(REDIS_COLOR_STREAKS_KEY, optimized_streaks)
        set_redis_json(REDIS_CONTROLLED_STREAKS_KEY, optimized_streaks)
        
        logger.info(f"📈 OPTIMIZED color streaks: win={optimized_streaks.get('current_win_streak', 0)}, lose={optimized_streaks.get('current_lose_streak', 0)}")
        
    except Exception as e:
        logger.error(f"❌ Error in optimized color streak calculation: {e}")
        # Fallback to original calculation if optimization fails
        calculate_color_streaks_original()

def calculate_size_streaks():
    """✅ OPTIMIZED: Size streak calculation using cached approach"""
    try:
        logger.debug("📈 Calculating size streaks with optimization...")
        
        if not redis_client or not size_streak_calculator:
            logger.error("❌ Redis client or size streak calculator not available")
            return
        
        # Get reset point
        reset_point_raw = get_redis_json(REDIS_RESET_POINT)
        reset_from = int(reset_point_raw) if reset_point_raw else None
        
        if not reset_from:
            logger.warning("⚠️ No reset point found for size streak calculation")
            return
        
        # ✅ USE OPTIMIZED CALCULATOR: Only recalculates when reset issue changes
        # This is the key optimization - O(1) most times, O(n) only on reset change
        optimized_streaks = size_streak_calculator.get_or_calculate_streaks(
            reset_issue_no=reset_from,
            data_for_calculation={
                'prediction_log': get_redis_hash_all(REDIS_SIZE_PREDICTION_LOG_KEY),
                'history': decode_history(),
                'calculation_type': 'size'
            }
        )
        
        # Store results in original keys for compatibility
        set_redis_json(REDIS_SIZE_STREAKS_KEY, optimized_streaks)
        set_redis_json(REDIS_CONTROLLED_SIZE_STREAKS_KEY, optimized_streaks)
        
        logger.info(f"📈 OPTIMIZED size streaks: win={optimized_streaks.get('current_win_streak', 0)}, lose={optimized_streaks.get('current_lose_streak', 0)}")
        
    except Exception as e:
        logger.error(f"❌ Error in optimized size streak calculation: {e}")
        # Fallback to original calculation if optimization fails
        calculate_size_streaks_original()

# ✅ FALLBACK: Original streak calculation methods (kept as backup)
def calculate_color_streaks_original():
    """Original color streak calculation - used as fallback"""
    try:
        logger.debug("📈 Fallback: Calculating color streaks using original method...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for color streak calculation")
            return
        
        # ✅ IMPROVED: Batch Redis operations
        with redis_lock:
            pipe = redis_client.pipeline()
            pipe.hgetall(REDIS_COLOR_PREDICTION_LOG_KEY)
            pipe.get(REDIS_HISTORY_KEY)
            pipe.get(REDIS_RESET_POINT)
            results = pipe.execute()
        
        pred_log_raw = results[0] if results[0] else {}
        history_raw = results[1]
        reset_point_raw = results[2]
        
        # Decode data
        try:
            history = json.loads(history_raw) if history_raw else {}
            reset_from = int(json.loads(reset_point_raw)) if reset_point_raw else None
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"❌ Error decoding data: {e}")
            history = {}
            reset_from = None
        
        if not pred_log_raw or not history:
            logger.warning("⚠️ Missing prediction log or history for color streaks")
            return
        
        # Process issues in chronological order
        try:
            sorted_issues = sorted([k for k in pred_log_raw.keys() if k.isdigit()], key=int)
        except Exception as e:
            logger.error(f"❌ Error sorting issues: {e}")
            return
        
        win = lose = max_win = max_lose = 0
        wd = defaultdict(int)  # win distribution
        ld = defaultdict(int)  # loss distribution
        
        for issue in sorted_issues:
            try:
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = safe_decode(pred_log_raw.get(issue))
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_val = history.get(issue)
                
                if actual_val is None or not isinstance(actual_val, int) or not (0 <= actual_val <= 9):
                    continue
                
                actual_color = "Red" if COLOR_MAP[actual_val] == 'R' else "Green"
                predicted = pred.get("next_color")
                
                if not predicted or predicted not in ["Red", "Green"]:
                    continue
                
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
                    
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for issue {issue}: {e}")
                continue
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
            "reset_from_issue": reset_from,
            "calculation_method": "original_fallback"
        }
        
        set_redis_json(REDIS_COLOR_STREAKS_KEY, result)
        set_redis_json(REDIS_CONTROLLED_STREAKS_KEY, result)
        
        logger.info(f"📈 Original color streaks updated from reset point {reset_from}: win={current_win}, lose={current_lose}")
        
    except Exception as e:
        logger.error(f"❌ Error calculating original color streaks: {e}", exc_info=True)

def calculate_size_streaks_original():
    """Original size streak calculation - used as fallback"""
    try:
        logger.debug("📈 Fallback: Calculating size streaks using original method...")
        
        if not redis_client:
            logger.error("❌ Redis client not available for size streak calculation")
            return
        
        # ✅ IMPROVED: Batch Redis operations
        with redis_lock:
            pipe = redis_client.pipeline()
            pipe.hgetall(REDIS_SIZE_PREDICTION_LOG_KEY)
            pipe.get(REDIS_HISTORY_KEY)
            pipe.get(REDIS_RESET_POINT)
            results = pipe.execute()
        
        pred_log_raw = results[0] if results[0] else {}
        history_raw = results[1]
        reset_point_raw = results[2]
        
        # Decode data
        try:
            history = json.loads(history_raw) if history_raw else {}
            reset_from = int(json.loads(reset_point_raw)) if reset_point_raw else None
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"❌ Error decoding data: {e}")
            history = {}
            reset_from = None
        
        if not pred_log_raw or not history:
            logger.warning("⚠️ Missing size prediction log or history for size streaks")
            return
        
        # Process issues in chronological order
        try:
            sorted_issues = sorted([k for k in pred_log_raw.keys() if k.isdigit()], key=int)
        except Exception as e:
            logger.error(f"❌ Error sorting issues: {e}")
            return
        
        win = lose = max_win = max_lose = 0
        wd = defaultdict(int)  # win distribution
        ld = defaultdict(int)  # loss distribution
        
        for issue in sorted_issues:
            try:
                if reset_from and int(issue) < reset_from:
                    continue
                
                pred_raw = safe_decode(pred_log_raw.get(issue))
                if not pred_raw:
                    continue
                
                pred = json.loads(pred_raw)
                actual_val = history.get(issue)
                
                if actual_val is None or not isinstance(actual_val, int) or not (0 <= actual_val <= 9):
                    continue
                
                actual_size = "Big" if SIZE_MAP[actual_val] == 'B' else "Small"
                predicted = pred.get("next_size")
                
                if not predicted or predicted not in ["Big", "Small"]:
                    continue
                
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
                    
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ JSON decode error for issue {issue}: {e}")
                continue
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
            "reset_from_issue": reset_from,
            "calculation_method": "original_fallback"
        }
        
        set_redis_json(REDIS_SIZE_STREAKS_KEY, result)
        set_redis_json(REDIS_CONTROLLED_SIZE_STREAKS_KEY, result)
        
        logger.info(f"📈 Original size streaks updated from reset point {reset_from}: win={current_win}, lose={current_lose}")
        
    except Exception as e:
        logger.error(f"❌ Error calculating original size streaks: {e}", exc_info=True)

# ✅ MOVED: Initialize streak calculators at the end of Part 2 (not in the middle)
# This will be called after all functions are properly defined
def initialize_part2_components():
    """Initialize Part 2 components after all functions are defined"""
    try:
        initialize_streak_calculators()
        logger.info("✅ Part 2 components initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Part 2 components: {e}")

# --- Enhanced Main Prediction Functions ---


## --- Enhanced Main Prediction Functions ---
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
            return True  # ✅ FIXED: Added return value
        
        history = decode_history()
        if not history:
            logger.warning("⚠️ History not found. Skipping color cycle.")
            update_color_worker_status("warning", "No history data available")
            return False  # ✅ FIXED: Added return value

        sequence_raw = get_color_sequence(history)
        if not sequence_raw:
            logger.warning("⚠️ Color sequence is empty. Skipping color cycle.")
            update_color_worker_status("warning", "Empty color sequence")
            return False  # ✅ FIXED: Added return value
        
        # ✅ IMPROVED: Validate sequence length
        if len(sequence_raw) < 5:
            logger.warning("⚠️ Insufficient color sequence data for reliable prediction.")
            update_color_worker_status("warning", f"Only {len(sequence_raw)} entries available")
        
        # --- Step 1: Evaluate the PREVIOUS prediction ---
        last_issue_key = max(history.keys(), key=int)
        actual_value = history.get(str(last_issue_key))

        if actual_value is not None:
            actual_color = "Red" if COLOR_MAP[actual_value] == 'R' else "Green"
            
            # Fetch the prediction that was made for this now-completed issue
            last_prediction_data = get_redis_hash_json(REDIS_COLOR_PREDICTION_LOG_KEY, str(last_issue_key))
            
            if last_prediction_data:
                last_predicted_color = last_prediction_data.get("next_color")

                if last_predicted_color == actual_color:
                    current_loss = 0
                    logger.info(f"✅ Correct color prediction for issue #{last_issue_key} ('{actual_color}'). Loss streak reset.")
                else:
                    current_loss += 1
                    logger.warning(f"❌ Color mismatch for issue #{last_issue_key}: expected '{actual_color}', but predicted '{last_predicted_color}'. Loss streak is now {current_loss}.")
                    
                    # Check for retraining conditions
                    current_streak = get_redis_json(REDIS_COLOR_STREAKS_KEY, {})
                    current_lose_streak = current_streak.get("current_lose_streak", 0)

                    if current_loss >= MAX_ALLOWED_LOSS_STREAK or current_lose_streak >= EMERGENCY_LOSS_STREAK:
                        logger.warning(f"🚨 Triggering color rule retraining...")
                        update_color_worker_status("retraining", f"Loss streak: {current_loss}, Current streak: {current_lose_streak}")
                        
                        # ✅ IMPROVED: Better training data selection
                        if current_lose_streak >= EMERGENCY_LOSS_STREAK:
                            logger.warning(f"🚨 Emergency color retrain triggered: current_lose_streak = {current_lose_streak}")
                            train_data = sequence_raw[-min(300, len(sequence_raw)):]  # ✅ FIXED: Prevent index error
                        else:
                            logger.warning(f"📉 Regular color retrain triggered: current_loss = {current_loss}")
                            train_data = sequence_raw[-min(TRAINING_WINDOW_SIZE, len(sequence_raw)):]  # ✅ FIXED: Prevent index error

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
                                logger.error("❌ Failed to retrain color rules")
            else:
                logger.info(f"ℹ️ No previous prediction found for issue #{last_issue_key}")

        # --- Step 2: Generate NEW prediction for next issue ---
        
        # Get next issue
        next_issue = calculate_next_issue_number(last_issue_key)
        
        # ✅ IMPROVED: Enhanced validation
        if not validate_issue_key(next_issue):
            logger.error(f"❌ Generated invalid issue key: {next_issue}")
            update_color_worker_status("error", f"Invalid issue key generated: {next_issue}")
            return False  # ✅ FIXED: Added return value
        
        # Combine learned rules with predefined fallback rules
        with rules_lock:
            effective_rules = get_effective_rulebook(rules, PREDEFINED_COLOR_RULES, "color")
        
        if not effective_rules:
            logger.warning("⚠️ Empty rulebook for color prediction, using predefined rules only")
            effective_rules = PREDEFINED_COLOR_RULES
        
        # ✅ IMPROVED: Enhanced prediction with better error handling
        try:
            color_pred, color_rule, color_acc = predict_next_color(sequence_raw, rules)
        except Exception as e:
            logger.error(f"❌ Error during color prediction: {e}")
            # Fallback to time-based prediction
            color_pred, color_rule, color_acc = get_time_based_color_prediction()
            logger.warning("⚠️ Using emergency time-based color prediction")
        
        # ✅ IMPROVED: Enhanced prediction data with validation
        color_prediction_data = {
            "issue": next_issue,
            "next_color": "Red" if color_pred == "R" else "Green",
            "rule_name": str(color_rule),  # ✅ FIXED: Ensure string
            "confidence": max(0.0, min(1.0, color_acc / 100.0 if color_acc else 0.5)),  # ✅ FIXED: Clamp to [0,1]
            "score": max(0.0, min(100.0, color_acc if color_acc else 50.0)),  # ✅ FIXED: Clamp to [0,100]
            "observed_sequence": ''.join([c for _, c in sequence_raw[-6:]]) if len(sequence_raw) >= 6 else ''.join([c for _, c in sequence_raw]),
            "last_updated": datetime.now(pytz.utc).isoformat(),
            "prediction_source": "enhanced_rules",
            "available_rules": len(effective_rules),
            "sequence_length": len(sequence_raw),  # ✅ ADDED: For debugging
            "worker_version": "v3.0_optimized"  # ✅ ADDED: Version tracking
        }
        
        # Log the prediction
        if log_color_prediction(color_prediction_data):
            logger.info(f"✅ New color prediction logged for issue #{next_issue} → '{color_prediction_data['next_color']}' via rule '{color_rule}' ({color_acc:.1f}%)")
        else:
            logger.error("❌ Failed to log color prediction")
            return False  # ✅ FIXED: Return on failure
        
        # Update accuracy and streaks
        try:
            update_prediction_history()
            update_accuracy()
            
            # ✅ OPTIMIZED: Use the optimized streak calculation
            calculate_color_streaks()
        except Exception as e:
            logger.error(f"❌ Error updating color statistics: {e}")
            # Continue despite statistics update failure
        
        # Update worker status
        update_color_worker_status("idle", f"Color prediction completed for issue {next_issue}")
        
        logger.info("✅ Color prediction cycle completed successfully")
        return True  # ✅ FIXED: Added return value
        
    except Exception as e:
        logger.error(f"❌ Color prediction and monitoring failed: {e}")
        logger.exception("Full traceback:")
        update_color_worker_status("error", f"Color prediction failed: {str(e)}")
        return False  # ✅ FIXED: Added return value

def run_size_prediction_and_monitor():
    """Enhanced size prediction and monitoring with predefined rule support"""
    global size_rules, current_size_loss
    
    try:
        logger.info("📏 Running size prediction cycle...")
        update_color_worker_status("size_prediction", "Running size prediction cycle")
        
        # ✅ ADDED: Check for shutdown signal
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during size prediction")
            return True
        
        history = decode_history()
        if not history:
            logger.warning("⚠️ History not found. Skipping size cycle.")
            return False  # ✅ FIXED: Added return value

        size_sequence_raw = get_size_sequence(history)
        if not size_sequence_raw:
            logger.warning("⚠️ Size sequence is empty. Skipping size cycle.")
            return False  # ✅ FIXED: Added return value
        
        # ✅ IMPROVED: Validate sequence length
        if len(size_sequence_raw) < 5:
            logger.warning("⚠️ Insufficient size sequence data for reliable prediction.")
            update_color_worker_status("warning", f"Only {len(size_sequence_raw)} entries available")
        
        # Get next issue
        last_issue_key = max(history.keys(), key=int)
        next_issue = calculate_next_issue_number(last_issue_key)
        
        # ✅ IMPROVED: Enhanced validation
        if not validate_issue_key(next_issue):
            logger.error(f"❌ Generated invalid issue key: {next_issue}")
            update_color_worker_status("error", f"Invalid issue key generated: {next_issue}")
            return False  # ✅ FIXED: Added return value
        
        # Evaluate previous size prediction if exists
        actual_value = history.get(str(last_issue_key))
        if actual_value is not None:
            actual_size = "Big" if SIZE_MAP[actual_value] == 'B' else "Small"
            
            last_prediction_data = get_redis_hash_json(REDIS_SIZE_PREDICTION_LOG_KEY, str(last_issue_key))
            if last_prediction_data:
                last_predicted_size = last_prediction_data.get("next_size")
                
                if last_predicted_size == actual_size:
                    current_size_loss = 0
                    logger.info(f"✅ Correct size prediction for issue #{last_issue_key} ('{actual_size}'). Loss streak reset.")
                else:
                    current_size_loss += 1
                    logger.warning(f"❌ Size mismatch for issue #{last_issue_key}: expected '{actual_size}', but predicted '{last_predicted_size}'. Loss streak is now {current_size_loss}.")
                    
                    # Check for retraining conditions
                    current_size_streak = get_redis_json(REDIS_SIZE_STREAKS_KEY, {})
                    current_size_lose_streak = current_size_streak.get("current_lose_streak", 0)

                    if current_size_loss >= MAX_ALLOWED_LOSS_STREAK or current_size_lose_streak >= EMERGENCY_LOSS_STREAK:
                        logger.warning(f"🚨 Triggering size rule retraining...")
                        update_color_worker_status("size_retraining", f"Loss streak: {current_size_loss}, Current streak: {current_size_lose_streak}")
                        
                        # ✅ IMPROVED: Better training data selection
                        if current_size_lose_streak >= EMERGENCY_LOSS_STREAK:
                            logger.warning(f"🚨 Emergency size retrain triggered: current_lose_streak = {current_size_lose_streak}")
                            train_data = size_sequence_raw
                            #[-min(300, len(size_sequence_raw)):]  # ✅ FIXED: Prevent index error
                        else:
                            logger.warning(f"📉 Regular size retrain triggered: current_loss = {current_size_loss}")
                            train_data = size_sequence_raw
                            #[-min(TRAINING_WINDOW_SIZE, len(size_sequence_raw)):]  # ✅ FIXED: Prevent index error

                        with rules_lock:
                            new_size_rules = retrain_size_rules(train_data)
                            
                            if new_size_rules:
                                logger.info(f"🧠 Size model retrained at {datetime.now().isoformat()} with {len(new_size_rules)} rules.")
                                logger.info("📊 Top 5 size patterns after retraining:")
                                top_rules = sorted(new_size_rules.items(), key=lambda item: item[1]['total'], reverse=True)[:5]
                                for rule_pattern, info in top_rules:
                                    logger.info(f"🔍 {rule_pattern} → predict '{info['predict']}' | Accuracy: {info['accuracy']}% | Total: {info['total']}")
                                
                                size_rules.clear()
                                size_rules.update(new_size_rules)
                                current_size_loss = 0  # Reset after retraining
                                logger.info("✅ Size rules updated successfully")
                            else:
                                logger.error("❌ Failed to retrain size rules")
        
        # Combine learned rules with predefined fallback rules
        with rules_lock:
            effective_rules = get_effective_rulebook(size_rules, PREDEFINED_SIZE_RULES, "size")
        
        if not effective_rules:
            logger.warning("⚠️ Empty size rulebook, using predefined rules only")
            effective_rules = PREDEFINED_SIZE_RULES
        
        # ✅ IMPROVED: Enhanced prediction with better error handling
        try:
            size_pred, size_rule, size_acc = predict_next_size(size_sequence_raw, size_rules)
        except Exception as e:
            logger.error(f"❌ Error during size prediction: {e}")
            # Fallback to time-based prediction
            size_pred, size_rule, size_acc = get_time_based_size_prediction()
            logger.warning("⚠️ Using emergency time-based size prediction")
        
        # ✅ IMPROVED: Enhanced prediction data with validation
        size_prediction_data = {
            "issue": next_issue,
            "next_size": "Big" if size_pred == "B" else "Small",
            "rule_name": str(size_rule),  # ✅ FIXED: Ensure string
            "confidence": max(0.0, min(1.0, size_acc / 100.0 if size_acc else 0.5)),  # ✅ FIXED: Clamp to [0,1]
            "score": max(0.0, min(100.0, size_acc if size_acc else 50.0)),  # ✅ FIXED: Clamp to [0,100]
            "observed_sequence": ''.join([s for _, s in size_sequence_raw[-6:]]) if len(size_sequence_raw) >= 6 else ''.join([s for _, s in size_sequence_raw]),
            "last_updated": datetime.now(pytz.utc).isoformat(),
            "prediction_source": "enhanced_rules",
            "available_rules": len(effective_rules),
            "sequence_length": len(size_sequence_raw),  # ✅ ADDED: For debugging
            "worker_version": "v3.0_optimized"  # ✅ ADDED: Version tracking
        }
        
        # Log the prediction
        if log_size_prediction(size_prediction_data):
            logger.info(f"✅ New size prediction logged for issue #{next_issue} → '{size_prediction_data['next_size']}' via rule '{size_rule}' ({size_acc:.1f}%)")
        else:
            logger.error("❌ Failed to log size prediction")
            return False  # ✅ FIXED: Return on failure
        
        # Update accuracy and streaks
        try:
            update_size_prediction_history()
            update_size_accuracy()
            
            # ✅ OPTIMIZED: Use the optimized streak calculation
            calculate_size_streaks()
        except Exception as e:
            logger.error(f"❌ Error updating size statistics: {e}")
            # Continue despite statistics update failure
        
        logger.info("✅ Size prediction cycle completed successfully")
        return True  # ✅ FIXED: Added missing return statement
        
    except Exception as e:
        logger.error(f"❌ Size prediction and monitoring failed: {e}")
        logger.exception("Full traceback:")
        update_color_worker_status("error", f"Size prediction failed: {str(e)}")
        return False  # ✅ FIXED: Added return value

def validate_issue_key(issue_key):
    """
    ✅ ENHANCED: Validate that issue key follows correct format and numbering.
    
    Args:
        issue_key (str): Issue key to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        # ✅ IMPROVED: Better validation
        if not issue_key or not isinstance(issue_key, str):
            logger.warning(f"⚠️ Invalid issue key type: {type(issue_key)}")
            return False
        
        if len(issue_key) < 17:
            logger.warning(f"⚠️ Issue key too short: {len(issue_key)} chars")
            return False
        
        # ✅ IMPROVED: More robust parsing
        try:
            date_part = issue_key[:8]
            middle_part = issue_key[8:13]
            issue_num = int(issue_key[13:])
        except (ValueError, IndexError) as e:
            logger.warning(f"⚠️ Could not parse issue key '{issue_key}': {e}")
            return False
        
        # Check date format
        try:
            datetime.strptime(date_part, "%Y%m%d")
        except ValueError as e:
            logger.warning(f"⚠️ Invalid date in issue key '{issue_key}': {e}")
            return False
        
        # Check middle part (should be '10001')
        if middle_part != '10001':
            logger.warning(f"⚠️ Invalid middle part in issue key '{issue_key}': expected '10001', got '{middle_part}'")
            return False
        
        # Check issue number range
        if issue_num < 1 or issue_num > 1440:
            logger.warning(f"⚠️ Invalid issue number {issue_num} in key {issue_key}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error validating issue key '{issue_key}': {e}")
        return False

def run_dual_prediction_cycle():
    """✅ ENHANCED: Run both color and size predictions with better error handling"""
    logger.info("🚀 Starting dual prediction cycle...")
    
    color_success = False
    size_success = False
    
    try:
        # ✅ IMPROVED: Independent execution with individual error handling
        try:
            color_success = run_color_prediction_and_monitor()
        except Exception as e:
            logger.error(f"❌ Color prediction failed independently: {e}")
            color_success = False
        
        try:
            size_success = run_size_prediction_and_monitor()
        except Exception as e:
            logger.error(f"❌ Size prediction failed independently: {e}")
            size_success = False
        
        # ✅ IMPROVED: Detailed result reporting
        if color_success and size_success:
            logger.info("✅ Dual prediction cycle completed successfully")
            return True
        elif color_success or size_success:
            logger.warning(f"⚠️ Partial success: Color={'✅' if color_success else '❌'}, Size={'✅' if size_success else '❌'}")
            return True  # Partial success is still acceptable
        else:
            logger.error("❌ Both color and size predictions failed")
            return False
        
    except Exception as e:
        logger.error(f"❌ Critical error in dual prediction cycle: {e}")
        logger.exception("Full traceback:")
        return False

def initialize_predefined_rules():
    """✅ ENHANCED: Initialize predefined rules and log statistics"""
    try:
        logger.info("🎯 Initializing predefined rule system...")
        # ✅ Clean up corrupted data on startup
        cleanup_corrupted_prediction_logs()
        # ✅ IMPROVED: Validate rule structures
        def validate_rule_dict(rules_dict, name):
            valid_count = 0
            for pattern, rule in rules_dict.items():
                if isinstance(rule, dict) and all(k in rule for k in ['predict', 'accuracy']):
                    valid_count += 1
                else:
                    logger.warning(f"⚠️ Invalid {name} rule for pattern '{pattern}': {rule}")
            return valid_count
        
        # Log predefined color rules
        valid_color_rules = validate_rule_dict(PREDEFINED_COLOR_RULES, "color")
        logger.info(f"🎨 Loaded {valid_color_rules}/{len(PREDEFINED_COLOR_RULES)} valid predefined color rules:")
        for pattern, rule in list(PREDEFINED_COLOR_RULES.items())[:5]:  # Show first 5
            logger.info(f"  📋 {pattern} → {rule['predict']} ({rule['accuracy']}%)")
            
        # Log 25 pattern-matching rules
        logger.info(f"🎨 Loaded {len(RED_GREEN_PATTERNS)} 25-Rules patterns.")
        for rule_name, ab_pattern in list(RED_GREEN_PATTERNS.items())[:3]:  # Show first 3
            logger.info(f"  📋 {rule_name}: {ab_pattern}")
        
        # Log predefined size rules  
        valid_size_rules = validate_rule_dict(PREDEFINED_SIZE_RULES, "size")
        logger.info(f"📏 Loaded {valid_size_rules}/{len(PREDEFINED_SIZE_RULES)} valid predefined size rules:")
        for pattern, rule in list(PREDEFINED_SIZE_RULES.items())[:5]:  # Show first 5
            logger.info(f"  📋 {pattern} → {rule['predict']} ({rule['accuracy']}%)")

        # Log 25 pattern-matching rules for size
        logger.info(f"📏 Loaded {len(SMALL_BIG_PATTERNS)} 25-Rules size patterns.")
        for rule_name, ab_pattern in list(SMALL_BIG_PATTERNS.items())[:3]:  # Show first 3
            logger.info(f"  📋 {rule_name}: {ab_pattern}")
        
        # ✅ IMPROVED: Enhanced fallback testing
        try:
            test_color_pred = get_time_based_color_prediction()
            test_size_pred = get_time_based_size_prediction()
            logger.info(f"⏰ Time-based fallbacks ready: Color={test_color_pred[0]}, Size={test_size_pred[0]}")
        except Exception as e:
            logger.error(f"❌ Error testing fallback predictions: {e}")
        
        # ✅ IMPROVED: Enhanced streak calculator status
        if color_streak_calculator and size_streak_calculator:
            logger.info("⚡ Optimized streak calculators ready (only recalculate when reset issue changes)")
            
            # Check if cached streaks exist
            try:
                color_cached = get_redis_json(REDIS_COLOR_CACHED_STREAKS)
                size_cached = get_redis_json(REDIS_SIZE_CACHED_STREAKS)
                
                logger.info(f"💾 Color streaks cached: {'Yes' if color_cached else 'No'}")
                logger.info(f"💾 Size streaks cached: {'Yes' if size_cached else 'No'}")
                
                # Show current reset issues if available
                color_reset = get_redis_json(REDIS_COLOR_CURRENT_RESET_ISSUE)
                size_reset = get_redis_json(REDIS_SIZE_CURRENT_RESET_ISSUE)
                
                if color_reset:
                    logger.info(f"🔄 Color streak reset issue: {color_reset}")
                if size_reset:
                    logger.info(f"🔄 Size streak reset issue: {size_reset}")
            except Exception as e:
                logger.error(f"❌ Error checking streak calculator status: {e}")
        else:
            logger.warning("⚠️ Optimized streak calculators not initialized - falling back to original calculation")
        
        logger.info("✅ Predefined rule system initialized successfully")
        
    except Exception as e:
        logger.error(f"❌ Error initializing predefined rules: {e}")
        logger.exception("Full traceback:")

def calculate_next_run_time():
    """✅ ENHANCED: Calculate the next run time (59th second)"""
    try:
        now_utc = datetime.now(pytz.utc)
        current_sec = now_utc.second
        
        # Target 59th second of current minute, or next minute if past 59th second
        target_time = now_utc.replace(second=59, microsecond=0)
        if current_sec >= 59:
            target_time += timedelta(minutes=1)
        
        sleep_duration = (target_time - now_utc).total_seconds()
        
        # ✅ IMPROVED: Ensure positive sleep duration
        if sleep_duration < 0:
            target_time += timedelta(minutes=1)
            sleep_duration = (target_time - now_utc).total_seconds()
        
        return target_time, max(0, sleep_duration)
        
    except Exception as e:
        logger.error(f"❌ Error calculating next run time: {e}")
        # Fallback: run in 60 seconds
        now = datetime.now(pytz.utc)
        return now + timedelta(seconds=60), 60.0

def cleanup_on_shutdown():
    """✅ NEW: Cleanup function for graceful shutdown"""
    try:
        logger.info("🧹 Starting cleanup on shutdown...")
        
        # Update worker status
        update_color_worker_status("shutdown", "Worker shutting down gracefully")
        
        # ✅ Clear any pending operations
        try:
            with redis_lock:
                # Set final heartbeat
                heartbeat_data = {
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "status": "shutdown",
                    "final_message": "Worker shutdown complete"
                }
                set_redis_json(REDIS_COLOR_WORKER_HEARTBEAT_KEY, heartbeat_data, expiry=300)
        except Exception as e:
            logger.error(f"❌ Error during Redis cleanup: {e}")
        
        logger.info("✅ Cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}")

def main_worker_loop():
    """✅ ENHANCED: Main color worker loop with improved reliability"""
    logger.info("🚀 Color worker loop starting...")
    
    # ✅ IMPROVED: Enhanced initialization
    try:
        initialize_predefined_rules()
    except Exception as e:
        logger.critical(f"💥 Failed to initialize predefined rules: {e}")
        return False
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    total_cycles = 0
    successful_cycles = 0
    
    # ✅ NEW: Performance tracking
    start_time = datetime.now(pytz.utc)
    last_success_time = start_time
    
    try:
        while not shutdown_event.is_set():
            cycle_start = datetime.now(pytz.utc)
            total_cycles += 1
            
            try:
                # Calculate next run time
                target_time, sleep_duration = calculate_next_run_time()
                current_time = datetime.now(pytz.utc)
                
                logger.info(
                    f"⏰ Cycle #{total_cycles} | Current: {current_time.strftime('%H:%M:%S')} UTC | "
                    f"Next run: {target_time.strftime('%H:%M:%S')} UTC | "
                    f"Sleep: {sleep_duration:.1f}s"
                )
                
                # ✅ IMPROVED: Interruptible sleep with heartbeat
                elapsed = 0
                heartbeat_interval = 30  # Send heartbeat every 30 seconds during sleep
                last_heartbeat = 0
                
                while elapsed < sleep_duration and not shutdown_event.is_set():
                    sleep_chunk = min(5, sleep_duration - elapsed)
                    
                    if shutdown_event.wait(timeout=sleep_chunk):
                        break
                    
                    elapsed += sleep_chunk
                    
                    # Send periodic heartbeats during long sleeps
                    if elapsed - last_heartbeat >= heartbeat_interval:
                        try:
                            send_color_worker_heartbeat()
                            last_heartbeat = elapsed
                        except Exception as e:
                            logger.warning(f"⚠️ Heartbeat failed during sleep: {e}")
                
                if shutdown_event.is_set():
                    logger.info("🛑 Shutdown requested during sleep")
                    break
                
                # Execute dual prediction cycle
                current_utc = datetime.now(pytz.utc)
                logger.info(f"🎯 Executing enhanced dual prediction cycle at {current_utc.strftime('%H:%M:%S')} UTC")
                
                success = run_dual_prediction_cycle()
                
                cycle_duration = (datetime.now(pytz.utc) - cycle_start).total_seconds()
                
                if success:
                    consecutive_failures = 0
                    successful_cycles += 1
                    last_success_time = datetime.now(pytz.utc)
                    logger.info(f"✅ Cycle #{total_cycles} completed successfully in {cycle_duration:.2f}s")
                else:
                    consecutive_failures += 1
                    logger.error(f"❌ Cycle #{total_cycles} failed (consecutive failures: {consecutive_failures})")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.critical(f"💥 {max_consecutive_failures} consecutive failures. Exiting.")
                        break
                
                # ✅ IMPROVED: Performance logging
                if total_cycles % 60 == 0:  # Every hour
                    uptime = (datetime.now(pytz.utc) - start_time).total_seconds()
                    success_rate = (successful_cycles / total_cycles) * 100
                    logger.info(f"📊 Performance: {success_rate:.1f}% success rate over {total_cycles} cycles, {uptime/3600:.1f}h uptime")
                
                # ✅ IMPROVED: Brief pause between cycles
                time.sleep(2)
                
            except KeyboardInterrupt:
                logger.info("🛑 Keyboard interrupt received")
                break
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"❌ Unexpected error in worker loop cycle #{total_cycles}: {e}", exc_info=True)
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.critical(f"💥 {max_consecutive_failures} consecutive errors. Exiting.")
                    break
                
                # ✅ IMPROVED: Exponential backoff for errors
                backoff_time = min(300, 30 * (2 ** min(consecutive_failures - 1, 4)))  # Max 5 minutes
                logger.info(f"⏳ Backing off for {backoff_time} seconds after error...")
                
                if shutdown_event.wait(timeout=backoff_time):
                    break
    
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
    except Exception as e:
        logger.critical(f"💥 Critical error in main worker loop: {e}", exc_info=True)
    finally:
        # ✅ IMPROVED: Final statistics
        uptime = (datetime.now(pytz.utc) - start_time).total_seconds()
        success_rate = (successful_cycles / total_cycles * 100) if total_cycles > 0 else 0
        
        logger.info(f"📊 Final stats: {successful_cycles}/{total_cycles} successful cycles ({success_rate:.1f}%), {uptime/3600:.1f}h uptime")
        
        cleanup_on_shutdown()
        logger.info("🏁 Color worker loop ended")

# ✅ ENHANCED: Graceful shutdown handling
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    try:
        signal_name = signal.Signals(signum).name
        logger.info(f"🛑 Received {signal_name} signal. Initiating graceful shutdown...")
        shutdown_event.set()
    except Exception as e:
        logger.error(f"❌ Error in signal handler: {e}")
        shutdown_event.set()  # Still set the event

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("🚀 LOTTERY COLOR & SIZE PREDICTION WORKER WITH OPTIMIZED STREAKS")
    logger.info("=" * 80)
    
    # ✅ IMPROVED: Enhanced environment validation
    REDIS_URL = os.getenv("REDIS_URL")
    
    logger.info(f"🔗 Redis URL: {REDIS_URL[:50] if REDIS_URL else 'NOT SET'}...")
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📂 Data Directory: {DATA_DIR}")
    logger.info(f"🎨 Predefined Color Rules: {len(PREDEFINED_COLOR_RULES)}")
    logger.info(f"📏 Predefined Size Rules: {len(PREDEFINED_SIZE_RULES)}")
    
    # ✅ IMPROVED: Enhanced optimization status
    logger.info("⚡ OPTIMIZED STREAK CALCULATION:")
    logger.info("   - Only recalculates when reset issue changes (O(n) → O(1))")
    logger.info("   - Persistent caching in Redis")
    logger.info("   - Incremental updates between resets")
    logger.info("   - Enhanced error handling and recovery")
    
    # ✅ IMPROVED: Better environment validation
    if not REDIS_URL:
        logger.critical("💥 REDIS_URL environment variable not set. Exiting.")
        sys.exit(1)
    
    # ✅ NEW: Test Redis connection before starting
    try:
        if redis_client:
            redis_client.ping()
            logger.info("✅ Redis connection verified")
        else:
            logger.critical("💥 Redis client not initialized. Exiting.")
            sys.exit(1)
    except Exception as e:
        logger.critical(f"💥 Redis connection test failed: {e}. Exiting.")
        sys.exit(1)
    
    logger.info("🎯 Starting enhanced color & size prediction worker...")
    logger.info("📅 Scheduled to run every minute at 59th second")
    logger.info("🛡️ Predefined rules ensure predictions even without historical data")
    logger.info("⚡ Optimized streak calculation reduces computation overhead")
    logger.info("🔧 Enhanced error handling and graceful shutdown support")
    
    # ✅ IMPROVED: Better main execution with cleanup
    exit_code = 0
    try:
        main_worker_loop()
    except Exception as e:
        logger.critical(f"💥 Fatal error in main execution: {e}", exc_info=True)
        exit_code = 1
    finally:
        try:
            cleanup_on_shutdown()
        except Exception as e:
            logger.error(f"❌ Error during final cleanup: {e}")
        
        logger.info("🏁 Enhanced color worker shutdown complete")
        sys.exit(exit_code)
