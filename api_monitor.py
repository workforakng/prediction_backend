# api_monitor.py
import requests
import time
import json
import os
import logging
import base64
import urllib.parse
import re
import numpy as np
import redis
import pytz
import signal
import sys
from collections import defaultdict, Counter
from datetime import datetime
from dotenv import load_dotenv
from threading import Lock

# Load .env file first, so os.getenv works everywhere
load_dotenv()

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'api_monitor')

# --- Setup & Logging with Railway Support ---
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_FILE = os.path.join(DATA_DIR, "lottery_predictor.log")

# Enhanced logging configuration for Railway
log_format = '%(asctime)s - API_MONITOR - %(levelname)s - %(message)s'
handlers = [logging.StreamHandler()]  # Always include stream handler for Railway logs

# Add file handler if writable directory exists
if os.access(DATA_DIR, os.W_OK):
    try:
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
    except Exception as e:
        print(f"Warning: Could not create file handler: {e}")

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=handlers
)
logger = logging.getLogger(__name__)

# Log Railway environment info
if is_railway_environment():
    logger.info(f"🚂 API Monitor running on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
else:
    logger.info("🏠 API Monitor running in local environment")

# --- Enhanced Redis Connection with Railway Support ---
redis_client = None
redis_lock = Lock()

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

# Initialize Redis connection on import
if not initialize_redis():
    logger.critical("❌ Could not initialize Redis connection. Some features may not work.")

# --- Redis Keys ---
REDIS_HISTORY_KEY = "lottery:history"
REDIS_PREDICTIONS_KEY = "lottery:predictions"
REDIS_NUMBER_STATS_KEY = "lottery:number_stats"
REDIS_TREND_ANALYSIS_KEY = "lottery:trend_analysis"
REDIS_API_MONITOR_STATUS_KEY = "lottery:api_monitor_status"
REDIS_API_MONITOR_HEARTBEAT_KEY = "lottery:api_monitor_heartbeat"

# --- Configuration with Validation ---
USERNAME = os.getenv("DAMAN_USERNAME")
PASSWORD = os.getenv("DAMAN_PASSWORD")
REAL_IP = os.getenv("AR_REAL_IP", "103.242.197.139")
GAME_CODE = os.getenv("WIN_GO_GAME_CODE", "WinGo_1M")

# Validate required environment variables
if not USERNAME or not PASSWORD:
    logger.critical("💥 DAMAN_USERNAME and/or DAMAN_PASSWORD not set")
    if __name__ == "__main__":
        sys.exit(1)

DAMAN_LOGIN_RANDOM = os.getenv("DAMAN_LOGIN_RANDOM", "f5e666e3c4e54514b9a882fb79a968ce")
DAMAN_LOGIN_SIGNATURE = os.getenv("DAMAN_LOGIN_SIGNATURE", "4E9D13A54A8A0C88A8E1408340F4AE04")
DAMAN_TREND_RANDOM = os.getenv("DAMAN_TREND_RANDOM", "993368201325")
DAMAN_TREND_SIGNATURE = os.getenv("DAMAN_TREND_SIGNATURE", "78940F6894E13BBFB6D50FE8B864B9F5")

BASE_URL = "https://api.damanapiopawer.com"
DRAW_HISTORY_URL = "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json"
TREND_STAT_URL = "https://h5.ar-lottery01.com/api/Lottery/GetTrendStatistics"

common_headers = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36",
    "Referer": f"https://damanworld.mobi/#/saasLottery/WinGo?gameCode={GAME_CODE}&lottery=WinGo",
    "Ar-Origin": "https://damanworld.mobi",
    "AR-REAL-IP": REAL_IP
}

COLOR_MAP = {
    0: {'primary': 'Red', 'secondary': 'Violet'},
    1: {'primary': 'Green', 'secondary': None},
    2: {'primary': 'Red', 'secondary': None},
    3: {'primary': 'Green', 'secondary': None},
    4: {'primary': 'Red', 'secondary': None},
    5: {'primary': 'Green', 'secondary': 'Violet'},
    6: {'primary': 'Red', 'secondary': None},
    7: {'primary': 'Green', 'secondary': None},
    8: {'primary': 'Red', 'secondary': None},
    9: {'primary': 'Green', 'secondary': None}
}

# --- Status Management Functions ---
def update_api_monitor_status(status, message=None):
    """Update API monitor status in Redis"""
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
                
                redis_client.set(REDIS_API_MONITOR_STATUS_KEY, json.dumps(status_data), ex=300)  # 5 min expiry
                logger.debug(f"📊 API Monitor status updated: {status}")
    except Exception as e:
        logger.error(f"Failed to update API monitor status: {e}")

def send_api_monitor_heartbeat():
    """Send heartbeat to Redis"""
    try:
        if redis_client:
            with redis_lock:
                heartbeat_data = {
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "service_name": get_railway_service_name()
                }
                redis_client.set(REDIS_API_MONITOR_HEARTBEAT_KEY, json.dumps(heartbeat_data), ex=120)  # 2 min expiry
                logger.debug("💗 API Monitor heartbeat sent")
    except Exception as e:
        logger.error(f"Failed to send API monitor heartbeat: {e}")

# --- Enhanced Utility Functions ---
def make_request(method, url, headers=None, json_data=None, params=None, retries=3):
    """Utility function to make HTTP requests with retries and enhanced error handling."""
    last_error = None
    
    for attempt in range(retries):
        try:
            logger.debug(f"🌐 Making {method} request to {url} (attempt {attempt + 1}/{retries})")
            
            response = requests.request(
                method, 
                url, 
                headers=headers, 
                json=json_data, 
                params=params, 
                timeout=15
            )
            
            logger.info(f"📡 Response from {url}: {response.status_code}")
            
            # Check for successful status codes
            if response.status_code == 200:
                try:
                    json_response = response.json()
                    logger.debug(f"✅ Successfully parsed JSON response from {url}")
                    return json_response
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to decode JSON from {url}: {e}")
                    logger.debug(f"Raw response: {response.text[:200]}...")
                    last_error = e
            else:
                logger.warning(f"⚠️ HTTP {response.status_code} from {url}")
                last_error = Exception(f"HTTP {response.status_code}")
                
        except requests.exceptions.Timeout as e:
            logger.warning(f"⏰ Request timeout to {url} [attempt {attempt + 1}]: {e}")
            last_error = e
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"🔌 Connection error to {url} [attempt {attempt + 1}]: {e}")
            last_error = e
        except requests.RequestException as e:
            logger.warning(f"📡 Request error to {url} [attempt {attempt + 1}]: {e}")
            last_error = e
        except Exception as e:
            logger.error(f"💥 Unexpected error during request to {url} [attempt {attempt + 1}]: {e}")
            last_error = e
        
        # Exponential backoff for retries
        if attempt < retries - 1:
            backoff_time = 2 ** attempt
            logger.info(f"⏳ Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)
    
    logger.error(f"💥 Failed to complete request to {url} after {retries} attempts. Last error: {last_error}")
    return None

def decode_jwt_expiry(token):
    """Decodes JWT token to extract expiry timestamp with enhanced error handling."""
    try:
        if not token or not isinstance(token, str):
            logger.warning("Invalid token provided to decode_jwt_expiry")
            return 0
            
        parts = token.split('.')
        if len(parts) != 3:
            logger.warning("Invalid JWT token format - wrong number of parts")
            return 0
            
        payload = parts[1]
        # Add padding if necessary
        padding = '=' * (-len(payload) % 4)
        
        try:
            decoded_bytes = base64.urlsafe_b64decode(payload + padding)
            payload_json = json.loads(decoded_bytes)
            exp_timestamp = int(payload_json.get('exp', 0))
            
            if exp_timestamp:
                exp_datetime = datetime.fromtimestamp(exp_timestamp, tz=pytz.utc)
                logger.debug(f"🔓 JWT expires at: {exp_datetime.isoformat()}")
            
            return exp_timestamp
        except (base64.binascii.Error, json.JSONDecodeError) as e:
            logger.error(f"Failed to decode JWT payload: {e}")
            return 0
            
    except Exception as e:
        logger.error(f"Unexpected error decoding JWT expiry: {e}")
        return 0

class TokenManager:
    """Enhanced token manager with Railway support and better error handling."""
    
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.access_token = None
        self.lottery_token = None
        self.access_token_expiry = 0
        self.lottery_token_expiry = 0
        self.last_login_attempt = 0
        self.login_attempt_count = 0
        self.max_login_attempts = 5
        self.login_cooldown = 300  # 5 minutes
        
        logger.info(f"🔐 TokenManager initialized for user: {username[:3]}***")

    def _can_attempt_login(self):
        """Check if login attempt is allowed based on rate limiting"""
        current_time = time.time()
        
        if self.login_attempt_count >= self.max_login_attempts:
            if current_time - self.last_login_attempt < self.login_cooldown:
                remaining_cooldown = self.login_cooldown - (current_time - self.last_login_attempt)
                logger.warning(f"🚫 Login rate limited. Try again in {remaining_cooldown:.0f} seconds")
                return False
            else:
                # Reset attempts after cooldown
                self.login_attempt_count = 0
        
        return True

    def login(self):
        """Enhanced login with rate limiting and better error handling."""
        if not self._can_attempt_login():
            return False
        
        start_time = time.time()
        self.last_login_attempt = start_time
        self.login_attempt_count += 1
        
        logger.info(f"🔐 Attempting login (attempt {self.login_attempt_count})...")
        update_api_monitor_status("authenticating", "Attempting login")
        
        try:
            url = f"{BASE_URL}/api/webapi/Login"
            random_val = DAMAN_LOGIN_RANDOM
            signature_val = DAMAN_LOGIN_SIGNATURE
            
            data = {
                "username": self.username,
                "pwd": self.password,
                "phonetype": 1,
                "logintype": "mobile",
                "language": 0,
                "random": random_val,
                "signature": signature_val,
                "timestamp": int(time.time())
            }
            
            headers = dict(common_headers)
            headers["Content-Type"] = "application/json;charset=UTF-8"

            resp = make_request("POST", url, headers=headers, json_data=data, retries=2)
            
            if not resp:
                logger.error("❌ No response received from login API")
                update_api_monitor_status("auth_error", "No response from login API")
                return False
            
            if resp.get("code") != 0:
                error_msg = resp.get("message", "Unknown error")
                logger.error(f"❌ Login failed with code {resp.get('code')}: {error_msg}")
                update_api_monitor_status("auth_error", f"Login failed: {error_msg}")
                return False
            
            token_data = resp.get("data", {})
            if not token_data:
                logger.error("❌ No token data in login response")
                update_api_monitor_status("auth_error", "No token data received")
                return False
            
            # Extract access token
            self.access_token = token_data.get("token")
            if not self.access_token:
                logger.error("❌ No access token in response")
                update_api_monitor_status("auth_error", "No access token received")
                return False
            
            # Set access token expiry with buffer
            expires_in = token_data.get("expiresIn", 3600)
            self.access_token_expiry = time.time() + (expires_in - 60)  # 60 sec buffer
            
            # Extract lottery token from URL
            lottery_login_url = token_data.get("lotteryLoginUrl")
            if not lottery_login_url:
                logger.error("❌ No lottery login URL in response")
                update_api_monitor_status("auth_error", "No lottery login URL received")
                return False
            
            try:
                parsed = urllib.parse.urlparse(lottery_login_url)
                query = urllib.parse.parse_qs(parsed.query)
                token_list = query.get("Token")
                
                if not token_list:
                    logger.error("❌ No lottery token in URL parameters")
                    update_api_monitor_status("auth_error", "No lottery token in URL")
                    return False
                
                self.lottery_token = token_list[0]
                self.lottery_token_expiry = decode_jwt_expiry(self.lottery_token)
                
                if self.lottery_token_expiry == 0:
                    logger.warning("⚠️ Could not decode lottery token expiry")
                
            except Exception as e:
                logger.error(f"❌ Error parsing lottery login URL: {e}")
                update_api_monitor_status("auth_error", f"URL parsing error: {str(e)}")
                return False
            
            # Reset attempt count on successful login
            self.login_attempt_count = 0
            
            login_duration = time.time() - start_time
            logger.info(f"✅ Login successful in {login_duration:.2f} seconds")
            update_api_monitor_status("authenticated", "Login successful")
            
            # Log token expiry times
            access_exp = datetime.fromtimestamp(self.access_token_expiry, tz=pytz.utc)
            lottery_exp = datetime.fromtimestamp(self.lottery_token_expiry, tz=pytz.utc) if self.lottery_token_expiry else None
            logger.info(f"🕐 Access token expires: {access_exp.isoformat()}")
            if lottery_exp:
                logger.info(f"🎲 Lottery token expires: {lottery_exp.isoformat()}")
            
            return True
            
        except Exception as e:
            logger.error(f"💥 Unexpected error during login: {e}", exc_info=True)
            update_api_monitor_status("auth_error", f"Login exception: {str(e)}")
            return False

    def ensure_valid_tokens(self):
        """Enhanced token validation with detailed logging."""
        current_time = time.time()
        
        # Check if tokens exist
        if not self.access_token or not self.lottery_token:
            logger.info("🔑 No tokens available, attempting login...")
            return self.login()
        
        # Check token expiry with buffer
        access_valid = current_time < self.access_token_expiry
        lottery_valid = current_time < self.lottery_token_expiry
        
        if access_valid and lottery_valid:
            access_remaining = self.access_token_expiry - current_time
            lottery_remaining = self.lottery_token_expiry - current_time
            logger.debug(f"🔑 Tokens valid - Access: {access_remaining:.0f}s, Lottery: {lottery_remaining:.0f}s")
            return True
        
        # Log which tokens are expired
        if not access_valid:
            logger.info("🔑 Access token expired, re-authenticating...")
        if not lottery_valid:
            logger.info("🎲 Lottery token expired, re-authenticating...")
        
        return self.login()

class LotteryPredictor:
    """Enhanced lottery predictor with Railway support and better error handling."""
    
    def __init__(self, max_history_size=100000):
        self.max_history_size = max_history_size
        self._data_lock = Lock()
        
        logger.info(f"🎯 Initializing LotteryPredictor with max history size: {max_history_size}")
        update_api_monitor_status("initializing", "Loading predictor data")
        
        # Load data from Redis keys with error handling
        try:
            self.history = self._load_from_redis(REDIS_HISTORY_KEY)
            self.predictions = self._load_from_redis(REDIS_PREDICTIONS_KEY)
            self.number_stats = self._load_from_redis(REDIS_NUMBER_STATS_KEY)
            self.trend_analysis = self._load_from_redis(REDIS_TREND_ANALYSIS_KEY)
            
            logger.info(f"📊 Loaded {len(self.history)} history entries, {len(self.predictions)} predictions")
            
        except Exception as e:
            logger.error(f"❌ Error loading initial data: {e}")
            self.history = {}
            self.predictions = {}
            self.number_stats = {}
            self.trend_analysis = {}
        
        # Initialize streaks
        self.streaks = {
            'number': {'current_win': 0, 'max_win': 0, 'current_loss': 0, 'max_loss': 0},
            'big_small': {'current_win': 0, 'max_win': 0, 'current_loss': 0, 'max_loss': 0},
            'color': {'current_win': 0, 'max_win': 0, 'current_loss': 0, 'max_loss': 0}
        }
        
        # Load persisted streaks
        try:
            persisted_streaks = self._load_from_redis("lottery:streaks")
            if persisted_streaks:
                for k, v in persisted_streaks.items():
                    if k in self.streaks:
                        self.streaks[k].update(v)
                logger.info("📈 Loaded persisted streak data")
        except Exception as e:
            logger.warning(f"⚠️ Could not load persisted streaks: {e}")

        # Ensure history is sorted descending on init
        self._ensure_history_desc_order()
        
        logger.info("✅ LotteryPredictor initialization complete")
        update_api_monitor_status("ready", "Predictor initialized successfully")

    def _ensure_history_desc_order(self):
        """Ensures the history dictionary is sorted by issue number in descending order."""
        try:
            with self._data_lock:
                sorted_items = sorted(self.history.items(), key=lambda x: int(x[0]), reverse=True)
                self.history = dict(sorted_items)
                logger.debug(f"📊 History sorted: {len(self.history)} entries")
        except Exception as e:
            logger.error(f"❌ Failed to sort history descending: {e}")

    def _load_from_redis(self, key):
        """Enhanced Redis data loading with error handling."""
        try:
            if not redis_client:
                logger.warning(f"⚠️ Redis client not available for key: {key}")
                return {}
            
            with redis_lock:
                data_str = redis_client.get(key)
                
            if not data_str:
                logger.debug(f"📭 No data found for Redis key: {key}")
                return {}
            
            # Handle both bytes and string data
            if isinstance(data_str, bytes):
                data_str = data_str.decode('utf-8')
            
            data = json.loads(data_str)
            logger.debug(f"📥 Loaded {len(data) if isinstance(data, (dict, list)) else 'data'} from {key}")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error for Redis key {key}: {e}")
            return {}
        except Exception as e:
            logger.error(f"❌ Error loading from Redis key {key}: {e}")
            return {}

    def _save_to_redis(self, key, data):
        """Enhanced Redis data saving with error handling."""
        try:
            if not redis_client:
                logger.warning(f"⚠️ Redis client not available, cannot save key: {key}")
                return False
            
            with redis_lock:
                json_data_str = json.dumps(data)
                redis_client.set(key, json_data_str)
                
            logger.debug(f"💾 Saved data to Redis key: {key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving data to Redis key {key}: {e}")
            return False

    def save_all(self):
        """Enhanced save all with error handling and status reporting."""
        try:
            logger.info("💾 Saving all predictor data to Redis...")
            
            save_results = {
                'history': self._save_to_redis(REDIS_HISTORY_KEY, self.history),
                'predictions': self._save_to_redis(REDIS_PREDICTIONS_KEY, self.predictions),
                'number_stats': self._save_to_redis(REDIS_NUMBER_STATS_KEY, self.number_stats),
                'trend_analysis': self._save_to_redis(REDIS_TREND_ANALYSIS_KEY, self.trend_analysis),
                'streaks': self._save_to_redis("lottery:streaks", self.streaks)
            }
            
            failed_saves = [key for key, success in save_results.items() if not success]
            
            if failed_saves:
                logger.warning(f"⚠️ Failed to save some data: {failed_saves}")
                update_api_monitor_status("partial_save", f"Failed to save: {failed_saves}")
            else:
                logger.info("✅ All predictor data saved successfully")
                update_api_monitor_status("data_saved", "All data saved successfully")
            
        except Exception as e:
            logger.error(f"💥 Error during save_all: {e}", exc_info=True)
            update_api_monitor_status("save_error", f"Save error: {str(e)}")

    def update_history(self, history_api_response):
        """Enhanced history update with detailed validation and logging."""
        try:
            logger.info("📥 Updating lottery history from API response...")
            
            if not history_api_response:
                logger.error("❌ Empty history API response")
                return
            
            if history_api_response.get("code") != 0:
                error_msg = history_api_response.get("message", "Unknown error")
                logger.error(f"❌ Invalid history API response - Code: {history_api_response.get('code')}, Message: {error_msg}")
                return

            data_list = history_api_response.get("data", {}).get("list", [])
            if not data_list:
                logger.warning("⚠️ No data list in history API response")
                return
            
            logger.info(f"📊 Processing {len(data_list)} history entries...")
            
            new_entries = 0
            updated_entries = 0
            invalid_entries = 0
            
            with self._data_lock:
                for entry in data_list:
                    try:
                        issue = entry.get("issueNumber")
                        number = entry.get("number")
                        
                        # Validate entry data
                        if not issue:
                            invalid_entries += 1
                            continue
                        
                        if number is None:
                            invalid_entries += 1
                            continue
                        
                        issue_str = str(issue)
                        number_int = int(number)
                        
                        # Validate number range
                        if not (0 <= number_int <= 9):
                            logger.warning(f"⚠️ Invalid number {number_int} for issue {issue_str}")
                            invalid_entries += 1
                            continue
                        
                        # Check if this is new or updated data
                        if issue_str in self.history:
                            if self.history[issue_str] != number_int:
                                logger.info(f"🔄 Updating issue {issue_str}: {self.history[issue_str]} → {number_int}")
                                self.history[issue_str] = number_int
                                updated_entries += 1
                        else:
                            self.history[issue_str] = number_int
                            new_entries += 1
                            
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Invalid entry data: {entry} - {e}")
                        invalid_entries += 1
                        continue

            # Post-processing
            if new_entries or updated_entries:
                self._ensure_history_desc_order()
                self._prune_history()
                
                total_changes = new_entries + updated_entries
                logger.info(f"✅ History updated: {new_entries} new, {updated_entries} updated, {invalid_entries} invalid")
                update_api_monitor_status("history_updated", f"{total_changes} entries processed")
            else:
                logger.info("📊 No new or updated history entries found")
                
        except Exception as e:
            logger.error(f"💥 Error updating history: {e}", exc_info=True)
            update_api_monitor_status("history_error", f"History update error: {str(e)}")

    def _prune_history(self):
        """Enhanced history pruning with detailed logging."""
        try:
            if len(self.history) <= self.max_history_size:
                logger.debug(f"📊 History size OK: {len(self.history)}/{self.max_history_size}")
                return
            
            logger.info(f"✂️ Pruning history: {len(self.history)} → {self.max_history_size}")
            
            with self._data_lock:
                # Sort by issue number (ascending for removal of oldest)
                sorted_issues = sorted(self.history.keys(), key=int)
                to_remove_count = len(self.history) - self.max_history_size
                
                removed_history = 0
                removed_predictions = 0
                
                for i in range(to_remove_count):
                    issue = sorted_issues[i]
                    if issue in self.history:
                        self.history.pop(issue, None)
                        removed_history += 1
                    if issue in self.predictions:
                        self.predictions.pop(issue, None)
                        removed_predictions += 1
                
                logger.info(f"✂️ Pruned {removed_history} history entries and {removed_predictions} predictions")
                
        except Exception as e:
            logger.error(f"❌ Error during history pruning: {e}")

    def update_trend_stats(self, trend_api_response):
        """Enhanced trend statistics update with validation."""
        try:
            logger.info("📊 Updating trend statistics from API response...")
            
            if not trend_api_response:
                logger.error("❌ Empty trend API response")
                return
                
            if trend_api_response.get("code") != 0:
                error_msg = trend_api_response.get("message", "Unknown error")
                logger.error(f"❌ Invalid trend API response - Code: {trend_api_response.get('code')}, Message: {error_msg}")
                return

            data_list = trend_api_response.get("data", [])
            if not data_list:
                logger.warning("⚠️ No data in trend API response")
                return
            
            logger.info(f"📈 Processing {len(data_list)} trend statistics...")
            
            updated_stats = 0
            
            with self._data_lock:
                for item in data_list:
                    try:
                        num = item.get("number")
                        if num is None:
                            continue
                        
                        num_str = str(num)
                        
                        # Validate number range
                        if not (0 <= num <= 9):
                            logger.warning(f"⚠️ Invalid trend number: {num}")
                            continue
                        
                        self.number_stats[num_str] = {
                            "missingCount": item.get("missingCount", 0),
                            "avgMissing": item.get("avgMissing", 0),
                            "openCount": item.get("openCount", 0),
                            "maxContinuous": item.get("maxContinuous", 0)
                        }
                        updated_stats += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Error processing trend item {item}: {e}")
                        continue
                
                # Store full trend analysis
                self.trend_analysis = trend_api_response
            
            logger.info(f"✅ Updated {updated_stats} trend statistics")
            update_api_monitor_status("trends_updated", f"{updated_stats} trend stats updated")
            
        except Exception as e:
            logger.error(f"💥 Error updating trend stats: {e}", exc_info=True)
            update_api_monitor_status("trends_error", f"Trend update error: {str(e)}")

    def calculate_likelihoods(self):
        """Enhanced likelihood calculation with detailed logging."""
        try:
            logger.debug("🎯 Calculating number likelihoods...")
            
            # Get recent draws for recency analysis
            recent_draws = sorted(self.history.items(), key=lambda item: int(item[0]), reverse=True)[:30]
            recent_numbers = [num for _, num in recent_draws]
            recency_counts = Counter(recent_numbers)
            
            likelihoods = []
            
            for num_str in map(str, range(10)):
                try:
                    num_int = int(num_str)
                    stats = self.number_stats.get(num_str, {})
                    
                    current_missing = stats.get("missingCount", 0)
                    avg_missing = stats.get("avgMissing", 10) or 10  # Prevent division by zero

                    # Calculate overdue score
                    overdue_score = 0.0
                    if current_missing > avg_missing * 1.5:
                        overdue_score = (current_missing / avg_missing) * 30.0
                    elif current_missing > avg_missing:
                        overdue_score = (current_missing / avg_missing) * 15.0

                    # Calculate recency score
                    recency_score = (recency_counts.get(num_int, 0) / 30) * 100 * 0.20  # 20% weight

                    # Combine scores
                    likelihood = 50.0 + overdue_score + recency_score
                    likelihood = min(likelihood, 99.0)  # Cap at 99%

                    # Determine status
                    status = "🟢 Normal"
                    if overdue_score > 30:
                        status = "🔴 Very Overdue"
                    elif recency_score > 10:
                        status = "🔥 Hot"

                    likelihoods.append({
                        "number": num_int,
                        "current_missing": current_missing,
                        "avg_missing": round(avg_missing, 1),
                        "likelihood": round(likelihood, 1),
                        "status": status,
                        "deviation": round(current_missing - avg_missing, 1)
                    })
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error calculating likelihood for number {num_str}: {e}")
                    continue
            
            # Sort by likelihood descending
            likelihoods.sort(key=lambda x: x["likelihood"], reverse=True)
            
            logger.debug(f"🎯 Calculated likelihoods for {len(likelihoods)} numbers")
            return likelihoods
            
        except Exception as e:
            logger.error(f"💥 Error calculating likelihoods: {e}", exc_info=True)
            return []

    def get_big_small_prediction(self, likelihoods):
        """Enhanced big/small prediction with validation."""
        try:
            if not likelihoods:
                logger.warning("⚠️ No likelihoods provided for big/small prediction")
                return "N/A", {"small_percentage": 0.0, "big_percentage": 0.0}
            
            small_like = sum(p['likelihood'] for p in likelihoods if p['number'] <= 4)
            big_like = sum(p['likelihood'] for p in likelihoods if p['number'] >= 5)
            total = small_like + big_like
            
            if total == 0:
                logger.warning("⚠️ Total likelihood is zero for big/small prediction")
                return "N/A", {"small_percentage": 0.0, "big_percentage": 0.0}
            
            predicted = "Small" if small_like >= big_like else "Big"
            percentages = {
                "small_percentage": round((small_like / total) * 100, 2),
                "big_percentage": round((big_like / total) * 100, 2)
            }
            
            logger.debug(f"📏 Big/Small prediction: {predicted} (Small: {percentages['small_percentage']}%, Big: {percentages['big_percentage']}%)")
            return predicted, percentages
            
        except Exception as e:
            logger.error(f"💥 Error in big/small prediction: {e}")
            return "N/A", {"small_percentage": 0.0, "big_percentage": 0.0}

    def get_color_prediction(self, likelihoods):
        """Enhanced color prediction with validation."""
        try:
            if not likelihoods:
                logger.warning("⚠️ No likelihoods provided for color prediction")
                return "N/A", {}
            
            color_scores = defaultdict(float)
            
            for p in likelihoods:
                num = p['number']
                likelihood = p['likelihood']
                color_info = COLOR_MAP.get(num, {})
                
                # Primary color gets full weight
                if color_info.get('primary'):
                    color_scores[color_info['primary']] += likelihood
                
                # Secondary color gets half weight
                if color_info.get('secondary'):
                    color_scores[color_info['secondary']] += likelihood * 0.5
            
            if not color_scores:
                logger.warning("⚠️ No color scores calculated")
                return "N/A", {}
            
            total = sum(color_scores.values())
            if total == 0:
                logger.warning("⚠️ Total color score is zero")
                return "N/A", {}
            
            percentages = {color: (score / total) * 100 for color, score in color_scores.items()}
            predicted = max(percentages, key=percentages.get)
            
            rounded_percentages = {color: round(p, 2) for color, p in percentages.items()}
            
            logger.debug(f"🎨 Color prediction: {predicted} ({rounded_percentages})")
            return predicted, rounded_percentages
            
        except Exception as e:
            logger.error(f"💥 Error in color prediction: {e}")
            return "N/A", {}

    def get_next_issue_number(self):
        """Enhanced next issue number calculation with validation."""
        try:
            if not self.history:
                logger.warning("⚠️ No history available to determine next issue")
                return None
            
            # Get all valid issue numbers
            valid_issues = []
            for issue_str in self.history.keys():
                try:
                    issue_int = int(issue_str)
                    valid_issues.append(issue_int)
                except ValueError:
                    logger.warning(f"⚠️ Invalid issue number format: {issue_str}")
                    continue
            
            if not valid_issues:
                logger.warning("⚠️ No valid issue numbers found")
                return None
            
            last_issue = max(valid_issues)
            next_issue = str(last_issue + 1)
            
            logger.debug(f"🔢 Next issue number: {next_issue}")
            return next_issue
            
        except Exception as e:
            logger.error(f"💥 Error determining next issue number: {e}")
            return None

    def record_prediction(self, issue_number, predicted_numbers, predicted_big_small, predicted_color, likelihoods):
        """Enhanced prediction recording with validation."""
        try:
            if issue_number is None:
                logger.warning("⚠️ No issue number provided for prediction recording")
                return
            
            issue_str = str(issue_number)
            
            with self._data_lock:
                if issue_str in self.predictions:
                    logger.info(f"📝 Prediction for issue {issue_str} already exists, skipping")
                    return
                
                # Build predicted likelihoods
                predicted_likelihoods = []
                for num in predicted_numbers:
                    for l in likelihoods:
                        if l["number"] == num:
                            predicted_likelihoods.append({
                                "number": num, 
                                "likelihood": l["likelihood"]
                            })
                            break
                
                self.predictions[issue_str] = {
                    "predicted_numbers": predicted_numbers,
                    "predicted_likelihoods": predicted_likelihoods,
                    "predicted_big_small": predicted_big_small,
                    "predicted_color": predicted_color,
                    "recorded_at": datetime.now(pytz.utc).isoformat()
                }
            
            logger.info(f"📝 Recorded prediction for issue {issue_str}: {predicted_numbers}")
            
        except Exception as e:
            logger.error(f"💥 Error recording prediction for issue {issue_number}: {e}")

    def update_streak(self, prediction_type, is_correct):
        """Enhanced streak update with validation."""
        try:
            if prediction_type not in self.streaks:
                logger.warning(f"⚠️ Unknown prediction type for streak: {prediction_type}")
                return
            
            streak = self.streaks[prediction_type]
            
            if is_correct:
                streak['current_win'] += 1
                streak['current_loss'] = 0
                if streak['current_win'] > streak['max_win']:
                    streak['max_win'] = streak['current_win']
                logger.debug(f"✅ {prediction_type} streak: Win {streak['current_win']}")
            else:
                streak['current_loss'] += 1
                streak['current_win'] = 0
                if streak['current_loss'] > streak['max_loss']:
                    streak['max_loss'] = streak['current_loss']
                logger.debug(f"❌ {prediction_type} streak: Loss {streak['current_loss']}")
                
        except Exception as e:
            logger.error(f"💥 Error updating streak for {prediction_type}: {e}")

    def verify_past_predictions(self):
        """Enhanced prediction verification with detailed logging."""
        try:
            logger.debug("🔍 Verifying past predictions against actual results...")
            
            # Get issues that need verification
            with self._data_lock:
                issues_to_verify = [
                    issue for issue in self.predictions.keys()
                    if issue in self.history and 'outcome' not in self.predictions[issue]
                ]
            
            if not issues_to_verify:
                logger.debug("🔍 No new predictions to verify")
                return
            
            logger.info(f"🔍 Verifying {len(issues_to_verify)} predictions...")
            
            verified_count = 0
            
            for issue in issues_to_verify:
                try:
                    pred = self.predictions[issue]
                    actual_num = self.history.get(issue)
                    
                    if actual_num is None:
                        continue
                    
                    # Number prediction verification
                    predicted_nums = pred.get("predicted_numbers", [])
                    if isinstance(predicted_nums, list):
                        correct_number = actual_num in predicted_nums
                    else:
                        correct_number = actual_num == predicted_nums
                    
                    # Big/Small verification
                    actual_bs = "Small" if actual_num <= 4 else "Big"
                    predicted_bs = pred.get("predicted_big_small")
                    correct_bs = (predicted_bs == actual_bs)
                    
                    # Color verification
                    actual_colors = [COLOR_MAP.get(actual_num, {}).get('primary')]
                    if COLOR_MAP.get(actual_num, {}).get('secondary'):
                        actual_colors.append(COLOR_MAP.get(actual_num, {}).get('secondary'))
                    
                    predicted_color = pred.get("predicted_color")
                    correct_color = predicted_color in actual_colors if predicted_color else False
                    
                    # Update streaks
                    self.update_streak('number', correct_number)
                    self.update_streak('big_small', correct_bs)
                    self.update_streak('color', correct_color)
                    
                    # Record outcome
                    with self._data_lock:
                        self.predictions[issue]['outcome'] = {
                            "correct_number": correct_number,
                            "correct_big_small": correct_bs,
                            "correct_color": correct_color,
                            "verified_at": datetime.now(pytz.utc).isoformat()
                        }
                    
                    verified_count += 1
                    
                    result_symbols = {
                        'number': '✅' if correct_number else '❌',
                        'big_small': '✅' if correct_bs else '❌',
                        'color': '✅' if correct_color else '❌'
                    }
                    
                    logger.debug(f"🔍 Issue {issue}: {result_symbols['number']}Num {result_symbols['big_small']}B/S {result_symbols['color']}Color")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error verifying prediction for issue {issue}: {e}")
                    continue
            
            if verified_count > 0:
                logger.info(f"✅ Verified {verified_count} predictions and updated streaks")
                update_api_monitor_status("predictions_verified", f"{verified_count} predictions verified")
            
        except Exception as e:
            logger.error(f"💥 Error during prediction verification: {e}", exc_info=True)

    def calculate_accuracy(self):
        """Enhanced accuracy calculation with detailed breakdown."""
        try:
            total_num = total_bs = total_color = 0
            correct_num = correct_bs = correct_color = 0
            
            for pred in self.predictions.values():
                outcome = pred.get("outcome")
                if not outcome:
                    continue
                
                total_num += 1
                total_bs += 1
                total_color += 1
                
                if outcome.get("correct_number"):
                    correct_num += 1
                if outcome.get("correct_big_small"):
                    correct_bs += 1
                if outcome.get("correct_color"):
                    correct_color += 1

            def calc_percent(correct, total):
                return (correct / total * 100) if total > 0 else 0.0

            accuracy_data = {
                "number_accuracy": f"{calc_percent(correct_num, total_num):.2f}% ({correct_num}/{total_num})",
                "big_small_accuracy": f"{calc_percent(correct_bs, total_bs):.2f}% ({correct_bs}/{total_bs})",
                "color_accuracy": f"{calc_percent(correct_color, total_color):.2f}% ({correct_color}/{total_color})"
            }
            
            logger.debug(f"📊 Accuracy - Num: {accuracy_data['number_accuracy']}, B/S: {accuracy_data['big_small_accuracy']}, Color: {accuracy_data['color_accuracy']}")
            return accuracy_data
            
        except Exception as e:
            logger.error(f"💥 Error calculating accuracy: {e}")
            return {
                "number_accuracy": "0.00% (0/0)",
                "big_small_accuracy": "0.00% (0/0)",
                "color_accuracy": "0.00% (0/0)"
            }

    def get_last_n_predictions(self, n=11):
        """Enhanced last N predictions retrieval with validation."""
        try:
            if not self.predictions:
                logger.debug("📭 No predictions available")
                return {}
            
            sorted_issues = sorted(self.predictions.keys(), key=int, reverse=True)
            last_n = {}
            
            count = 0
            for issue in sorted_issues:
                if count >= n:
                    break
                last_n[issue] = self.predictions[issue]
                count += 1
            
            logger.debug(f"📋 Retrieved last {len(last_n)} predictions")
            return last_n
            
        except Exception as e:
            logger.error(f"💥 Error getting last {n} predictions: {e}")
            return {}

    def sequence_analysis(self, pattern_lengths=[3, 2], top_patterns=2, top_predictions=2):
        """Enhanced sequence analysis with better error handling."""
        try:
            if not self.history:
                logger.debug("📭 No history for sequence analysis")
                return []
            
            sorted_issues = sorted(self.history.keys(), key=int)
            history_numbers = [self.history[issue] for issue in sorted_issues]
            
            if len(history_numbers) < max(pattern_lengths):
                logger.debug(f"📊 Insufficient history for sequence analysis: {len(history_numbers)} entries")
                return []
            
            results = []
            
            for plen in pattern_lengths:
                try:
                    if len(history_numbers) < plen:
                        continue
                    
                    recent_pattern = tuple(history_numbers[-plen:])
                    pattern_counts = []
                    
                    # Find all occurrences of this pattern
                    for i in range(len(history_numbers) - plen):
                        if tuple(history_numbers[i:i+plen]) == recent_pattern:
                            if i + plen < len(history_numbers):
                                next_num = history_numbers[i+plen]
                                pattern_counts.append(next_num)

                    if not pattern_counts:
                        continue

                    counter = Counter(pattern_counts)
                    total = sum(counter.values())
                    top_preds = counter.most_common(top_predictions)

                    predictions = []
                    for num, count in top_preds:
                        chance = round(100 * count / total, 1)
                        predictions.append({
                            "number": num, 
                            "count": count, 
                            "chance": chance
                        })

                    results.append({
                        "message": f"Found {total} occurrences.",
                        "pattern": list(recent_pattern),
                        "predictions": predictions
                    })

                    if len(results) >= top_patterns:
                        break
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error analyzing pattern length {plen}: {e}")
                    continue
            
            logger.debug(f"🔍 Sequence analysis found {len(results)} patterns")
            return results
            
        except Exception as e:
            logger.error(f"💥 Error in sequence analysis: {e}")
            return []

    def _build_prediction_history(self, limit=11):
        """Enhanced prediction history building with detailed validation."""
        try:
            history = []
            
            if not self.predictions:
                logger.debug("📭 No predictions for history building")
                return history
            
            sorted_issues = sorted(self.predictions.keys(), key=int, reverse=True)
            count = 0
            
            for issue in sorted_issues:
                if count >= limit:
                    break
                
                try:
                    pred = self.predictions.get(issue)
                    actual = self.history.get(issue)
                    
                    if not pred or actual is None:
                        continue
                    
                    outcome = pred.get('outcome', {})
                    pred_likelihoods = pred.get("predicted_likelihoods", [])
                    
                    # Build actual result data
                    actual_big_small = "Small" if actual <= 4 else "Big"
                    actual_colors = [COLOR_MAP.get(actual, {}).get('primary')]
                    if COLOR_MAP.get(actual, {}).get('secondary'):
                        actual_colors.append(COLOR_MAP.get(actual, {}).get('secondary'))
                    
                    history.append({
                        "issue": issue,
                        "prediction": {
                            "number": pred.get("predicted_numbers"),
                            "likelihoods": pred_likelihoods,
                            "big_small": pred.get("predicted_big_small"),
                            "color": pred.get("predicted_color")
                        },
                        "result": {
                            "number": actual,
                            "big_small": actual_big_small,
                            "colors": actual_colors
                        },
                        "correct": {
                            "number": outcome.get("correct_number"),
                            "big_small": outcome.get("correct_big_small"),
                            "color": outcome.get("correct_color")
                        }
                    })
                    count += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error building history for issue {issue}: {e}")
                    continue
            
            logger.debug(f"📋 Built prediction history with {len(history)} entries")
            return history
            
        except Exception as e:
            logger.error(f"💥 Error building prediction history: {e}")
            return []

    def generate_report(self):
        """Enhanced report generation with comprehensive error handling and status updates."""
        try:
            logger.info("📋 Generating comprehensive lottery prediction report...")
            update_api_monitor_status("generating_report", "Creating prediction report")
            send_api_monitor_heartbeat()
            
            # Calculate likelihoods
            logger.debug("🎯 Calculating likelihoods...")
            likelihoods = self.calculate_likelihoods()
            if not likelihoods:
                raise Exception("Failed to calculate likelihoods")
            
            # Get next issue
            next_issue = self.get_next_issue_number()
            if not next_issue:
                logger.warning("⚠️ Could not determine next issue number")
            
            # Generate predictions
            predicted_numbers = [p['number'] for p in likelihoods[:3]]  # Top 3 numbers
            predicted_bs, bs_details = self.get_big_small_prediction(likelihoods)
            predicted_color, color_details = self.get_color_prediction(likelihoods)
            
            logger.info(f"🎯 Generated predictions - Numbers: {predicted_numbers}, B/S: {predicted_bs}, Color: {predicted_color}")
            
            # Record prediction
            if next_issue:
                self.record_prediction(next_issue, predicted_numbers, predicted_bs, predicted_color, likelihoods)
            
            # Verify past predictions and update streaks
            self.verify_past_predictions()
            
            # Calculate accuracy
            accuracy = self.calculate_accuracy()
            
            # Build comprehensive report
            report = {
                "next_issue": next_issue,
                "prediction": {
                    "numbers": predicted_numbers,
                    "predicted_likelihoods": [
                        {
                            "number": num, 
                            "likelihood": next((l["likelihood"] for l in likelihoods if l["number"] == num), 0)
                        }
                        for num in predicted_numbers
                    ],
                    "big_small": predicted_bs,
                    "big_small_percentages": bs_details,
                    "color": predicted_color,
                    "color_percentages": color_details
                },
                "likelihoods": likelihoods,
                "accuracy": accuracy,
                "streaks": self.streaks,
                "recent_results": dict(sorted(self.history.items(), key=lambda item: int(item[0]), reverse=True)[:10]),
                "past_predictions": self.get_last_n_predictions(11),
                "prediction_history": self._build_prediction_history(limit=11),
                "sequence_analysis": self.sequence_analysis(),
                "report_metadata": {
                    "generated_at": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "history_entries": len(self.history),
                    "predictions_count": len(self.predictions),
                    "service_name": get_railway_service_name()
                }
            }
            
            logger.info("✅ Comprehensive report generated successfully")
            update_api_monitor_status("report_ready", "Report generated successfully")
            
            return report
            
        except Exception as e:
            logger.error(f"💥 Error generating report: {e}", exc_info=True)
            update_api_monitor_status("report_error", f"Report generation failed: {str(e)}")
            
            # Return minimal error report
            return {
                "status": "error",
                "message": f"Report generation failed: {str(e)}",
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "next_issue": None,
                "prediction": {
                    "numbers": [],
                    "predicted_likelihoods": [],
                    "big_small": "N/A",
                    "big_small_percentages": {},
                    "color": "N/A",
                    "color_percentages": {}
                },
                "likelihoods": [],
                "accuracy": {
                    "number_accuracy": "0.00% (0/0)",
                    "big_small_accuracy": "0.00% (0/0)",
                    "color_accuracy": "0.00% (0/0)"
                },
                "streaks": self.streaks,
                "recent_results": {},
                "past_predictions": {},
                "prediction_history": [],
                "sequence_analysis": []
            }

# --- Enhanced API Fetch Functions ---
def get_history(token_manager):
    """Enhanced history fetching with better error handling."""
    try:
        logger.info("📥 Fetching lottery draw history...")
        update_api_monitor_status("fetching_history", "Requesting history data")
        
        url = DRAW_HISTORY_URL
        params = {
            "ts": int(time.time() * 1000)
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": common_headers["User-Agent"],
            "Referer": f"https://damanworld.mobi/#/saasLottery/WinGo?gameCode={GAME_CODE}&lottery=WinGo"
        }
        
        response = make_request("GET", url, headers=headers, params=params, retries=2)
        
        if response:
            logger.info("✅ History data fetched successfully")
            update_api_monitor_status("history_fetched", "History data received")
        else:
            logger.error("❌ Failed to fetch history data")
            update_api_monitor_status("history_error", "Failed to fetch history")
        
        return response
        
    except Exception as e:
        logger.error(f"💥 Error in get_history: {e}", exc_info=True)
        update_api_monitor_status("history_error", f"History fetch error: {str(e)}")
        return None

def get_trends(token_manager):
    """Enhanced trend fetching with better error handling."""
    try:
        logger.info("📊 Fetching lottery trend statistics...")
        update_api_monitor_status("fetching_trends", "Requesting trend data")
        
        url = TREND_STAT_URL
        params = {
            "gameCode": GAME_CODE,
            "pageNo": 1,
            "pageSize": 10,
            "language": "en",
            "random": DAMAN_TREND_RANDOM,
            "signature": DAMAN_TREND_SIGNATURE,
            "timestamp": int(time.time())
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": common_headers["User-Agent"],
            "Referer": f"https://damanworld.mobi/#/saasLottery/WinGo?gameCode={GAME_CODE}&lottery=WinGo"
        }
        
        # Add authorization if available
        if token_manager and token_manager.lottery_token:
            headers["Authorization"] = f"Bearer {token_manager.lottery_token}"
            logger.debug("🔐 Added authorization header for trends request")
        
        response = make_request("GET", url, headers=headers, params=params, retries=2)
        
        if response:
            logger.info("✅ Trend data fetched successfully")
            update_api_monitor_status("trends_fetched", "Trend data received")
        else:
            logger.error("❌ Failed to fetch trend data")
            update_api_monitor_status("trends_error", "Failed to fetch trends")
        
        return response
        
    except Exception as e:
        logger.error(f"💥 Error in get_trends: {e}", exc_info=True)
        update_api_monitor_status("trends_error", f"Trends fetch error: {str(e)}")
        return None

def get_prediction_data(predictor, token_manager):
    """Enhanced main orchestration function with comprehensive error handling."""
    try:
        logger.info("🚀 Starting prediction data orchestration...")
        update_api_monitor_status("starting", "Beginning prediction cycle")
        send_api_monitor_heartbeat()
        
        # Validate inputs
        if not predictor:
            raise ValueError("Predictor instance is required")
        if not token_manager:
            raise ValueError("Token manager instance is required")
        
        # Ensure valid authentication
        logger.info("🔐 Validating authentication...")
        if not token_manager.ensure_valid_tokens():
            logger.error("❌ Authentication failed in get_prediction_data")
            update_api_monitor_status("auth_failed", "Authentication validation failed")
            return {
                "status": "error", 
                "message": "Authentication failed",
                "timestamp": datetime.now(pytz.utc).isoformat()
            }
        
        logger.info("✅ Authentication validated successfully")
        
        # Fetch history data
        logger.info("📥 Fetching history data...")
        history_resp = get_history(token_manager)
        if history_resp:
            predictor.update_history(history_resp)
            logger.info("✅ History data updated successfully")
        else:
            logger.warning("⚠️ Failed to fetch history data - continuing with cached data")
        
        # Fetch trend data
        logger.info("📊 Fetching trend data...")
        trend_resp = get_trends(token_manager)
        if trend_resp:
            predictor.update_trend_stats(trend_resp)
            logger.info("✅ Trend data updated successfully")
        else:
            logger.warning("⚠️ Failed to fetch trend data - continuing with cached data")
        
        # Generate comprehensive report
        logger.info("📋 Generating prediction report...")
        report = predictor.generate_report()
        
        if not report or report.get("status") == "error":
            raise Exception("Report generation failed or returned error status")
        
        # Save all data to persistence
        logger.info("💾 Saving predictor data...")
        predictor.save_all()
        
        # Finalize report
        report["status"] = "success"
        report["timestamp"] = datetime.now(pytz.utc).isoformat()
        
        logger.info("✅ Prediction data orchestration completed successfully")
        update_api_monitor_status("completed", "Prediction cycle completed successfully")
        
        return report
        
    except Exception as e:
        logger.error(f"💥 Error in get_prediction_data orchestration: {e}", exc_info=True)
        update_api_monitor_status("error", f"Orchestration error: {str(e)}")
        
        return {
            "status": "error",
            "message": f"Prediction data orchestration failed: {str(e)}",
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "error_details": {
                "environment": "railway" if is_railway_environment() else "local",
                "service_name": get_railway_service_name()
            }
        }

# Graceful shutdown handling
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    signal_name = signal.Signals(signum).name
    logger.info(f"🛑 Received {signal_name} signal in api_monitor")
    update_api_monitor_status("shutting_down", f"Received {signal_name} signal")
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Module initialization
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 LOTTERY API MONITOR MODULE - STANDALONE MODE")
    logger.info("=" * 60)
    
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
    logger.info(f"🔗 Redis status: {'Connected' if redis_client else 'Disconnected'}")
    
    if not USERNAME or not PASSWORD:
        logger.critical("💥 Missing required credentials. Set DAMAN_USERNAME and DAMAN_PASSWORD.")
        sys.exit(1)
    
    try:
        # Test initialization
        logger.info("🧪 Testing module initialization...")
        predictor = LotteryPredictor()
        token_manager = TokenManager(USERNAME, PASSWORD)
        
        logger.info("✅ Module initialization successful")
        logger.info("📋 Running single prediction cycle...")
        
        result = get_prediction_data(predictor, token_manager)
        
        if result.get("status") == "success":
            logger.info("✅ Standalone prediction cycle completed successfully")
            print(json.dumps(result, indent=2))
        else:
            logger.error("❌ Standalone prediction cycle failed")
            print(f"Error: {result.get('message', 'Unknown error')}")
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"💥 Critical error in standalone mode: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🏁 API Monitor standalone execution complete")
        update_api_monitor_status("standalone_complete", "Standalone execution finished")

else:
    # Module imported
    logger.info("📦 API Monitor module imported successfully")
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"🔗 Redis: {'Connected' if redis_client else 'Disconnected'}")
    update_api_monitor_status("imported", "Module imported and ready")
