# worker.py
import time
import logging
import os
import json
import signal
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import redis
import pytz
from threading import Event

# Load environment variables
load_dotenv()

# Define logging directory with Railway support
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")

# Railway environment detection
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'main-worker')

# Setup enhanced logging for Railway
os.makedirs(DATA_DIR, exist_ok=True)
log_file_path = os.path.join(DATA_DIR, "worker.log")

# Configure logging with Railway-specific formatting
log_format = '%(asctime)s - WORKER - %(levelname)s - %(message)s'
handlers = [logging.StreamHandler()]  # Always include stream handler for Railway logs

# Add file handler if writable directory exists
if os.access(DATA_DIR, os.W_OK):
    try:
        file_handler = logging.FileHandler(log_file_path)
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
    logger.info(f"🚂 Running on Railway - Service: {get_railway_service_name()}")
    logger.info(f"📁 Data directory: {DATA_DIR}")
else:
    logger.info("🏠 Running in local environment")

# Global shutdown event for graceful shutdown
shutdown_event = Event()

# Enhanced Redis connection with Railway support and Upstash compatibility
redis_client = None

def initialize_redis():
    """Initialize Redis connection with retry logic and Railway/Upstash support"""
    global redis_client
    
    # Get Redis URL from environment - Railway should provide this
    redis_url = os.getenv("REDIS_URL")
    
    if not redis_url:
        logger.critical("💥 REDIS_URL environment variable not set!")
        logger.critical("💡 Please set REDIS_URL in Railway environment variables")
        return False
    
    logger.info(f"🔗 Using Redis URL: {redis_url[:30]}...")
    
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Add family=0 for Railway IPv6 compatibility if needed
            working_redis_url = redis_url
            if "railway.internal" in redis_url and "family=" not in redis_url:
                separator = "&" if "?" in redis_url else "?"
                working_redis_url = f"{redis_url}{separator}family=0"
            
            redis_client = redis.from_url(
                working_redis_url, 
                decode_responses=True,
                socket_connect_timeout=15,  # Increased timeout for external Redis
                socket_timeout=15,          # Increased timeout for external Redis
                retry_on_timeout=True,
                health_check_interval=30,
                max_connections=10          # Connection pool
            )
            
            # Test connection
            redis_client.ping()
            logger.info(f"✅ Successfully connected to Redis (attempt {attempt + 1})")
            
            # Get Redis info for debugging
            try:
                info = redis_client.info()
                logger.info(f"📊 Redis version: {info.get('redis_version', 'unknown')}")
                logger.info(f"📊 Connected clients: {info.get('connected_clients', 0)}")
                logger.info(f"📊 Used memory: {info.get('used_memory_human', 'unknown')}")
            except Exception as info_error:
                logger.warning(f"⚠️ Could not get Redis info: {info_error}")
            
            return True
            
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Redis connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.critical(f"💥 Failed to connect to Redis after {max_retries} attempts")
                logger.critical(f"💡 Check if REDIS_URL is correct: {redis_url[:50]}...")
                return False
                
        except redis.exceptions.TimeoutError as e:
            logger.error(f"❌ Redis timeout (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.critical(f"💥 Redis timeout after {max_retries} attempts")
                return False
                
        except Exception as e:
            logger.error(f"❌ Unexpected Redis error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.critical(f"💥 Unexpected Redis error after {max_retries} attempts")
                return False
    
    return False

def safe_redis_operation(operation_func, *args, **kwargs):
    """Execute Redis operations with retry logic"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if not redis_client:
                if not initialize_redis():
                    raise redis.exceptions.ConnectionError("Redis client not available")
            
            return operation_func(*args, **kwargs)
            
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            retry_count += 1
            logger.warning(f"Redis operation failed (attempt {retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)  # Exponential backoff
                # Force reconnection
                if not initialize_redis():
                    continue
            else:
                logger.error("Redis operation failed after all retries")
                raise e
                
        except Exception as e:
            logger.error(f"Unexpected error in Redis operation: {e}")
            raise e
    
    return None

# Redis keys
REDIS_PREDICTION_KEY = "latest_prediction_data"
REDIS_AI_TRIGGER_CHANNEL = "lottery:ai_trigger"
REDIS_WORKER_STATUS_KEY = "lottery:worker_status"
REDIS_WORKER_HEARTBEAT_KEY = "lottery:worker_heartbeat"

# Graceful shutdown handling
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    signal_name = signal.Signals(signum).name
    logger.info(f"🛑 Received {signal_name} signal. Initiating graceful shutdown...")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def update_worker_status(status, message=None):
    """Update worker status in Redis with safe operation"""
    try:
        status_data = {
            "status": status,
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "environment": "railway" if is_railway_environment() else "local",
            "service_name": get_railway_service_name()
        }
        if message:
            status_data["message"] = message
        
        def _set_status():
            return redis_client.set(REDIS_WORKER_STATUS_KEY, json.dumps(status_data), ex=300)
        
        safe_redis_operation(_set_status)
        logger.debug(f"📊 Worker status updated: {status}")
        
    except Exception as e:
        logger.error(f"Failed to update worker status: {e}")

def send_heartbeat():
    """Send heartbeat to Redis with safe operation"""
    try:
        heartbeat_data = {
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "environment": "railway" if is_railway_environment() else "local",
            "service_name": get_railway_service_name()
        }
        
        def _set_heartbeat():
            return redis_client.set(REDIS_WORKER_HEARTBEAT_KEY, json.dumps(heartbeat_data), ex=120)
        
        safe_redis_operation(_set_heartbeat)
        logger.debug("💗 Heartbeat sent")
        
    except Exception as e:
        logger.error(f"Failed to send heartbeat: {e}")

def run_prediction_cycle(predictor, token_manager):
    """
    Runs a single prediction cycle: fetch, update, predict, write to Redis.
    Enhanced with better error handling and Railway support.
    """
    cycle_start = time.time()
    report = None
    
    try:
        logger.info("🔄 Starting prediction cycle...")
        update_worker_status("running", "Executing prediction cycle")
        
        # Check for shutdown signal
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during prediction cycle")
            return False
        
        # Validate token manager
        if not token_manager.ensure_valid_tokens():
            raise Exception("Authentication failed - tokens invalid")
        
        logger.info("📥 Fetching latest history data...")
        history_resp = get_history(token_manager)
        if history_resp:
            predictor.update_history(history_resp)
            logger.info("✅ History data updated successfully")
        else:
            logger.warning("⚠️  Failed to fetch history data")
        
        # Check for shutdown again
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during history fetch")
            return False
        
        logger.info("📊 Fetching latest trend statistics...")
        trend_resp = get_trends(token_manager)
        if trend_resp:
            predictor.update_trend_stats(trend_resp)
            logger.info("✅ Trend statistics updated successfully")
        else:
            logger.warning("⚠️  Failed to fetch trend data")
        
        # Check for shutdown again
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during trend fetch")
            return False
        
        # Generate prediction report
        logger.info("🎯 Generating prediction report...")
        report = predictor.generate_report()
        
        if not report:
            raise Exception("Generated report is None or empty")
        
        # Enhance report with metadata
        report["status"] = "success"
        report["timestamp"] = datetime.now(pytz.utc).isoformat()
        report["worker_environment"] = "railway" if is_railway_environment() else "local"
        report["generation_time_seconds"] = round(time.time() - cycle_start, 2)
        
        # Save to Redis with safe operation
        try:
            def _save_prediction():
                return redis_client.set(REDIS_PREDICTION_KEY, json.dumps(report))
            
            safe_redis_operation(_save_prediction)
            logger.info("✅ Main prediction data successfully saved to Redis")
            
        except Exception as redis_error:
            logger.error(f"❌ Failed to save prediction to Redis: {redis_error}")
            raise redis_error
        
        # Save persistent data
        try:
            predictor.save_all()
            logger.info("✅ Persistent data saved successfully")
        except Exception as save_error:
            logger.error(f"⚠️  Failed to save persistent data: {save_error}")
            # Don't raise - this is not critical for current prediction
        
        # Trigger AI worker via pub/sub with safe operation
        if "next_issue" in report:
            try:
                next_issue = report["next_issue"]
                ai_trigger_message = json.dumps({
                    "trigger_type": "new_issue_prediction_needed",
                    "issue": next_issue,
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "source": "main_worker"
                })
                
                def _publish_ai_trigger():
                    return redis_client.publish(REDIS_AI_TRIGGER_CHANNEL, ai_trigger_message)
                
                safe_redis_operation(_publish_ai_trigger)
                logger.info(f"🤖 AI trigger published for issue {next_issue}")
                
            except Exception as ai_error:
                logger.error(f"⚠️  Failed to publish AI trigger: {ai_error}")
                # Don't raise - this is not critical for main prediction
        else:
            logger.warning("⚠️  No 'next_issue' in report - cannot trigger AI")
        
        update_worker_status("idle", "Prediction cycle completed successfully")
        logger.info(f"✅ Prediction cycle completed successfully in {time.time() - cycle_start:.2f} seconds")
        return True
        
    except Exception as e:
        duration = time.time() - cycle_start
        logger.error(f"❌ Prediction cycle failed after {duration:.2f} seconds: {e}", exc_info=True)
        
        # Save error state to Redis with safe operation
        error_report = {
            "status": "error",
            "message": "Worker exception during prediction cycle",
            "details": str(e),
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "worker_environment": "railway" if is_railway_environment() else "local",
            "error_duration_seconds": round(duration, 2)
        }
        
        try:
            def _save_error():
                return redis_client.set(REDIS_PREDICTION_KEY, json.dumps(error_report))
            
            safe_redis_operation(_save_error)
            logger.info("📝 Error state saved to Redis")
            
        except Exception as redis_error:
            logger.error(f"❌ Failed to save error state to Redis: {redis_error}")
        
        update_worker_status("error", f"Prediction cycle failed: {str(e)}")
        return False

def calculate_next_run_time():
    """Calculate the next run time (58th second of current or next minute)"""
    now_utc = datetime.now(pytz.utc)
    current_sec = now_utc.second
    
    # Target 58th second of current minute, or next minute if past 58th second
    target_time = now_utc.replace(second=58, microsecond=0)
    if current_sec >= 58:
        target_time += timedelta(minutes=1)
    
    return target_time, (target_time - now_utc).total_seconds()

def main_worker_loop():
    """Main worker loop with enhanced error handling and Railway support"""
    logger.info("🚀 Worker loop starting...")
    update_worker_status("starting", "Worker initialization complete")
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    try:
        while not shutdown_event.is_set():
            try:
                # Calculate next run time
                target_time, sleep_duration = calculate_next_run_time()
                current_time = datetime.now(pytz.utc)
                
                logger.info(
                    f"⏰ Current: {current_time.strftime('%H:%M:%S')} UTC | "
                    f"Next run: {target_time.strftime('%H:%M:%S')} UTC | "
                    f"Sleep: {sleep_duration:.1f}s"
                )
                
                update_worker_status("waiting", f"Next run at {target_time.strftime('%H:%M:%S')} UTC")
                
                # Sleep with periodic heartbeats and shutdown checks
                heartbeat_interval = 30  # seconds
                elapsed = 0
                
                while elapsed < sleep_duration and not shutdown_event.is_set():
                    sleep_chunk = min(heartbeat_interval, sleep_duration - elapsed)
                    if shutdown_event.wait(timeout=sleep_chunk):
                        break
                    elapsed += sleep_chunk
                    send_heartbeat()
                
                # Check if shutdown was requested during sleep
                if shutdown_event.is_set():
                    logger.info("🛑 Shutdown requested during sleep")
                    break
                
                # Execute prediction cycle
                current_utc = datetime.now(pytz.utc)
                logger.info(f"🎯 Executing prediction cycle at {current_utc.strftime('%H:%M:%S')} UTC")
                
                success = run_prediction_cycle(predictor, token_manager)
                
                if success:
                    consecutive_failures = 0
                    logger.info("✅ Prediction cycle completed successfully")
                else:
                    consecutive_failures += 1
                    logger.error(f"❌ Prediction cycle failed (consecutive failures: {consecutive_failures})")
                    
                    # If too many consecutive failures, exit
                    if consecutive_failures >= max_consecutive_failures:
                        logger.critical(f"💥 {max_consecutive_failures} consecutive failures. Exiting.")
                        update_worker_status("critical", f"{max_consecutive_failures} consecutive failures")
                        break
                
                # Small buffer to prevent immediate re-calculation
                time.sleep(2)
                
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"❌ Unexpected error in worker loop: {e}", exc_info=True)
                update_worker_status("error", f"Worker loop error: {str(e)}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.critical(f"💥 {max_consecutive_failures} consecutive errors. Exiting.")
                    break
                
                # Wait before retrying
                time.sleep(30)
    
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
    except Exception as e:
        logger.critical(f"💥 Critical error in main worker loop: {e}", exc_info=True)
        update_worker_status("critical", f"Critical worker error: {str(e)}")
    finally:
        logger.info("🏁 Worker loop ended")
        update_worker_status("stopped", "Worker loop terminated")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 LOTTERY PREDICTION MAIN WORKER STARTING")
    logger.info("=" * 60)
    
    # Environment validation
    USERNAME = os.getenv("DAMAN_USERNAME")
    PASSWORD = os.getenv("DAMAN_PASSWORD")
    REDIS_URL = os.getenv("REDIS_URL")
    
    logger.info(f"🔐 Username: {USERNAME}")
    logger.info(f"🔗 Redis URL: {REDIS_URL[:50] if REDIS_URL else 'NOT SET'}...")
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"📂 Data Directory: {DATA_DIR}")
    
    if not USERNAME or not PASSWORD:
        logger.critical("💥 DAMAN_USERNAME and/or DAMAN_PASSWORD not set. Exiting.")
        sys.exit(1)
    
    if not REDIS_URL:
        logger.critical("💥 REDIS_URL environment variable not set. Exiting.")
        logger.critical("💡 Please set REDIS_URL in Railway environment variables")
        sys.exit(1)
    
    # Initialize Redis connection
    logger.info("🔌 Initializing Redis connection...")
    if not initialize_redis():
        logger.critical("💥 Failed to initialize Redis connection. Exiting.")
        sys.exit(1)
    
    # Initialize prediction components
    try:
        logger.info("🔧 Initializing prediction components...")
        
        # Import after environment is validated
        from api_monitor import LotteryPredictor, TokenManager, get_history, get_trends
        
        predictor = LotteryPredictor()
        token_manager = TokenManager(USERNAME, PASSWORD)
        
        logger.info("✅ Prediction components initialized successfully")
        
        # Test authentication
        logger.info("🔐 Testing authentication...")
        if not token_manager.ensure_valid_tokens():
            logger.critical("💥 Authentication test failed. Check credentials.")
            sys.exit(1)
        logger.info("✅ Authentication test passed")
        
    except ImportError as e:
        logger.critical(f"💥 Failed to import required modules: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"💥 Failed to initialize components: {e}", exc_info=True)
        sys.exit(1)
    
    # Start main worker loop 
    logger.info("🎯 Starting main worker loop...")
    logger.info(f"📅 Scheduled to run every minute at 58th second")
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"🔗 Redis: Connected to {REDIS_URL[:30]}...")
    
    try:
        main_worker_loop()
    except Exception as e:
        logger.critical(f"💥 Fatal error in main execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🏁 Worker shutdown complete")
        update_worker_status("shutdown", "Worker process terminated")
