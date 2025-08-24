from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import logging
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import redis
import time

# Load environment variables from .env file
load_dotenv()

# Railway configuration path
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")

# --- Import simulator blueprint ---
from simulator_api import simulator_bp

# Initialize Flask app
app = Flask(__name__)

# Railway-specific configurations
app.config['PORT'] = int(os.environ.get('PORT', 5000))

# --- Redis Keys (Color and Size ALL) ---
REDIS_PREDICTION_KEY = "latest_prediction_data"  # (legacy/compat, from api_monitor)
REDIS_AI_PREDICTION_KEY = "lottery:ai_prediction"
REDIS_AI_FLAG_KEY = "lottery:ai_enabled"
REDIS_AI_BIG_SMALL_ACCURACY_KEY = "lottery:ai_big_small_accuracy"

# Color
REDIS_COLOR_PREDICTION_KEY = "lottery:color_prediction"
REDIS_COLOR_PREDICTION_LOG_KEY = "lottery:color_prediction_log"
REDIS_COLOR_RULE_HISTORY_KEY = "lottery:color_rule_history"
REDIS_COLOR_ACCURACY_KEY = "lottery:color_accuracy"
REDIS_COLOR_STREAKS_KEY = "lottery:color_streaks"
REDIS_CONTROLLED_STREAKS_KEY = "lottery:controlled_streaks"
REDIS_COLOR_PREDICTION_HISTORY_KEY = "lottery:color_prediction_history"

# Size/BigSmall
REDIS_SIZE_PREDICTION_KEY = "lottery:size_prediction"
REDIS_SIZE_PREDICTION_LOG_KEY = "lottery:size_prediction_log"
REDIS_SIZE_PREDICTION_HISTORY_KEY = "lottery:size_prediction_history"
REDIS_SIZE_ACCURACY_KEY = "lottery:size_accuracy"
REDIS_SIZE_STREAKS_KEY = "lottery:size_streaks"
REDIS_CONTROLLED_SIZE_STREAKS_KEY = "lottery:controlled_size_streaks"

REDIS_RESET_POINT = "lottery:streak_reset_point"

def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_domain():
    """Get Railway public domain"""
    return os.getenv('RAILWAY_PUBLIC_DOMAIN', 'localhost')

# --- CORS Setup with Railway Support ---
allowed_origins = [
    "https://prediction.up.railway.app",
    "https://workforakng.github.io",
    "http://localhost:3000",
    "http://127.0.0.1:5000"
]

# Add Railway domain if available
if is_railway_environment():
    railway_domain = get_railway_domain()
    if railway_domain != 'localhost':
        allowed_origins.append(f"https://{railway_domain}")
        allowed_origins.append(f"http://{railway_domain}")

CORS(app, resources={
    r"/api/*": {
        "origins": allowed_origins,
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# --- Logging Setup with Railway Support ---
os.makedirs(DATA_DIR, exist_ok=True)
log_file_path = os.path.join(DATA_DIR, "app.log")

app.logger.setLevel(logging.INFO)

# File handler for persistent logging
if os.access(DATA_DIR, os.W_OK):
    file_handler = logging.FileHandler(log_file_path)
    file_formatter = logging.Formatter('%(asctime)s - APP - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    app.logger.addHandler(file_handler)
    app.logger.info(f"App logging to: {log_file_path}")
else:
    app.logger.warning(f"Cannot write to {DATA_DIR}, skipping file logging")

# Stream handler for Railway logs
stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter('%(asctime)s - APP - %(levelname)s - %(message)s')
stream_handler.setFormatter(stream_formatter)
app.logger.addHandler(stream_handler)

# --- Enhanced Redis Connection for Railway ---
app.redis_client = None

def initialize_redis():
    """Initialize Redis connection with enhanced compatibility"""
    global app
    try:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        
        # Add family=0 for Railway IPv6 compatibility if needed
        if "railway.internal" in redis_url and "family=" not in redis_url:
            separator = "&" if "?" in redis_url else "?"
            redis_url = f"{redis_url}"
        
        app.redis_client = redis.from_url(
            redis_url, 
            decode_responses=True, 
            socket_connect_timeout=15,  # Increased timeout
            socket_timeout=15,          # Increased timeout
            retry_on_timeout=True,
            health_check_interval=30,
            max_connections=20          # Connection pool for multiple workers
        )
        app.redis_client.ping()
        app.logger.info(f"✅ Successfully connected to Redis: {redis_url[:30]}...")
        return True
    except redis.exceptions.ConnectionError as e:
        app.logger.critical(f"❌ Could not connect to Redis: {e}")
        app.redis_client = None
        return False
    except redis.exceptions.TimeoutError as e:
        app.logger.critical(f"❌ Redis connection timeout: {e}")
        app.redis_client = None
        return False
    except Exception as e:
        app.logger.critical(f"❌ Unexpected Redis error: {e}")
        app.redis_client = None
        return False

def safe_redis_operation(operation_func, *args, **kwargs):
    """Execute Redis operations with retry logic"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if not app.redis_client:
                if not initialize_redis():
                    raise redis.exceptions.ConnectionError("Redis client not available")
            
            return operation_func(*args, **kwargs)
            
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            retry_count += 1
            app.logger.warning(f"Redis operation failed (attempt {retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)  # Exponential backoff
                app.redis_client = None  # Force reconnection
            else:
                app.logger.error("Redis operation failed after all retries")
                raise e
        except Exception as e:
            app.logger.error(f"Unexpected error in Redis operation: {e}")
            raise e
    
    return None

# --- AI Flag Management Functions ---
def is_ai_enabled_app():
    """Checks if the AI prediction feature is enabled in Redis."""
    try:
        if not app.redis_client:
            return False
        return app.redis_client.exists(REDIS_AI_FLAG_KEY)
    except Exception as e:
        app.logger.error(f"Error checking AI flag: {e}")
        return False

def set_ai_enabled_app(enabled: bool):
    """Sets or unsets the AI prediction enabled flag in Redis."""
    try:
        if not app.redis_client:
            app.logger.error("Redis client not available for AI flag operation")
            return False
            
        if enabled:
            app.redis_client.set(REDIS_AI_FLAG_KEY, "1")
            app.logger.info("AI flag set to ENABLED in Redis by app.py.")
        else:
            app.redis_client.delete(REDIS_AI_FLAG_KEY)
            app.logger.info("AI flag set to DISABLED (removed from Redis by app.py).")
        return True
    except Exception as e:
        app.logger.error(f"Error setting AI flag: {e}")
        return False

# --- Register Blueprints ---
app.register_blueprint(simulator_bp, url_prefix="/api/simulator")

# ------------- ROUTES BELOW -------------

@app.route('/')
def home():
    """Main dashboard page"""
    try:
        return render_template("index.html")
    except Exception as e:
        app.logger.error(f"Error rendering index.html: {e}")
        return jsonify({
            "status": "error",
            "message": "Template not found. Please ensure index.html exists in templates folder."
        }), 500

@app.route('/mixpred/')
@app.route('/mixpred/index.html')
def mixpred_page():
    """Renders the mixed predictions insights page."""
    try:
        return render_template('mixpred.html')
    except Exception as e:
        app.logger.error(f"Error rendering mixpred.html: {e}")
        return jsonify({
            "status": "error",
            "message": "Mixed predictions template not found. Please ensure mixpred.html exists in templates folder."
        }), 500

@app.route('/health')
def health_check():
    """Health check endpoint for Railway"""
    try:
        redis_status = "disconnected"
        redis_error = None
        
        if app.redis_client:
            try:
                app.redis_client.ping()
                redis_status = "connected"
            except Exception as e:
                redis_status = "error"
                redis_error = str(e)
                # Try to reconnect if Redis fails
                if initialize_redis():
                    redis_status = "reconnected"
                    redis_error = None
        else:
            # Try to initialize Redis if not connected
            if initialize_redis():
                redis_status = "connected"
        
        health_data = {
            "status": "healthy" if redis_status in ["connected", "reconnected"] else "degraded",
            "redis": redis_status,
            "environment": "railway" if is_railway_environment() else "local",
            "port": app.config.get('PORT', 5000),
            "timestamp": datetime.now().isoformat()
        }
        
        if redis_error:
            health_data["redis_error"] = redis_error
        
        status_code = 200 if redis_status in ["connected", "reconnected"] else 503
        return jsonify(health_data), status_code
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/redis/status')
def redis_status():
    """Get detailed Redis connection status"""
    try:
        if not app.redis_client:
            return jsonify({
                "status": "disconnected",
                "connected": False,
                "timestamp": datetime.now().isoformat()
            }), 503
        
        # Test connection with timing
        start_time = datetime.now()
        app.redis_client.ping()
        end_time = datetime.now()
        latency = (end_time - start_time).total_seconds() * 1000
        
        # Get Redis info
        info = app.redis_client.info()
        
        return jsonify({
            "status": "connected",
            "connected": True,
            "latency_ms": round(latency, 2),
            "redis_version": info.get("redis_version", "unknown"),
            "connected_clients": info.get("connected_clients", 0),
            "used_memory_human": info.get("used_memory_human", "unknown"),
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "connected": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 503

@app.route('/api/prediction', methods=['GET'])
def get_current_prediction():
    """Get main prediction data"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                app.logger.error("Redis client not available for prediction fetch")
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available",
                    "ai_enabled": False
                }), 503

        raw_data = app.redis_client.get(REDIS_PREDICTION_KEY)
        if not raw_data:
            app.logger.info(f"No data found in Redis key: {REDIS_PREDICTION_KEY}")
            return jsonify({
                "status": "initializing",
                "message": "Main prediction data not yet available. Please wait for the worker to run.",
                "ai_enabled": is_ai_enabled_app()
            }), 503

        data = json.loads(raw_data)
        data["ai_enabled"] = is_ai_enabled_app()
        app.logger.info(f"Successfully served main prediction data from {REDIS_PREDICTION_KEY}.")
        return jsonify(data), 200

    except redis.exceptions.ConnectionError:
        app.logger.error("Redis connection error during /api/prediction fetch.")
        return jsonify({
            "status": "error",
            "message": "Redis connection error while fetching main prediction.",
            "ai_enabled": False
        }), 500

    except json.JSONDecodeError:
        app.logger.error(f"Invalid JSON data in Redis for key: {REDIS_PREDICTION_KEY}.")
        return jsonify({
            "status": "error",
            "message": "Corrupted main prediction data in storage.",
            "ai_enabled": is_ai_enabled_app()
        }), 500

    except Exception as e:
        app.logger.error(f"An unexpected error occurred in /api/prediction: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"An unexpected server error occurred: {str(e)}",
            "ai_enabled": is_ai_enabled_app()
        }), 500

@app.route('/api/ai_prediction', methods=['GET'])
def api_ai_prediction():
    """Get AI prediction data"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available"
                }), 503

        raw_ai_prediction = app.redis_client.get(REDIS_AI_PREDICTION_KEY)
        if raw_ai_prediction:
            try:
                ai_data = json.loads(raw_ai_prediction)
                app.logger.info(f"Served AI prediction data from {REDIS_AI_PREDICTION_KEY}.")
                return jsonify(ai_data), 200
            except json.JSONDecodeError:
                app.logger.error(f"Invalid JSON data in Redis for AI prediction key: {REDIS_AI_PREDICTION_KEY}.")
                return jsonify({
                    "status": "error",
                    "message": "Corrupted AI prediction data in storage."
                }), 500
        else:
            app.logger.info(f"No data found in Redis key: {REDIS_AI_PREDICTION_KEY}. AI prediction not yet generated.")
            return jsonify({
                "status": "no_data",
                "message": "AI prediction not yet available or AI is disabled."
            }), 200

    except redis.exceptions.ConnectionError:
        app.logger.error("Redis connection error during /api/ai_prediction access.")
        return jsonify({
            "status": "error",
            "message": "Redis connection error while fetching AI prediction."
        }), 500
    except Exception as e:
        app.logger.error(f"An unexpected error occurred in /api/ai_prediction: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"An unexpected server error occurred: {str(e)}"
        }), 500

@app.route('/api/ai_big_small_accuracy', methods=['GET'])
def api_ai_big_small_accuracy():
    """Get AI Big/Small accuracy data"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available"
                }), 503

        raw_accuracy_data = app.redis_client.get(REDIS_AI_BIG_SMALL_ACCURACY_KEY)
        if raw_accuracy_data:
            try:
                accuracy_data = json.loads(raw_accuracy_data)
                app.logger.info(f"Served AI Big/Small accuracy data from {REDIS_AI_BIG_SMALL_ACCURACY_KEY}.")
                return jsonify(accuracy_data), 200
            except json.JSONDecodeError:
                app.logger.error(f"Invalid JSON data in Redis for AI Big/Small accuracy key: {REDIS_AI_BIG_SMALL_ACCURACY_KEY}.")
                return jsonify({
                    "status": "error",
                    "message": "Corrupted AI Big/Small accuracy data in storage."
                }), 500
        else:
            app.logger.info(f"No data found in Redis key: {REDIS_AI_BIG_SMALL_ACCURACY_KEY}. AI Big/Small accuracy not yet generated.")
            return jsonify({
                "status": "no_data",
                "message": "AI Big/Small accuracy data not yet available."
            }), 200
    except redis.exceptions.ConnectionError:
        app.logger.error("Redis connection error during /api/ai_big_small_accuracy access.")
        return jsonify({
            "status": "error",
            "message": "Redis connection error while fetching AI Big/Small accuracy."
        }), 500
    except Exception as e:
        app.logger.error(f"An unexpected error occurred in /api/ai_big_small_accuracy: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"An unexpected server error occurred: {str(e)}"
        }), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get application status"""
    last_updated_timestamp = "N/A"
    data_status = "unknown"
    message = "N/A"

    try:
        if not app.redis_client:
            if not initialize_redis():
                data_status = "redis_unavailable"
                message = "Redis client not available"
        
        if app.redis_client:
            json_data_str = app.redis_client.get(REDIS_PREDICTION_KEY)
            if json_data_str:
                data = json.loads(json_data_str)
                last_updated_timestamp = data.get("timestamp", "N/A")
                data_status = data.get("status", "unknown")
                message = data.get("message", "N/A")
            else:
                data_status = "no_main_prediction_data"
                message = "Main prediction worker has not yet stored data."
    except redis.exceptions.ConnectionError:
        data_status = "redis_connection_error"
        message = "Failed to connect to Redis to get main prediction status."
        app.logger.error("Redis connection error during /api/status check.")
    except json.JSONDecodeError:
        data_status = "parsing_error"
        message = "Could not parse main prediction data from Redis."
        app.logger.error(f"Error parsing status from Redis key {REDIS_PREDICTION_KEY}.")
    except Exception as e:
        data_status = "server_error"
        message = f"An unexpected error occurred: {str(e)}"
        app.logger.error(f"An unexpected error occurred in /api/status: {e}", exc_info=True)

    return jsonify({
        "service_status": "running",
        "last_main_prediction_update_timestamp": last_updated_timestamp,
        "main_prediction_data_status": data_status,
        "main_prediction_message": message,
        "ai_enabled_flag": is_ai_enabled_app(),
        "environment": "railway" if is_railway_environment() else "local",
        "redis_available": app.redis_client is not None
    }), 200

@app.route('/api/ai/status', methods=['GET'])
def api_ai_status():
    """Get AI status"""
    return jsonify({
        "ai_enabled_flag": is_ai_enabled_app(),
        "redis_available": app.redis_client is not None
    }), 200

@app.route('/api/ai/start', methods=['POST'])
def api_ai_start():
    """Start AI prediction"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available",
                    "ai_enabled_flag": False
                }), 503

        success = set_ai_enabled_app(True)
        if success:
            app.logger.info("✅ AI prediction ENABLED by API request.")
            return jsonify({
                "status": "success",
                "message": "AI prediction enabled.",
                "ai_enabled_flag": True
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to enable AI prediction",
                "ai_enabled_flag": False
            }), 500
    except Exception as e:
        app.logger.error(f"Error enabling AI: {e}")
        return jsonify({
            "status": "error",
            "message": f"Failed to enable AI: {str(e)}",
            "ai_enabled_flag": False
        }), 500

@app.route('/api/ai/stop', methods=['POST'])
def api_ai_stop():
    """Stop AI prediction"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available",
                    "ai_enabled_flag": False
                }), 503

        success = set_ai_enabled_app(False)
        if success:
            app.logger.info("🛑 AI prediction DISABLED by API request.")
            return jsonify({
                "status": "success",
                "message": "AI prediction disabled.",
                "ai_enabled_flag": False
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to disable AI prediction",
                "ai_enabled_flag": is_ai_enabled_app()
            }), 500
    except Exception as e:
        app.logger.error(f"Error disabling AI: {e}")
        return jsonify({
            "status": "error",
            "message": f"Failed to disable AI: {str(e)}",
            "ai_enabled_flag": is_ai_enabled_app()
        }), 500

@app.route('/api/color_insights', methods=['GET'])
def get_color_insights():
    """Get color prediction insights"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available"
                }), 503

        raw_pred = app.redis_client.get(REDIS_COLOR_PREDICTION_KEY)
        color_prediction = json.loads(raw_pred) if raw_pred else None

        raw_acc = app.redis_client.get(REDIS_COLOR_ACCURACY_KEY)
        color_accuracy = json.loads(raw_acc) if raw_acc else None

        # ✅ Trim to top 10 most-used rules (by total)
        if color_accuracy and "per_rule" in color_accuracy:
            top_rules = dict(
                sorted(
                    color_accuracy["per_rule"].items(),
                    key=lambda item: item[1].get("total", 0),
                    reverse=True
                )[:10]
            )
            color_accuracy["per_rule"] = top_rules

        raw_streaks = app.redis_client.get(REDIS_COLOR_STREAKS_KEY)
        color_streaks = json.loads(raw_streaks) if raw_streaks else None

        raw_history = app.redis_client.get(REDIS_COLOR_PREDICTION_HISTORY_KEY)
        color_prediction_history = json.loads(raw_history) if raw_history else []

        return jsonify({
            "color_prediction": color_prediction,
            "color_accuracy": color_accuracy,
            "color_streaks": color_streaks,
            "color_prediction_history": color_prediction_history
        }), 200

    except Exception as e:
        app.logger.error(f"Error in /api/color_insights: {e}")
        return jsonify({
            "status": "error",
            "message": f"Failed to get color insights: {str(e)}"
        }), 500

@app.route('/api/size_insights', methods=['GET'])
def get_size_insights():
    """Get size prediction insights"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available"
                }), 503

        raw_pred = app.redis_client.get(REDIS_SIZE_PREDICTION_KEY)
        size_prediction = json.loads(raw_pred) if raw_pred else None

        raw_acc = app.redis_client.get(REDIS_SIZE_ACCURACY_KEY)
        size_accuracy = json.loads(raw_acc) if raw_acc else None

        if size_accuracy and "per_rule" in size_accuracy:
            top_rules = dict(
                sorted(
                    size_accuracy["per_rule"].items(),
                    key=lambda item: item[1].get("total", 0),
                    reverse=True
                )[:10]
            )
            size_accuracy["per_rule"] = top_rules

        raw_streaks = app.redis_client.get(REDIS_SIZE_STREAKS_KEY)
        size_streaks = json.loads(raw_streaks) if raw_streaks else None

        raw_history = app.redis_client.get(REDIS_SIZE_PREDICTION_HISTORY_KEY)
        size_prediction_history = json.loads(raw_history) if raw_history else []

        return jsonify({
            "size_prediction": size_prediction,
            "size_accuracy": size_accuracy,
            "size_streaks": size_streaks,
            "size_prediction_history": size_prediction_history
        }), 200

    except Exception as e:
        app.logger.error(f"Error in /api/size_insights: {e}")
        return jsonify({
            "status": "error",
            "message": f"Failed to get size insights: {str(e)}"
        }), 500

@app.route('/api/v2/insights', methods=['GET'])
def get_v2_insights():
    """Get combined color and size insights"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available"
                }), 503

        # Fetch color data
        color_response, color_status = get_color_insights()
        if color_status != 200:
            return color_response, color_status
        color_data = color_response.get_json()

        # Fetch size data
        size_response, size_status = get_size_insights()
        if size_status != 200:
            return size_response, size_status
        size_data = size_response.get_json()

        # Combine both datasets
        combined_data = {**color_data, **size_data}
        return jsonify(combined_data), 200
        
    except Exception as e:
        app.logger.error(f"Error in /api/v2/insights: {e}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/admin/reset-streaks', methods=['POST'])
def reset_all_streaks():
    """Reset all streak and accuracy data from a specific issue (URL param only)"""
    try:
        if not app.redis_client:
            if not initialize_redis():
                return jsonify({
                    "status": "error",
                    "message": "Database connection not available"
                }), 503

        # Get issue number from URL parameter only
        reset_issue = request.args.get('issue')
        
        # Default to latest issue if none provided
        if not reset_issue:
            latest_pred_raw = app.redis_client.get(REDIS_COLOR_PREDICTION_KEY)
            reset_issue = json.loads(latest_pred_raw).get("issue") if latest_pred_raw else None
            
            if not reset_issue:
                return jsonify({
                    "status": "error",
                    "message": "Prediction not yet initialized and no issue specified. Cannot reset streaks."
                }), 400

        # Validate issue format (optional)
        if not str(reset_issue).isdigit() and len(str(reset_issue)) < 10:
            return jsonify({
                "status": "error",
                "message": f"Invalid issue format: {reset_issue}. Expected numeric issue ID."
            }), 400

        # Set the reset point
        app.redis_client.set(REDIS_RESET_POINT, str(reset_issue))

        # Create blank streak and accuracy objects
        current_timestamp = datetime.now().isoformat()
        
        blank_streaks = {
            "current_win_streak": 0,
            "current_lose_streak": 0,
            "max_win_streak": 0,
            "max_lose_streak": 0,
            "win_streak_distribution": {},
            "lose_streak_distribution": {},
            "timestamp": current_timestamp,
            "reset_from_issue": str(reset_issue)
        }
        
        blank_accuracy = {
            "total_predictions": 0,
            "correct_predictions": 0,
            "accuracy_percentage": 0.0,
            "per_rule": {},
            "timestamp": current_timestamp,
            "reset_from_issue": str(reset_issue)
        }
        
        # Reset both color and size streaks & accuracy
        app.redis_client.set(REDIS_COLOR_STREAKS_KEY, json.dumps(blank_streaks))
        app.redis_client.set(REDIS_CONTROLLED_STREAKS_KEY, json.dumps(blank_streaks))
        app.redis_client.set(REDIS_COLOR_ACCURACY_KEY, json.dumps(blank_accuracy))
        app.redis_client.set(REDIS_SIZE_STREAKS_KEY, json.dumps(blank_streaks))
        app.redis_client.set(REDIS_CONTROLLED_SIZE_STREAKS_KEY, json.dumps(blank_streaks))
        app.redis_client.set(REDIS_SIZE_ACCURACY_KEY, json.dumps(blank_accuracy))
        
        app.logger.info(f"✅ /admin/reset-streaks → Color & Size data reset from issue {reset_issue}.")
        
        return jsonify({
            "status": "success",
            "message": f"✅ Color & Size streaks and accuracy data have been reset from issue {reset_issue}.",
            "reset_from_issue": str(reset_issue),
            "timestamp": current_timestamp
        }), 200
        
    except Exception as e:
        app.logger.exception("❌ Error in /admin/reset-streaks")
        return jsonify({
            "status": "error",
            "message": f"Failed to reset streaks: {str(e)}"
        }), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "status": "error",
        "message": "Endpoint not found",
        "available_endpoints": [
            "/",
            "/mixpred/",
            "/health",
            "/api/redis/status",
            "/api/prediction",
            "/api/ai_prediction",
            "/api/ai_big_small_accuracy",
            "/api/status",
            "/api/ai/status",
            "/api/ai/start",
            "/api/ai/stop",
            "/api/color_insights",
            "/api/size_insights",
            "/api/v2/insights",
            "/admin/reset-streaks"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    app.logger.error(f"Internal server error: {error}")
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "timestamp": datetime.now().isoformat()
    }), 500

# Initialize the application with app context (Flask 2.3+ compatible)
with app.app_context():
    app.logger.info(f"🚀 Lottery Prediction App initialized")
    app.logger.info(f"Environment: {'Railway' if is_railway_environment() else 'Local'}")
    
    # Initialize Redis connection
    redis_initialized = initialize_redis()
    app.logger.info(f"Redis Status: {'Connected' if redis_initialized else 'Disconnected'}")
    app.logger.info(f"Port: {app.config.get('PORT', 5000)}")

# Entry point for Railway deployment
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    
    app.logger.info(f"🚀 Starting Lottery Prediction App on port {port}")
    app.logger.info(f"Environment: {'Railway' if is_railway_environment() else 'Local'}")
    app.logger.info(f"Debug mode: {debug_mode}")
    app.logger.info(f"Redis available: {app.redis_client is not None}")
    
    app.run(host='0.0.0.0', port=port, debug=debug_mode, threaded=True)
