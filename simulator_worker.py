# simulator_worker.py
import os
import json
import time
import logging
import signal
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import redis
import pytz
from threading import Event, Lock

# Load environment variables
load_dotenv()

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'simulator_worker')

# --- Enhanced Logging Setup ---
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.join(DATA_DIR, "simulator_worker.log")

# Configure logging with Railway support
log_format = '%(asctime)s - SIMULATOR_WORKER - %(levelname)s - %(message)s'
handlers = [logging.StreamHandler()]

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
logger = logging.getLogger('simulator_worker')

# Log environment info
if is_railway_environment():
    logger.info(f"🚂 Simulator Worker running on Railway - Service: {get_railway_service_name()}")
else:
    logger.info("🏠 Simulator Worker running in local environment")

# Global shutdown event
shutdown_event = Event()
redis_lock = Lock()

# --- Redis Keys for 1-minute cycle ---
REDIS_PREDICTION_KEY = "latest_prediction_data"
REDIS_SIMULATOR_KEY = "lottery:simulator_state"
LOTTERY_HISTORY_KEY = "lottery:history"

# New 1-minute cycle prediction keys
REDIS_COLOR_PREDICTION_KEY = "lottery:color_prediction"
REDIS_SIZE_PREDICTION_KEY = "lottery:size_prediction"

# Status tracking
REDIS_SIMULATOR_WORKER_STATUS_KEY = "lottery:simulator_worker_status"
REDIS_SIMULATOR_WORKER_HEARTBEAT_KEY = "lottery:simulator_worker_heartbeat"

# --- Enhanced Redis Connection ---
redis_client = None

def initialize_redis():
    """Initialize Redis with retry logic"""
    global redis_client
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    max_retries = 5
    retry_delay = 5
    
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
            logger.info(f"✅ Simulator Worker connected to Redis (attempt {attempt + 1})")
            return True
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ Redis connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.critical(f"💥 Failed to connect to Redis after {max_retries} attempts")
                return False
    return False

if not initialize_redis():
    logger.critical("❌ Could not initialize Redis. Exiting.")
    sys.exit(1)

# --- Configuration for 1-minute cycle ---
SERVICE_FEE_PERCENTAGE = 0.02
SERVICE_FEE_MULTIPLIER = 1 - SERVICE_FEE_PERCENTAGE
CYCLE_DURATION = 60  # 1 minute in seconds
HEARTBEAT_INTERVAL = 30  # seconds

# --- Status Management ---
def update_simulator_worker_status(status, message=None):
    """Update worker status in Redis"""
    try:
        if redis_client:
            with redis_lock:
                status_data = {
                    "status": status,
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "service_name": get_railway_service_name(),
                    "cycle_duration": CYCLE_DURATION
                }
                if message:
                    status_data["message"] = message
                
                redis_client.set(REDIS_SIMULATOR_WORKER_STATUS_KEY, json.dumps(status_data), ex=300)
                logger.debug(f"📊 Simulator Worker status: {status}")
    except Exception as e:
        logger.error(f"Failed to update worker status: {e}")

def send_simulator_worker_heartbeat():
    """Send heartbeat to Redis"""
    try:
        if redis_client:
            with redis_lock:
                heartbeat_data = {
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "environment": "railway" if is_railway_environment() else "local",
                    "service_name": get_railway_service_name(),
                    "cycle_duration": CYCLE_DURATION,
                    "last_execution": datetime.now(pytz.utc).isoformat()
                }
                redis_client.set(REDIS_SIMULATOR_WORKER_HEARTBEAT_KEY, json.dumps(heartbeat_data), ex=120)
                logger.debug("💗 Simulator Worker heartbeat sent")
    except Exception as e:
        logger.error(f"Failed to send heartbeat: {e}")

# --- Enhanced Game Logic for WinGo 1-minute ---
def get_wingo_color(digit):
    """Get WinGo color for 1-minute game"""
    if digit in [1, 3, 7, 9]:
        return "Green"
    elif digit in [2, 4, 6, 8]:
        return "Red"
    elif digit in [0, 5]:
        return "Violet"
    return "Unknown"

def get_wingo_big_small(digit):
    """Get WinGo big/small for 1-minute game"""
    if digit in [0, 1, 2, 3, 4]:
        return "Small"
    elif digit in [5, 6, 7, 8, 9]:
        return "Big"
    return "Unknown"

def get_wingo_payout_factor(bet_type, predicted_value, actual_digit):
    """
    Enhanced payout calculation for 1-minute WinGo with proper violet handling
    """
    try:
        if bet_type == "color":
            actual_color = get_wingo_color(actual_digit)
            if predicted_value == actual_color:
                if predicted_value == "Green" and actual_digit == 5:
                    return 1.5  # Green 5 special case
                elif predicted_value == "Red" and actual_digit == 0:
                    return 1.5  # Red 0 special case
                elif predicted_value == "Violet":  # Violet hits on 0 or 5
                    return 4.5
                else:  # Standard color win
                    return 2.0
            return 0  # Loss
        elif bet_type == "big_small":
            actual_bs = get_wingo_big_small(actual_digit)
            if predicted_value == actual_bs:
                return 2.0
            return 0
        elif bet_type == "number":
            if predicted_value == actual_digit:
                return 9.0
            return 0
        return 0
    except Exception as e:
        logger.error(f"Error calculating payout: {e}")
        return 0

def get_internal_prediction_big_small(num1, num2):
    """Enhanced internal logic for big/small prediction"""
    try:
        total_sum = num1 + num2
        while total_sum > 9:
            total_sum = sum(int(digit) for digit in str(total_sum))

        if 0 <= total_sum <= 4:
            return "Small"
        elif 5 <= total_sum <= 9:
            return "Big"
        else:
            return "Small"  # Fallback
    except Exception as e:
        logger.error(f"Error in internal big/small prediction: {e}")
        return "Small"

# --- Enhanced Main Simulation Function for 1-minute cycle ---
def run_simulator_step():
    """
    Enhanced simulator step for 1-minute lottery cycle with Railway support
    """
    try:
        logger.info("🔄 Running simulator step for 1-minute cycle...")
        update_simulator_worker_status("running", "Executing 1-minute simulation step")
        send_simulator_worker_heartbeat()
        
        # Check for shutdown
        if shutdown_event.is_set():
            logger.info("🛑 Shutdown requested during simulation step")
            return
        
        # Get simulator state
        with redis_lock:
            simulator_state_str = redis_client.get(REDIS_SIMULATOR_KEY)
        
        if not simulator_state_str:
            logger.debug("📭 Simulator state not found. Waiting for initialization via API.")
            update_simulator_worker_status("waiting", "Waiting for simulator initialization")
            return

        simulator_state = json.loads(simulator_state_str)

        # Check if simulator is running
        if not simulator_state.get("is_running", False):
            logger.debug("⏸️ Simulator is stopped. Skipping simulation logic.")
            update_simulator_worker_status("stopped", "Simulator is not running")
            return

        # --- Fetch lottery history for 1-minute cycle ---
        with redis_lock:
            history_data_str = redis_client.get(LOTTERY_HISTORY_KEY)
        
        if not history_data_str:
            logger.warning("⚠️ Lottery history not available. Cannot simulate.")
            update_simulator_worker_status("warning", "No lottery history available")
            return

        history = json.loads(history_data_str)
        
        if not history or len(history) < 1:
            logger.warning("⚠️ Insufficient historical data for simulation.")
            update_simulator_worker_status("warning", "Insufficient history data")
            return

        # Get the latest issue number and result
        latest_issues = sorted([int(k) for k in history.keys()], reverse=True)
        if not latest_issues:
            logger.warning("⚠️ No valid issues in history")
            return
        
        latest_issue = str(latest_issues[0])
        actual_outcome_digit = history[latest_issue]
        
        if actual_outcome_digit is None or not isinstance(actual_outcome_digit, int):
            logger.warning(f"⚠️ Invalid outcome digit from history: {actual_outcome_digit}")
            return

        # Check if already simulated this issue
        if simulator_state.get("last_simulated_issue") == latest_issue:
            logger.debug(f"📊 Already simulated issue {latest_issue}. Skipping.")
            return

        logger.info(f"🎯 Simulating issue {latest_issue} with outcome {actual_outcome_digit}")

        # --- Get predictions based on simulation mode ---
        main_app_predicted_color = None
        main_app_predicted_big_small = None
        main_app_predicted_number = None

        simulation_mode = simulator_state.get("simulation_mode", "main_app_prediction")
        
        if simulation_mode == "main_app_prediction":
            # Use main app predictions
            with redis_lock:
                prediction_data_str = redis_client.get(REDIS_PREDICTION_KEY)
            
            if prediction_data_str:
                try:
                    prediction_data = json.loads(prediction_data_str)
                    # Extract predictions from main app data structure
                    pred_section = prediction_data.get('prediction', {})
                    main_app_predicted_color = pred_section.get('color')
                    main_app_predicted_big_small = pred_section.get('big_small')
                    if pred_section.get('numbers'):
                        main_app_predicted_number = pred_section['numbers'][0]  # First predicted number
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing main app prediction: {e}")
        
        elif simulation_mode == "color_size_prediction":
            # Use separate color and size predictions
            with redis_lock:
                color_pred_str = redis_client.get(REDIS_COLOR_PREDICTION_KEY)
                size_pred_str = redis_client.get(REDIS_SIZE_PREDICTION_KEY)
            
            if color_pred_str:
                try:
                    color_data = json.loads(color_pred_str)
                    main_app_predicted_color = color_data.get('next_color')
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing color prediction: {e}")
            
            if size_pred_str:
                try:
                    size_data = json.loads(size_pred_str)
                    predicted_size = size_data.get('next_size')
                    main_app_predicted_big_small = "Big" if predicted_size == "Big" else "Small"
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing size prediction: {e}")

        # Increment simulation counters
        simulator_state["total_simulated_rounds"] = simulator_state.get("total_simulated_rounds", 0) + 1
        strategies = simulator_state.get("bet_strategies", {})
        session_stats = simulator_state.setdefault("session_stats", {})
        
        history_entries_for_this_round = []

        # --- Process each betting strategy ---
        for strategy_key, strategy_state in strategies.items():
            if not strategy_state.get("is_active"):
                continue

            # Check wallet depletion
            if simulator_state["total_wallet"] <= 0:
                logger.warning(f"💸 Total wallet depleted. Stopping all strategies.")
                break

            # Calculate bet amount with progression
            base_bet = strategy_state["base_bet"]
            bet_multipliers = strategy_state.get("bet_multipliers", [1.0])
            current_progression_step = strategy_state["current_progression_step"]

            # Handle progression bounds
            if current_progression_step >= len(bet_multipliers):
                calculated_bet_amount = base_bet * bet_multipliers[0]
                strategy_state["current_progression_step"] = 0
                logger.warning(f"📈 Max progression reached for {strategy_key}. Resetting to base bet.")
            else:
                calculated_bet_amount = base_bet * bet_multipliers[current_progression_step]

            strategy_state["current_bet"] = calculated_bet_amount

            # Check insufficient funds for this strategy
            if simulator_state["total_wallet"] < calculated_bet_amount:
                logger.warning(f"💰 Insufficient funds for {strategy_key}. Disabling strategy.")
                strategy_state["is_active"] = False
                
                simulation_entry = {
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "issue": latest_issue,
                    "bet_type": strategy_key,
                    "message": f"Strategy DISABLED: Insufficient funds for {strategy_key}.",
                    "wallet_after_bet": round(simulator_state["total_wallet"], 2)
                }
                history_entries_for_this_round.append(simulation_entry)
                continue

            # --- Determine predicted value based on strategy and mode ---
            predicted_value = None
            bet_type_name = None

            if strategy_key == "color_bet_strategy":
                bet_type_name = "color"
                if strategy_state.get("auto_target", False) and main_app_predicted_color:
                    predicted_value = main_app_predicted_color
                elif simulation_mode == "internal_logic":
                    predicted_value = strategy_state.get("target_color", "Green")
                else:
                    predicted_value = main_app_predicted_color or strategy_state.get("target_color", "Green")
                    
            elif strategy_key == "big_small_bet_strategy":
                bet_type_name = "big_small"
                if strategy_state.get("auto_target", False) and main_app_predicted_big_small:
                    predicted_value = main_app_predicted_big_small
                elif simulation_mode == "internal_logic":
                    # Use internal logic with previous outcomes
                    if len(latest_issues) >= 2:
                        second_latest_issue = str(latest_issues[1])
                        second_last_outcome = history.get(second_latest_issue)
                        if second_last_outcome is not None:
                            predicted_value = get_internal_prediction_big_small(second_last_outcome, actual_outcome_digit)
                        else:
                            predicted_value = strategy_state.get("target_side", "Big")
                    else:
                        predicted_value = strategy_state.get("target_side", "Big")
                else:
                    predicted_value = main_app_predicted_big_small or strategy_state.get("target_side", "Big")
                    
            elif strategy_key == "number_bet_strategy":
                bet_type_name = "number"
                if strategy_state.get("auto_target", False) and main_app_predicted_number is not None:
                    predicted_value = main_app_predicted_number
                else:
                    predicted_value = strategy_state.get("target_number", 0)

            if predicted_value is None:
                logger.warning(f"⚠️ No valid prediction for {strategy_key}. Skipping bet.")
                continue

            # --- Calculate payout and update wallet ---
            payout_factor = get_wingo_payout_factor(bet_type_name, predicted_value, actual_outcome_digit)
            
            wallet_change = -calculated_bet_amount  # Bet amount always deducted
            result = "Loss"
            
            # Update session stats
            session_stats["total_bets_placed"] = session_stats.get("total_bets_placed", 0) + 1
            strategy_state["total_wagered"] = strategy_state.get("total_wagered", 0) + calculated_bet_amount

            if payout_factor > 0:  # Win
                winnings_before_fee = calculated_bet_amount * payout_factor
                actual_winnings_after_fee = winnings_before_fee * SERVICE_FEE_MULTIPLIER
                wallet_change = actual_winnings_after_fee  # Net gain
                simulator_state["total_wallet"] += actual_winnings_after_fee
                
                # Reset progression on win
                strategy_state["current_progression_step"] = 0
                strategy_state["current_bet"] = base_bet * bet_multipliers[0] if bet_multipliers else base_bet
                
                result = "Win"
                strategy_state["current_win_streak"] += 1
                strategy_state["current_loss_streak"] = 0
                strategy_state["max_win_streak"] = max(strategy_state["max_win_streak"], strategy_state["current_win_streak"])
                strategy_state["total_winnings"] = strategy_state.get("total_winnings", 0) + actual_winnings_after_fee
                
                session_stats["total_wins"] = session_stats.get("total_wins", 0) + 1
                
            else:  # Loss
                simulator_state["total_wallet"] -= calculated_bet_amount
                strategy_state["current_progression_step"] += 1
                
                result = "Loss"
                strategy_state["current_loss_streak"] += 1
                strategy_state["current_win_streak"] = 0
                strategy_state["max_loss_streak"] = max(strategy_state["max_loss_streak"], strategy_state["current_loss_streak"])
                
                session_stats["total_losses"] = session_stats.get("total_losses", 0) + 1

            strategy_state["rounds_played"] = strategy_state.get("rounds_played", 0) + 1
            strategy_state["last_updated"] = datetime.now(pytz.utc).isoformat()

            # Record detailed history entry
            simulation_entry = {
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "issue": latest_issue,
                "actual_outcome_digit": actual_outcome_digit,
                "bet_type": bet_type_name,
                "predicted_value": predicted_value,
                "bet_amount": round(calculated_bet_amount, 2),
                "payout_factor": payout_factor,
                "wallet_change": round(wallet_change, 2),
                "current_wallet": round(simulator_state["total_wallet"], 2),
                "result": result,
                "strategy_key": strategy_key,
                "strategy_win_streak": strategy_state["current_win_streak"],
                "strategy_loss_streak": strategy_state["current_loss_streak"],
                "progression_step": strategy_state["current_progression_step"],
                "simulation_mode": simulation_mode
            }
            history_entries_for_this_round.append(simulation_entry)

        # Update session statistics
        session_stats["net_profit_loss"] = simulator_state["total_wallet"] - simulator_state["initial_wallet"]
        session_stats["best_wallet"] = max(session_stats.get("best_wallet", simulator_state["initial_wallet"]), simulator_state["total_wallet"])
        session_stats["worst_wallet"] = min(session_stats.get("worst_wallet", simulator_state["initial_wallet"]), simulator_state["total_wallet"])

        # Append round history
        simulator_state["prediction_history"].extend(history_entries_for_this_round)

        # Limit history size
        max_history = 500  # Increased for 1-minute cycle
        if len(simulator_state["prediction_history"]) > max_history:
            simulator_state["prediction_history"] = simulator_state["prediction_history"][-max_history:]

        simulator_state["last_simulated_issue"] = latest_issue
        simulator_state["last_cycle_time"] = datetime.now(pytz.utc).isoformat()

        # Check for wallet depletion
        if simulator_state["total_wallet"] <= 0 and simulator_state["is_running"]:
            logger.warning(f"💸 Total wallet depleted: {simulator_state['total_wallet']:.2f}. Stopping simulator.")
            simulator_state["is_running"] = False
            
            depletion_entry = {
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "issue": latest_issue,
                "message": "🛑 Simulator STOPPED: Total wallet depleted.",
                "final_wallet": round(simulator_state["total_wallet"], 2)
            }
            simulator_state["prediction_history"].append(depletion_entry)

        # Save updated state
        with redis_lock:
            redis_client.set(REDIS_SIMULATOR_KEY, json.dumps(simulator_state))

        logger.info(f"✅ Simulated issue {latest_issue}. Wallet: {simulator_state['total_wallet']:.2f}, Bets: {len(history_entries_for_this_round)}")
        update_simulator_worker_status("completed", f"Simulated issue {latest_issue} successfully")

    except redis.exceptions.ConnectionError as e:
        logger.error(f"❌ Redis connection error during simulation: {e}")
        update_simulator_worker_status("redis_error", "Redis connection lost")
    except Exception as e:
        logger.error(f"💥 Error in simulation step: {e}", exc_info=True)
        update_simulator_worker_status("error", f"Simulation error: {str(e)}")

# --- Graceful Shutdown ---
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    signal_name = signal.Signals(signum).name
    logger.info(f"🛑 Received {signal_name} signal. Shutting down simulator worker...")
    update_simulator_worker_status("shutting_down", f"Received {signal_name} signal")
    shutdown_event.set()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# --- Main Execution Loop ---
if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("🚀 SIMULATOR WORKER STARTING (1-MINUTE CYCLE)")
    logger.info("=" * 60)
    
    logger.info(f"🌍 Environment: {'Railway' if is_railway_environment() else 'Local'}")
    logger.info(f"⏰ Cycle Duration: {CYCLE_DURATION} seconds")
    logger.info(f"🔗 Redis: {'Connected' if redis_client else 'Disconnected'}")
    
    update_simulator_worker_status("starting", "Simulator worker initializing")
    
    try:
        last_heartbeat = time.time()
        logger.info("🔄 Starting main simulation loop for 1-minute lottery cycle...")
        
        while not shutdown_event.is_set():
            try:
                # Run simulation step
                run_simulator_step()
                
                # Send periodic heartbeat
                current_time = time.time()
                if current_time - last_heartbeat >= HEARTBEAT_INTERVAL:
                    send_simulator_worker_heartbeat()
                    last_heartbeat = current_time
                
                # Wait for next cycle (1 minute)
                logger.debug(f"⏳ Waiting {CYCLE_DURATION} seconds until next 1-minute cycle...")
                if shutdown_event.wait(timeout=CYCLE_DURATION):
                    break  # Shutdown requested
                    
            except Exception as e:
                logger.error(f"💥 Error in main loop: {e}", exc_info=True)
                update_simulator_worker_status("loop_error", f"Main loop error: {str(e)}")
                time.sleep(10)  # Brief pause before retry
    
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
        update_simulator_worker_status("interrupted", "Keyboard interrupt")
    except Exception as e:
        logger.critical(f"💥 Critical error in simulator worker: {e}", exc_info=True)
        update_simulator_worker_status("critical", f"Critical error: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("🏁 Simulator worker shutdown complete")
        update_simulator_worker_status("stopped", "Simulator worker terminated")
