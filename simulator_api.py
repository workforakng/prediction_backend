# simulator_api.py
from flask import Blueprint, jsonify, request, render_template, current_app
import json
import os
import redis
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create a Blueprint for simulator routes
simulator_bp = Blueprint('simulator_bp', __name__)

# --- Railway Environment Detection ---
def is_railway_environment():
    """Check if running on Railway"""
    return os.getenv('RAILWAY_ENVIRONMENT') is not None

def get_railway_service_name():
    """Get Railway service name"""
    return os.getenv('RAILWAY_SERVICE_NAME', 'simulator_api')

# --- Redis Configuration ---
REDIS_SIMULATOR_KEY = "lottery:simulator_state"
LOTTERY_HISTORY_KEY = "lottery:history"  # Key for actual lottery history
REDIS_PREDICTION_KEY = "latest_prediction_data"  # Key for main app prediction data

# Color prediction keys for 1-minute cycle
REDIS_COLOR_PREDICTION_KEY = "lottery:color_prediction"
REDIS_SIZE_PREDICTION_KEY = "lottery:size_prediction"

# Status tracking
REDIS_SIMULATOR_STATUS_KEY = "lottery:simulator_status"

# --- Enhanced Helper Functions ---
def parse_bet_multipliers(multipliers_str):
    """
    Enhanced multipliers parser with validation for Railway deployment
    """
    try:
        if isinstance(multipliers_str, list):
            # Validate all elements are numeric
            return [float(x) for x in multipliers_str if isinstance(x, (int, float, str)) and str(x).replace('.', '').isdigit()]
        
        if isinstance(multipliers_str, str):
            multipliers = []
            for x in multipliers_str.split(','):
                x = x.strip()
                if x and x.replace('.', '').isdigit():
                    multipliers.append(float(x))
            return multipliers if multipliers else [1.0]
        
        return [1.0]  # Safe fallback
        
    except (ValueError, AttributeError) as e:
        current_app.logger.warning(f"Error parsing bet multipliers '{multipliers_str}': {e}")
        return [1.0]

def create_default_strategy_state(base_bet, multipliers_input):
    """
    Enhanced strategy state creation with validation
    """
    try:
        base_bet = max(0.01, float(base_bet))  # Minimum bet validation
        bet_multipliers = parse_bet_multipliers(multipliers_input)
        max_progression_steps = len(bet_multipliers) - 1
        
        return {
            "is_active": False,
            "base_bet": base_bet,
            "bet_multipliers": bet_multipliers,
            "max_progression_steps": max_progression_steps,
            "current_bet": base_bet * bet_multipliers[0] if bet_multipliers else base_bet,
            "current_progression_step": 0,
            "current_win_streak": 0,
            "max_win_streak": 0,
            "current_loss_streak": 0,
            "max_loss_streak": 0,
            "rounds_played": 0,
            "total_wagered": 0.0,
            "total_winnings": 0.0,
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        current_app.logger.error(f"Error creating strategy state: {e}")
        return create_default_strategy_state(10, "1")  # Safe fallback

def update_simulator_status(status, message=None):
    """Update simulator status in Redis"""
    try:
        if hasattr(current_app, 'redis_client') and current_app.redis_client:
            status_data = {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "environment": "railway" if is_railway_environment() else "local",
                "service_name": get_railway_service_name()
            }
            if message:
                status_data["message"] = message
            
            current_app.redis_client.set(REDIS_SIMULATOR_STATUS_KEY, json.dumps(status_data), ex=300)
            current_app.logger.debug(f"Simulator status updated: {status}")
    except Exception as e:
        current_app.logger.error(f"Failed to update simulator status: {e}")

# --- Enhanced API Endpoints ---

@simulator_bp.route('/start', methods=['POST'])
def simulator_start():
    """
    Enhanced simulator initialization for 1-minute cycle with Railway support
    """
    try:
        current_app.logger.info("🚀 Starting simulator initialization...")
        update_simulator_status("starting", "Initializing simulator")
        
        data = request.json or {}
        
        # Enhanced input validation
        try:
            initial_wallet = float(data.get('initial_wallet', 1000))
            if initial_wallet <= 0:
                raise ValueError("Initial wallet must be positive")
        except ValueError as e:
            return jsonify({
                "status": "error", 
                "message": f"Invalid initial wallet: {e}"
            }), 400
        
        simulation_mode = data.get('simulation_mode', 'main_app_prediction')  # Updated default
        
        # Validate simulation mode
        valid_modes = ['internal_logic', 'main_app_prediction', 'color_size_prediction']
        if simulation_mode not in valid_modes:
            return jsonify({
                "status": "error",
                "message": f"Invalid simulation mode. Must be one of: {valid_modes}"
            }), 400

        # Enhanced strategy configurations
        color_strategy_data = data.get('color_bet_strategy', {})
        big_small_strategy_data = data.get('big_small_bet_strategy', {})
        number_strategy_data = data.get('number_bet_strategy', {})

        # Create enhanced strategy states
        color_bet_strategy = create_default_strategy_state(
            color_strategy_data.get('base_bet', 10),
            color_strategy_data.get('bet_multipliers', '1,2,4,8,16')  # Extended progression
        )
        color_bet_strategy.update({
            'is_active': color_strategy_data.get('is_active', False),
            'target_color': color_strategy_data.get('target_color', 'Green'),
            'auto_target': color_strategy_data.get('auto_target', False)  # Use predictions
        })

        big_small_bet_strategy = create_default_strategy_state(
            big_small_strategy_data.get('base_bet', 10),
            big_small_strategy_data.get('bet_multipliers', '1,2,4,8,16')
        )
        big_small_bet_strategy.update({
            'is_active': big_small_strategy_data.get('is_active', False),
            'target_side': big_small_strategy_data.get('target_side', 'Big'),
            'auto_target': big_small_strategy_data.get('auto_target', False)
        })

        number_bet_strategy = create_default_strategy_state(
            number_strategy_data.get('base_bet', 10),
            '1'  # No progression for number bets
        )
        number_bet_strategy.update({
            'is_active': number_strategy_data.get('is_active', False),
            'target_number': int(number_strategy_data.get('target_number', 0)),
            'auto_target': number_strategy_data.get('auto_target', False)
        })

        # Enhanced simulator state for 1-minute cycle
        simulator_state = {
            "is_running": True,
            "total_wallet": initial_wallet,
            "initial_wallet": initial_wallet,
            "simulation_mode": simulation_mode,
            "cycle_duration": 60,  # 1-minute cycle
            "last_cycle_time": datetime.now().isoformat(),
            "bet_strategies": {
                "color_bet_strategy": color_bet_strategy,
                "big_small_bet_strategy": big_small_bet_strategy,
                "number_bet_strategy": number_bet_strategy,
            },
            "prediction_history": [],
            "last_simulated_issue": None,  # Changed from draw_id to issue
            "total_simulated_rounds": 0,
            "session_stats": {
                "start_time": datetime.now().isoformat(),
                "total_bets_placed": 0,
                "total_wins": 0,
                "total_losses": 0,
                "net_profit_loss": 0.0,
                "best_wallet": initial_wallet,
                "worst_wallet": initial_wallet
            },
            "environment": "railway" if is_railway_environment() else "local"
        }
        
        # Save to Redis
        current_app.redis_client.set(REDIS_SIMULATOR_KEY, json.dumps(simulator_state))
        update_simulator_status("running", f"Simulator started with {initial_wallet} wallet")
        
        current_app.logger.info(f"✅ Simulator initialized: Wallet={initial_wallet}, Mode={simulation_mode}")
        
        return jsonify({
            "status": "success", 
            "message": "Simulator started for 1-minute cycle.", 
            "state": simulator_state
        })
        
    except Exception as e:
        current_app.logger.error(f"❌ Error initializing simulator: {e}", exc_info=True)
        update_simulator_status("error", f"Initialization failed: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": f"Failed to initialize simulator: {str(e)}"
        }), 500

@simulator_bp.route('/stop', methods=['POST'])
def simulator_stop():
    """
    Enhanced simulator stop with session summary
    """
    try:
        current_app.logger.info("🛑 Stopping simulator...")
        update_simulator_status("stopping", "Stopping simulator")
        
        # Fetch current state for session summary
        simulator_state_str = current_app.redis_client.get(REDIS_SIMULATOR_KEY)
        session_summary = {}
        
        if simulator_state_str:
            current_state = json.loads(simulator_state_str)
            
            # Calculate session summary
            initial_wallet = current_state.get('initial_wallet', 1000)
            final_wallet = current_state.get('total_wallet', initial_wallet)
            session_stats = current_state.get('session_stats', {})
            
            session_summary = {
                "initial_wallet": initial_wallet,
                "final_wallet": final_wallet,
                "net_change": final_wallet - initial_wallet,
                "percentage_change": ((final_wallet - initial_wallet) / initial_wallet) * 100,
                "total_rounds": current_state.get('total_simulated_rounds', 0),
                "session_duration": session_stats.get('start_time'),
                "total_bets": session_stats.get('total_bets_placed', 0),
                "win_rate": (session_stats.get('total_wins', 0) / max(1, session_stats.get('total_bets_placed', 1))) * 100
            }
            
            # Reset state
            simulation_mode = current_state.get('simulation_mode', 'main_app_prediction')
            
            # Create fresh state
            reset_state = {
                "is_running": False,
                "total_wallet": initial_wallet,
                "initial_wallet": initial_wallet,
                "simulation_mode": simulation_mode,
                "cycle_duration": 60,
                "last_cycle_time": None,
                "bet_strategies": {
                    strategy_key: create_default_strategy_state(10, '1,2,4,8,16')
                    for strategy_key in ['color_bet_strategy', 'big_small_bet_strategy', 'number_bet_strategy']
                },
                "prediction_history": [],
                "last_simulated_issue": None,
                "total_simulated_rounds": 0,
                "session_stats": {
                    "start_time": None,
                    "total_bets_placed": 0,
                    "total_wins": 0,
                    "total_losses": 0,
                    "net_profit_loss": 0.0,
                    "best_wallet": initial_wallet,
                    "worst_wallet": initial_wallet
                },
                "environment": "railway" if is_railway_environment() else "local"
            }
            
            current_app.redis_client.set(REDIS_SIMULATOR_KEY, json.dumps(reset_state))
        
        update_simulator_status("stopped", "Simulator stopped and reset")
        current_app.logger.info("✅ Simulator stopped and reset")
        
        return jsonify({
            "status": "success", 
            "message": "Simulator stopped and reset for 1-minute cycle.", 
            "session_summary": session_summary
        })
        
    except Exception as e:
        current_app.logger.error(f"❌ Error stopping simulator: {e}", exc_info=True)
        update_simulator_status("error", f"Stop failed: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": f"Failed to stop simulator: {str(e)}"
        }), 500

@simulator_bp.route('/status', methods=['GET'])
def simulator_status():
    """
    Enhanced status endpoint with detailed metrics
    """
    try:
        simulator_state_str = current_app.redis_client.get(REDIS_SIMULATOR_KEY)
        
        if not simulator_state_str:
            return jsonify({
                "status": "not_initialized", 
                "message": "Simulator not initialized for 1-minute cycle."
            }), 200

        simulator_state = json.loads(simulator_state_str)
        
        # Add real-time calculated metrics
        if simulator_state.get('total_wallet') and simulator_state.get('initial_wallet'):
            initial = simulator_state['initial_wallet']
            current = simulator_state['total_wallet']
            simulator_state['performance_metrics'] = {
                "profit_loss": current - initial,
                "percentage_change": ((current - initial) / initial) * 100,
                "is_profitable": current > initial,
                "cycle_type": "1-minute",
                "environment": "railway" if is_railway_environment() else "local"
            }
        
        return jsonify({
            "status": "success", 
            "state": simulator_state
        })
        
    except redis.exceptions.ConnectionError:
        current_app.logger.error("Redis connection error during simulator status.")
        return jsonify({
            "status": "error", 
            "message": "Failed to connect to Redis for simulator status."
        }), 500
    except Exception as e:
        current_app.logger.error(f"Error getting simulator status: {e}", exc_info=True)
        return jsonify({
            "status": "error", 
            "message": f"Failed to retrieve simulator status: {str(e)}"
        }), 500

@simulator_bp.route('/history', methods=['GET'])
def simulator_history():
    """
    Get simulator prediction history with pagination
    """
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        
        simulator_state_str = current_app.redis_client.get(REDIS_SIMULATOR_KEY)
        if not simulator_state_str:
            return jsonify({
                "status": "not_initialized",
                "message": "Simulator not initialized."
            }), 200
        
        simulator_state = json.loads(simulator_state_str)
        history = simulator_state.get('prediction_history', [])
        
        # Pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_history = history[start_idx:end_idx]
        
        return jsonify({
            "status": "success",
            "history": paginated_history,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_entries": len(history),
                "total_pages": (len(history) + per_page - 1) // per_page
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"Error getting simulator history: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Failed to retrieve history: {str(e)}"
        }), 500

@simulator_bp.route('/config', methods=['POST'])
def update_simulator_config():
    """
    Update simulator configuration without stopping
    """
    try:
        data = request.json or {}
        
        simulator_state_str = current_app.redis_client.get(REDIS_SIMULATOR_KEY)
        if not simulator_state_str:
            return jsonify({
                "status": "error",
                "message": "Simulator not initialized."
            }), 400
        
        simulator_state = json.loads(simulator_state_str)
        
        # Update strategies if provided
        strategies = simulator_state.get('bet_strategies', {})
        
        if 'color_bet_strategy' in data:
            strategies['color_bet_strategy'].update(data['color_bet_strategy'])
        if 'big_small_bet_strategy' in data:
            strategies['big_small_bet_strategy'].update(data['big_small_bet_strategy'])
        if 'number_bet_strategy' in data:
            strategies['number_bet_strategy'].update(data['number_bet_strategy'])
        
        # Update simulation mode if provided
        if 'simulation_mode' in data:
            valid_modes = ['internal_logic', 'main_app_prediction', 'color_size_prediction']
            if data['simulation_mode'] in valid_modes:
                simulator_state['simulation_mode'] = data['simulation_mode']
        
        # Save updated state
        current_app.redis_client.set(REDIS_SIMULATOR_KEY, json.dumps(simulator_state))
        
        return jsonify({
            "status": "success",
            "message": "Simulator configuration updated.",
            "state": simulator_state
        })
        
    except Exception as e:
        current_app.logger.error(f"Error updating simulator config: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Failed to update configuration: {str(e)}"
        }), 500

# Route to render the simulator HTML page
@simulator_bp.route('/')
def simulator_home():
    """
    Enhanced simulator home page with 1-minute cycle info
    """
    try:
        if not os.path.exists('templates'):
            os.makedirs('templates')
        
        # Pass environment info to template
        template_context = {
            "cycle_duration": "1 minute",
            "environment": "Railway" if is_railway_environment() else "Local",
            "service_name": get_railway_service_name()
        }
        
        return render_template('simulator.html', **template_context)
        
    except Exception as e:
        current_app.logger.error(f"Error rendering simulator home: {e}")
        return jsonify({
            "status": "error",
            "message": "Simulator page not available"
        }), 500

# Error handler for the blueprint
@simulator_bp.errorhandler(Exception)
def handle_simulator_error(error):
    """Handle unexpected errors in simulator blueprint"""
    current_app.logger.error(f"Simulator blueprint error: {error}", exc_info=True)
    update_simulator_status("error", f"Blueprint error: {str(error)}")
    return jsonify({
        "status": "error",
        "message": "An unexpected error occurred in the simulator"
    }), 500
