#!/bin/bash

# --- Configuration ---
# Name of your virtual environment directory in your Termux home (~/)
VENV_NAME="l_venv"
# Assumes the script is run from the project root (~/lottery)
PROJECT_DIR=$(pwd)
# File to store process IDs of Gunicorn and Worker
PID_FILE="$PROJECT_DIR/.app_pids"
# Log files for processes
GUNICORN_LOG="$PROJECT_DIR/gunicorn.log"
WORKER_LOG="$PROJECT_DIR/.worker.log" # Hidden by default
REDIS_LOG="$PROJECT_DIR/.redis.log"   # Hidden by default
# Port your Flask app runs on (from your .env)
PORT=5000
# PID file for Redis server (managed by this script)
REDIS_PID_FILE="$PROJECT_DIR/.redis_pid"
# New AI worker log file
AI_WORKER_LOG="$PROJECT_DIR/ai_worker.log"
# New color worker log file
COLOR_WORKER_LOG="$PROJECT_DIR/color_worker_monitor.log"

# --- Functions ---

start_app() {
    echo "--- Starting Application Processes ---"

    # Check if processes are already running via PID file
    if [ -f "$PID_FILE" ]; then
        echo "PID file found ($PID_FILE). Processes might already be running."
        echo "Please stop them first using: ./manage_app.sh stop"
        exit 1
    fi

    # 1. Start Redis server if not running (more robust check using PID file)
    # Check if PID file exists OR if the process in the PID file is not running
    if [ ! -f "$REDIS_PID_FILE" ] || ! kill -0 "$(cat "$REDIS_PID_FILE" 2>/dev/null)" 2>/dev/null; then
        echo "Redis server is not running or PID file is stale. Starting Redis server..."
        # Start Redis in daemonized mode, logging to file and storing its PID
        redis-server --daemonize yes --pidfile "$REDIS_PID_FILE" --logfile "$REDIS_LOG"
        sleep 3 # Give Redis a bit more time to fully stabilize and write its PID

        # Verify Redis started by checking its new PID file and if the process exists
        if [ -f "$REDIS_PID_FILE" ] && kill -0 "$(cat "$REDIS_PID_FILE" 2>/dev/null)" 2>/dev/null; then
            echo "Redis server started successfully (PID: $(cat "$REDIS_PID_FILE")). Logs in $REDIS_LOG"
        else
            echo "Error: Failed to start Redis server. Check .redis.log for details."
            exit 1 # Exit if Redis genuinely failed to start
        fi
    else
        echo "Redis server is already running (PID: $(cat "$REDIS_PID_FILE"))."
    fi

    # 2. Activate Venv and start Gunicorn (web process) in background
    echo "Starting Gunicorn web process..."
    # Running in a subshell '()' ensures 'cd' and 'source' don't affect the main script's shell
    (
        source "$HOME/$VENV_NAME/bin/activate"
        cd "$PROJECT_DIR"
        # Start Gunicorn, bind to all interfaces on specified port, log to file
        gunicorn --bind 0.0.0.0:"$PORT" app:app --log-file "$GUNICORN_LOG" --access-logfile "$PROJECT_DIR/gunicorn_access.log" &
        echo $! > "$PID_FILE" # Save Gunicorn's PID to the PID file (overwrites previous content)
    ) &
    WEB_PID=$! # Capture the PID of the subshell that starts Gunicorn
    echo "Started Gunicorn with PID: $WEB_PID. Name: Gunicorn"

    # Give Gunicorn a moment to initialize
    sleep 2

    # 3. Activate Venv and start Worker process in background
    echo "Starting worker process..."
    (
        source "$HOME/$VENV_NAME/bin/activate"
        cd "$PROJECT_DIR"
        # Start worker.py, redirecting its output to a log file
        python worker.py > "$WORKER_LOG" 2>&1 & # Redirect stdout and stderr
        echo $! >> "$PID_FILE" # Append worker's PID to the PID file
    ) &
    WORKER_PID=$! # Capture the PID of the subshell that starts the worker
    echo "Started worker with PID: $WORKER_PID. Name: worker.py"

    echo "--- Application processes are launching in the background ---"

    # 4. Start AI worker process
    echo "Starting AI worker process..."
    (
        source "$HOME/$VENV_NAME/bin/activate"
        cd "$PROJECT_DIR"
        python ai_worker.py >> "$AI_WORKER_LOG" 2>&1 &
        echo $! >> "$PID_FILE"
    ) &
    AI_WORKER_PID=$!
    echo "Started AI worker with PID: $AI_WORKER_PID. Name: ai_worker.py"
    
    # 5. Start color_worker_monitor.py process
    echo "Starting color worker monitor process..."
    (
        source "$HOME/$VENV_NAME/bin/activate"
        cd "$PROJECT_DIR"
        python color_worker_monitor.py >> "$COLOR_WORKER_LOG" 2>&1 &
        echo $! >> "$PID_FILE"
    ) &
    COLOR_WORKER_PID=$!
    echo "Started color worker monitor with PID: $COLOR_WORKER_PID. Name: color_worker_monitor.py"

    echo "Access web app at http://127.0.0.1:$PORT"
}

stop_app() {
    echo "--- Stopping Application Processes ---"

    # Stop Gunicorn and Worker processes first
    if [ -f "$PID_FILE" ]; then
        PIDS_TO_KILL=$(cat "$PID_FILE")
        for PID in $PIDS_TO_KILL; do
            if kill -0 "$PID" 2>/dev/null; then # Check if process exists
                echo "Attempting to kill process with PID: $PID"
                kill "$PID"
                sleep 1 # Give it a moment to terminate gracefully
                if kill -0 "$PID" 2>/dev/null; then # Check if it's still running after soft kill
                    echo "Process $PID did not stop gracefully. Force killing..."
                    kill -9 "$PID" # Force kill if necessary
                fi
            else
                echo "Process with PID $PID not found, likely already stopped."
            fi
        done
        rm "$PID_FILE" # Remove the PID file after processes are stopped
        echo "Gunicorn, Worker, and AI Worker processes stopped and PID file removed."
    else
        echo "No Gunicorn/Worker PID file found. Processes may not be running or were not started by this script."
    fi

    # Stop Redis server (if started by this script or has a PID file)
    if [ -f "$REDIS_PID_FILE" ]; then
        REDIS_PID=$(cat "$REDIS_PID_FILE" 2>/dev/null)
        if [ -n "$REDIS_PID" ] && kill -0 "$REDIS_PID" 2>/dev/null; then
            echo "Attempting to stop Redis server with PID: $REDIS_PID"
            kill "$REDIS_PID"
            sleep 2 # Give Redis a bit more time to save data and shut down
            if kill -0 "$REDIS_PID" 2>/dev/null; then
                echo "Redis server $REDIS_PID did not stop gracefully. Force killing..."
                kill -9 "$REDIS_PID"
            fi
        else
            echo "No active Redis process found for PID in $REDIS_PID_FILE, or PID file is stale."
        fi
        rm -f "$REDIS_PID_FILE" # Remove the Redis PID file
        echo "Redis PID file removed."
    else
        echo "No Redis PID file found to stop Redis."
    fi

    echo "--- All managed processes stopped ---"
}

# --- Main Script Logic ---
case "$1" in
    start)
        start_app
        ;;
    stop)
        stop_app
        ;;
    restart)
        stop_app
        sleep 3 # Give a bit more time for ports/resources to release completely before restarting
        start_app
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        echo "  start   - Starts Redis, then the web and worker processes."
        echo "  stop    - Stops the web, worker, and Redis processes."
        echo "  restart - Stops and then starts all processes."
        exit 1
        ;;
esac
