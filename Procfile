web: gunicorn --bind 0.0.0.0:$PORT app:app --workers 4 --log-file -
worker: python worker.py
ai-worker: python ai_worker.py  
color-worker: python color_worker_monitor.py
simulator-worker: python simulator_worker.py
