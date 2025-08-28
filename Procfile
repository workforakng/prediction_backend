web: gunicorn --bind 0.0.0.0:$PORT app:app --workers 4 --log-file -
worker: python worker.py
color-worker: python color_worker_monitor.py
bot: python bot.py
