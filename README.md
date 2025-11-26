# Prediction Backend — Lottery Prediction Service
Live link
https://workforakng.github.io/prediction_backend/

Professional, production-ready backend for the Lottery Prediction system.

This repository contains the Flask API, worker processes, simulator blueprint and helper scripts that power a lottery prediction service using Redis as the primary datastore. It is designed to run locally for development and on platforms such as Railway for production.

Developer: Akshar — GitHub: `workforakng` (https://github.com/workforakng)

Contents: `app.py`, `worker.py`, `ai_worker.py`, `bot.py`, `simulator_api.py`, `simulator_worker.py`, `templates/`, `requirements.txt`, `Procfile`, and `railway.toml`.

Table of contents

- Overview
- Features
- Architecture & Components
- Quick start (Local)
- Environment variables
- API Reference (summary)
- Deployment notes
- Observability & Troubleshooting
- Contributing & Contact

Overview

This project provides the backend services for a lottery prediction product:

- `app.py`: Flask web service that serves the API and frontend templates.
- `worker.py`: Main prediction worker that fetches history, computes predictions, and persists results in Redis.
- `ai_worker.py`: AI enrichment worker that listens on a Redis pub/sub channel and produces AI-based predictions.
- `simulator_api.py`: Flask blueprint that exposes simulator endpoints for strategy testing and analysis.
- `bot.py`: Telegram bot integration to broadcast predictions and interact with users.
- Redis is the primary datastore for predictions, history, flags, accuracy metrics and pub/sub triggers.

Features

- Real-time Flask API with health checks and Redis diagnostics
- Worker process that generates and persists prediction data and triggers AI enrichment
- AI worker integrating with external AI providers (e.g., Perplexity) with retry/backoff and accuracy tracking
- Simulator API for running strategy simulations and collecting session analytics
- Telegram bot for multi-chat notifications and interactions
- Railway-friendly configuration and file-based logs when a data mount is provided

Architecture & components

- `app.py` (Web service): exposes endpoints such as `/api/prediction`, `/api/status`, `/api/ai/*`, `/health`, and registers the simulator blueprint.
- `worker.py` (Prediction worker): produces `latest_prediction_data` and publishes `lottery:ai_trigger` messages to Redis.
- `ai_worker.py` (AI worker): subscribes to `lottery:ai_trigger`, writes `lottery:ai_prediction`, and maintains accuracy hashes such as `lottery:ai_preds_history` and `lottery:ai_big_small_accuracy`.
- `simulator_api.py`: provides `/api/simulator/*` endpoints to start/stop simulations and inspect simulator state.
- `bot.py`: Telegram bot using `python-telegram-bot`, stores chat state in Redis keys prefixed with `telegram_chat:`.

Quick start (Local development)

Prerequisites

- Python 3.10+
- Redis server (local or remote)

Clone and set up

```powershell
git clone https://github.com/workforakng/prediction_backend.git
cd prediction_backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Environment

Create a `.env` file in the repository root with important variables (see next section). Example:

```env
REDIS_URL=redis://localhost:6379/0
BOT_TOKEN=your_telegram_bot_token
DAMAN_USERNAME=example_user
DAMAN_PASSWORD=example_pass
PERPLEXITY_API_KEY=sk-xxxx
RAILWAY_VOLUME_MOUNT_PATH=/data
```

Run services (each in its own terminal)

```powershell
python app.py        # web API
python worker.py     # main prediction worker
python ai_worker.py  # AI pub/sub listener
python bot.py        # Telegram bot (optional)
```

Environment variables (important)

- `REDIS_URL` — Primary Redis connection (e.g., `redis://localhost:6379/0`)
- `REDIS_URL_UPSTASH` — Optional Upstash URL used by `ai_worker.py` as a fallback
- `BOT_TOKEN` — Telegram bot token (required for `bot.py`)
- `DAMAN_USERNAME`, `DAMAN_PASSWORD` — Credentials used by `worker.py`'s `TokenManager`
- `PERPLEXITY_API_KEY`, `PERPLEXITY_API_KEY1` — API keys for the AI worker
- `RAILWAY_VOLUME_MOUNT_PATH` — Path for logs when running on Railway (defaults to `/data`)
- `PORT` — Flask port (defaults to `5000`)
- Railway-specific flags: `RAILWAY_ENVIRONMENT`, `RAILWAY_PUBLIC_DOMAIN`, `RAILWAY_SERVICE_NAME`

API reference (summary)

- `GET /api/prediction` — Returns the latest prediction payload (includes `ai_enabled` flag)
- `GET /api/status` — High-level service & Redis status
- `GET /api/ai/status` — Whether AI predictions are enabled
- `POST /api/ai/start` — Enable AI predictions (sets `lottery:ai_enabled` key in Redis)
- `POST /api/ai/stop` — Disable AI predictions (removes the key)
- Simulator endpoints are exposed under the blueprint at `/api/simulator/*`

Deployment notes (Railway or similar)

- `Procfile` and `railway.toml` are included. Configure separate services for the web and workers.
- Ensure `REDIS_URL` is available in the deployment environment.
- When available, use `RAILWAY_VOLUME_MOUNT_PATH` for persistent logs.

Observability & logging

- Each component writes logs to stdout (suitable for platform log collectors) and to files under the configured data mount when writable.
- Health endpoints include `GET /health` and `GET /api/redis/status` to inspect Redis connectivity and latency.

Troubleshooting

- Redis connection failures: verify `REDIS_URL`, network, and credentials. For managed Redis (Upstash), use the Upstash URL.
- AI worker not responding: confirm `lottery:ai_trigger` channel subscribers and valid Perplexity API keys.
- Worker exits at startup: ensure `DAMAN_USERNAME`, `DAMAN_PASSWORD`, and `REDIS_URL` are present.

Contributing



Contact & attribution

- Developer: Akshar — GitHub: `workforakng` — https://github.com/workforakng

License


---

**Your GitHub Repository Link:**

[https://github.com/workforakng/prediction_backend.git](https://github.com/workforakng/prediction_backend.git)

---

