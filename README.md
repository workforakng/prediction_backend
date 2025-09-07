<<<<<<< HEAD
# prediction_backend
=======
# Lottery Prediction Service

This repository contains the backend (Flask API) and worker service for a lottery prediction application. It utilizes Redis for data storage and facilitates communication with a frontend application (e.g., hosted on GitHub Pages).

## Table of Contents

-   [Features](#features)
-   [Architecture](#architecture)
-   [Deployment](#deployment)
-   [Getting Started (Local Development)](#getting-started-local-development)
-   [API Endpoints](#api-endpoints)
-   [Contributing](#contributing)
-   [License](#license)

## Features

* **Lottery Prediction:** Generates and stores lottery predictions.
* **Redis Integration:** Leverages Redis as a fast in-memory data store for predictions, history, and status.
* **AI Toggle:** Allows enabling/disabling AI-based predictions via API.
* **Status Monitoring:** Provides API endpoints to check the service status and latest prediction updates.
* **CORS Enabled:** Configured for secure cross-origin resource sharing with specified frontend domains.

## Architecture

The service is composed of two main parts, typically deployed as separate services on platforms like Railway:

1.  **Web Service (`app.py`):**
    * A Flask application that exposes RESTful API endpoints.
    * Handles requests from the frontend for current predictions, service status, and AI toggle.
    * Interacts with Redis to retrieve prediction data.
    * Configured with CORS to allow specific frontend origins.

2.  **Worker Service (e.g., `worker.py` or a script executed periodically):**
    * Responsible for the core prediction logic.
    * Fetches historical lottery data.
    * Runs prediction algorithms (potentially involving AI, like Gemini AI).
    * Stores the latest predictions and related statistics into Redis.
    * *Note: While not explicitly in `app.py`, the logs indicate a worker is saving data to Redis keys like `lottery:history`, `lottery:predictions`, etc.*

3.  **Redis Database:**
    * Used for storing various data points including:
        * `latest_prediction_data` (under `REDIS_PREDICTION_KEY`).
        * `lottery:history`
        * `lottery:predictions`
        * `lottery:number_stats`
        * `lottery:trend_analysis`
        * `lottery:streaks`
    * Also stores the AI enabled/disabled status.

## Deployment

This service is designed to be deployed on platforms like [Railway](https://railway.app/).

**Railway Deployment Steps:**

1.  **Fork/Clone this Repository:** Get a copy of this codebase.
2.  **Create a New Project on Railway:** Connect your GitHub repository to a new Railway project.
3.  **Add Redis Database:** Add a Redis database service to your Railway project. Railway will automatically inject the `REDIS_URL` environment variable.
4.  **Configure Services:**
    * **Web Service:** Point your `web` service to `app.py`. Ensure its start command is `python app.py`.
    * **Worker Service:** Configure a separate `worker` service (e.g., a cron job or background worker) that runs your prediction script periodically. Its command might be `python worker.py` or similar, depending on your worker implementation.
5.  **Environment Variables:** Ensure `REDIS_URL` is correctly set (Railway usually handles this automatically for Redis).
6.  **CORS Configuration:** The `app.py` is pre-configured to allow requests from:
    * `https://prediction.up.railway.app` (your Railway app URL)
    * `https://workforakng.github.io` (your GitHub Pages frontend URL)
    * **Important:** If your GitHub Pages URL changes (e.g., if you host it under a project-specific path), you **must** update the `CORS` configuration in `app.py` accordingly.

## Getting Started (Local Development)

To run this project locally for development and testing:

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/workforakng/prediction.git](https://github.com/workforakng/prediction.git)
    cd prediction
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate # On Windows: .\venv\Scripts\activate
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *(Make sure `requirements.txt` contains `Flask`, `Flask-CORS`, `python-dotenv`, `redis`)*

4.  **Set up Environment Variables:**
    Create a `.env` file in the root of the project and add:
    ```
    REDIS_URL=redis://localhost:6379/0
    RAILWAY_VOLUME_MOUNT_PATH=/data # Or a local path for logs if needed
    ```
    (Ensure you have a local Redis server running, or provide a public Redis URL for testing).

5.  **Run the Flask App:**
    ```bash
    python app.py
    ```
    The Flask API will typically run on `http://127.0.0.1:5000` (or the `PORT` specified in your `.env`).

6.  **Run the Worker (in a separate terminal):**
    You would need to run your worker script (e.g., `worker.py`) separately if it's not integrated within the Flask app itself.
    ```bash
    python worker.py # Replace with your actual worker script name
    ```

## API Endpoints

All API endpoints are prefixed with `/api`.

* **`GET /api/prediction`**
    * Returns the latest lottery prediction data.
    * **Response:** JSON object containing prediction data, timestamp, and `ai_enabled` status.
    * **Example Success Response:**
        ```json
        {
            "prediction": [1, 2, 3, 4, 5],
            "timestamp": "2025-06-25 12:30:00",
            "status": "success",
            "message": "Latest prediction data.",
            "ai_enabled": true
        }
        ```
* **`GET /api/status`**
    * Provides the current status of the prediction service.
    * **Response:** JSON object with service status, last update timestamp, and `ai_enabled` status.
    * **Example Success Response:**
        ```json
        {
            "service_status": "running",
            "last_prediction_update": "2025-06-25 12:30:00",
            "current_data_status": "success",
            "message": "Prediction cycle completed.",
            "ai_enabled": true
        }
        ```
* **`GET /api/ai/status`**
    * Checks if the AI prediction is currently enabled or disabled.
    * **Response:** `{"ai_enabled": true/false}`
* **`POST /api/ai/start`**
    * Enables AI-based predictions.
    * **Response:** `{"status": "success", "ai_enabled": true}`
* **`POST /api/ai/stop`**
    * Disables AI-based predictions.
    * **Response:** `{"status": "success", "ai_enabled": false}`

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

[Specify your license here, e.g., MIT License]

---

**Your GitHub Repository Link:**

[https://github.com/workforakng/prediction.git](https://github.com/workforakng/prediction.git)

---
>>>>>>> 568df4d (Initial lottery prediction system for Railway deployment)
