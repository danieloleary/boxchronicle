# Box Chronicle Ingestion

This repository contains a simple script for ingesting Box events into Google Security Operations (Chronicle). The script can be deployed to Cloud Run and scheduled via Cloud Scheduler.

## Files
- `main.py` – Cloud Run entry point for fetching Box events and sending them to Chronicle. Stream position is persisted in Firestore.
- `requirements.txt` – Python dependencies.
- `.env.yml` – Example configuration loaded at runtime.

## Running Locally
1. Install dependencies: `pip install -r requirements.txt`.
2. Set `GOOGLE_CLOUD_PROJECT` environment variable.
3. Ensure Firestore is enabled in your project (used to store Box stream position).
4. Execute `python main.py`.
