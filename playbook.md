---
title: BoxChronicle Operational Playbook
description: Step-by-step guide to deploy and run the BoxChronicle integration.
keywords: [playbook, operations, box, chronicle, monitoring, setup]
---

## 1. Setup Environment

Prepare the local or cloud environment before running the integration.

1. Install Python dependencies with `pip install -r requirements.txt`.
2. Install Node.js dependencies with `npm install`.
3. Create an `.env.yml` file with Chronicle and Box configuration parameters.
   - Include service account names, Chronicle region, and Box enterprise ID.
4. Ensure any referenced secrets exist in Google Secret Manager.
   - At minimum, store the Chronicle ingestion key and the Box developer token.
5. Set the `GOOGLE_CLOUD_PROJECT` environment variable to your project ID.
6. Verify your local `gcloud` CLI is authenticated using `gcloud auth login`.
7. Confirm Node.js (v16+) and Python (3.9+) are installed.

## 2. Start the Integration

Run the ingestion logic and serve the monitoring dashboard.

1. Launch the Express server using `npm start`.
   - The server hosts the dashboard at `http://localhost:3000`.
2. In a separate terminal execute `python main.py` to begin fetching Box events.
   - Pass `--debug` for verbose output during initial testing.
3. Watch the terminal for messages indicating successful Chronicle ingestion.
4. Verify log files under `logs/` to confirm events were processed without error.
5. Leave both processes running to continuously forward Box events.

## 3. Monitor Operations

Use the dashboard at `http://localhost:3000` to track recent activity and statistics.

- **Integration Status** shows whether the Python process is running.
- **Event Statistics** summarizes the total events processed and the current batch size.
- **Recent Activity** lists each ingestion attempt with a success or error indicator.
- The "Run Now" button can manually trigger a polling cycle.
- The timestamp at the top reflects when data was last refreshed from the server.

## 4. Troubleshooting

1. Review log files under `logs/` for detailed error messages.
   - Look specifically for HTTP 4xx or 5xx responses from the Chronicle API.
2. Ensure service account credentials in Secret Manager have permission to access Chronicle.
   - Test using `gcloud auth application-default print-access-token` to verify.
3. Check the Box API rate limits if you encounter repeated retry warnings.
   - Use the Box developer console to review your account's request quota.
4. Confirm Firestore is reachable to store and retrieve the stream position.
   - Network or firewall issues can prevent the Python client from connecting.
5. If all else fails, rerun the integration with `--debug` and inspect the verbose output.

