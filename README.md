# BoxChronicle Project

This repo contains a small integration for forwarding Box events to [Google Chronicle](https://cloud.google.com/chronicle). A minimal Node.js frontend is provided for simple local testing.

## Components

- **`main.py`** – Python function that:
  - Reads configuration from `.env.yml`.
  - Retrieves secrets from Google Secret Manager.
  - Fetches events from the Box Enterprise Events API.
  - Sends those events to Chronicle.
  - Stores the latest stream position in Firestore to avoid duplicates.
- **Node.js frontend** – A tiny Express app (`index.js` and the `public/` folder) that hosts a static page so you can confirm the project is running locally.

## Setup

1. Install the Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Install the Node.js dependencies:
   ```bash
   npm install
   ```
3. Populate `.env.yml` with your Chronicle and Box configuration and ensure the referenced secrets exist in Secret Manager.

## Usage

- **Run the Express server**
  ```bash
  npm start
  ```
  This serves the contents of `public/` on [http://localhost:3000](http://localhost:3000).

- **Execute the Python script locally**
  ```bash
  python main.py
  ```
  The script fetches Box events and ships them to Chronicle using the credentials and settings from `.env.yml`.

## Development

Feel free to extend the frontend or the ingestion logic. Pull requests are welcome!

