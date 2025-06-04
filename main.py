import os
import json
import logging
import yaml
import requests
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.cloud import firestore
from boxsdk import OAuth2, Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(path='.env.yml'):
    """Load configuration from YAML file."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_secret(secret_id, project_id):
    """Retrieve secret payload from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('utf-8')

def obtain_chronicle_token(service_account_json):
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_json),
        scopes=['https://www.googleapis.com/auth/chronicle']
    )
    credentials.refresh(Request())
    return credentials.token

def fetch_box_events(box_client, stream_position=0, limit=100):
    events = box_client.events().get_enterprise_events(
        stream_position=stream_position,
        limit=limit
    )
    return events

def send_to_chronicle(token, region, customer_id, events):
    url = f"https://{region}-chronicle.googleapis.com/v1alpha/customers/{customer_id}/events:batchCreate"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    logs = [{'log_type': 'BOX', 'event': e} for e in events['entries']]
    payload = {'events': logs}
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        logger.error('Chronicle ingestion failed: %s', resp.text)
    else:
        logger.info('Ingested %d events', len(logs))

def get_stream_position(firestore_client):
    """Retrieve last saved stream position from Firestore."""
    doc_ref = firestore_client.collection('state').document('box')
    doc = doc_ref.get()
    if doc.exists:
        return int(doc.get('stream_position', 0))
    return 0

def save_stream_position(firestore_client, position):
    """Persist stream position to Firestore."""
    doc_ref = firestore_client.collection('state').document('box')
    doc_ref.set({'stream_position': int(position)}, merge=True)


def main(request=None):
    config = load_config()
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    service_account_secret = config['CHRONICLE_SERVICE_ACCOUNT']
    box_client_secret_id = config['BOX_CLIENT_SECRET']

    service_account_json = get_secret(service_account_secret, project_id)
    box_client_secret = get_secret(box_client_secret_id, project_id)

    chronicle_token = obtain_chronicle_token(service_account_json)

    oauth = OAuth2(
        client_id=config['BOX_CLIENT_ID'],
        client_secret=box_client_secret
    )
    client = Client(oauth)

    firestore_client = firestore.Client()
    stream_position = get_stream_position(firestore_client)
    events = fetch_box_events(client, stream_position)
    send_to_chronicle(
        chronicle_token,
        config['CHRONICLE_REGION'],
        config['CHRONICLE_CUSTOMER_ID'],
        events
    )
    next_position = events.get('next_stream_position', stream_position)
    save_stream_position(firestore_client, next_position)
    return 'OK', 200

if __name__ == '__main__':
    os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'my-project')
    main()
