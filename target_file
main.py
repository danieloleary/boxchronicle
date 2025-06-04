import os
import json
import logging
import yaml
import requests
import time
from functools import wraps
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.cloud import firestore
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException
from requests.exceptions import RequestException
from google.api_core import exceptions as google_exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RetryableError(Exception):
    """Base class for retryable errors."""
    pass

class NonRetryableError(Exception):
    """Base class for non-retryable errors."""
    pass

def retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10, backoff_factor=2):
    """Decorator for retrying functions with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except RetryableError as e:
                    last_exception = e
                    if attempt == max_retries:
                        raise NonRetryableError(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                    
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                except NonRetryableError as e:
                    logger.error(f"Non-retryable error occurred: {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error: {str(e)}")
                    raise NonRetryableError(f"Unexpected error: {str(e)}")
            
            raise last_exception
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3)
def get_secret(secret_id, project_id):
    """Retrieve secret payload from Secret Manager with retry logic."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode('utf-8')
    except google_exceptions.NotFound:
        raise NonRetryableError(f"Secret {secret_id} not found")
    except google_exceptions.PermissionDenied:
        raise NonRetryableError(f"Permission denied accessing secret {secret_id}")
    except Exception as e:
        raise RetryableError(f"Error accessing secret: {str(e)}")

@retry_with_backoff(max_retries=3)
def obtain_chronicle_token(service_account_json):
    """Obtain Chronicle token with retry logic."""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=['https://www.googleapis.com/auth/chronicle']
        )
        credentials.refresh(Request())
        return credentials.token
    except Exception as e:
        raise RetryableError(f"Failed to obtain Chronicle token: {str(e)}")

@retry_with_backoff(max_retries=3)
def fetch_box_events(box_client, stream_position=0, limit=100):
    """Fetch Box events with retry logic."""
    try:
        events = box_client.events().get_enterprise_events(
            stream_position=stream_position,
            limit=limit
        )
        return events
    except BoxAPIException as e:
        if e.status in [429, 500, 502, 503, 504]:  # Rate limit or server errors
            raise RetryableError(f"Box API error (status {e.status}): {str(e)}")
        raise NonRetryableError(f"Box API error: {str(e)}")
    except Exception as e:
        raise RetryableError(f"Error fetching Box events: {str(e)}")

@retry_with_backoff(max_retries=3)
def send_to_chronicle(token, region, customer_id, events):
    """Send events to Chronicle with retry logic."""
    url = f"https://{region}-chronicle.googleapis.com/v1alpha/customers/{customer_id}/events:batchCreate"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    logs = [{'log_type': 'BOX', 'event': e} for e in events['entries']]
    payload = {'events': logs}
    
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        logger.info('Successfully ingested %d events', len(logs))
    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [429, 500, 502, 503, 504]:
            raise RetryableError(f"Chronicle API error (status {e.response.status_code}): {str(e)}")
        raise NonRetryableError(f"Chronicle API error: {str(e)}")
    except RequestException as e:
        raise RetryableError(f"Network error sending to Chronicle: {str(e)}")

@retry_with_backoff(max_retries=3)
def get_stream_position(firestore_client):
    """Retrieve last saved stream position from Firestore with retry logic."""
    try:
        doc_ref = firestore_client.collection('state').document('box')
        doc = doc_ref.get()
        if doc.exists:
            return int(doc.get('stream_position', 0))
        return 0
    except Exception as e:
        raise RetryableError(f"Error getting stream position: {str(e)}")

@retry_with_backoff(max_retries=3)
def save_stream_position(firestore_client, position):
    """Persist stream position to Firestore with retry logic."""
    try:
        doc_ref = firestore_client.collection('state').document('box')
        doc_ref.set({'stream_position': int(position)}, merge=True)
    except Exception as e:
        raise RetryableError(f"Error saving stream position: {str(e)}")

def main(request=None):
    try:
        config = load_config()
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            raise NonRetryableError("GOOGLE_CLOUD_PROJECT environment variable not set")

        service_account_secret = config.get('CHRONICLE_SERVICE_ACCOUNT')
        box_client_secret_id = config.get('BOX_CLIENT_SECRET')
        
        if not all([service_account_secret, box_client_secret_id]):
            raise NonRetryableError("Missing required configuration: CHRONICLE_SERVICE_ACCOUNT or BOX_CLIENT_SECRET")

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
        
        if events and events.get('entries'):
            send_to_chronicle(
                chronicle_token,
                config['CHRONICLE_REGION'],
                config['CHRONICLE_CUSTOMER_ID'],
                events
            )
            next_position = events.get('next_stream_position', stream_position)
            save_stream_position(firestore_client, next_position)
            logger.info("Successfully processed events up to position %s", next_position)
        else:
            logger.info("No new events to process")

        return 'OK', 200

    except NonRetryableError as e:
        logger.error("Non-retryable error occurred: %s", str(e))
        return str(e), 500
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        return str(e), 500

if __name__ == '__main__':
    os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'my-project')
    main() 