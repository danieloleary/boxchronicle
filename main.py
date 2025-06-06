import os
import json
import logging
import yaml
import requests
import time
import datetime
from functools import wraps
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.cloud import firestore
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException
from requests.exceptions import RequestException
from google.api_core import exceptions as google_exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add file handler for persistent logging
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
file_handler = logging.FileHandler(
    os.path.join(log_dir, f'boxchronicle_{datetime.datetime.now().strftime("%Y%m%d")}.log')
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

class RetryableError(Exception):
    """Base class for retryable errors."""
    pass

class NonRetryableError(Exception):
    """Base class for non-retryable errors."""
    pass

def log_operation(operation_name):
    """Decorator for logging operation start, end, and duration."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"Starting operation: {operation_name}")
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"Completed operation: {operation_name} (took {duration:.2f}s)")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Failed operation: {operation_name} after {duration:.2f}s - Error: {str(e)}")
                raise
        return wrapper
    return decorator

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
                        logger.error(f"Operation failed after {max_retries} retries. Last error: {str(e)}")
                        raise NonRetryableError(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                    
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed: {str(e)}. "
                        f"Retrying in {delay} seconds... (Function: {func.__name__})"
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                except NonRetryableError as e:
                    logger.error(f"Non-retryable error in {func.__name__}: {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise NonRetryableError(f"Unexpected error: {str(e)}")
            
            raise last_exception
        return wrapper
    return decorator

@log_operation("Load Configuration")
def load_config(path='.env.yml'):
    """Load configuration from YAML file."""
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Successfully loaded configuration from {path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration from {path}: {str(e)}")
        raise

@retry_with_backoff(max_retries=3)
@log_operation("Get Secret")
def get_secret(secret_id, project_id):
    """Retrieve secret payload from Secret Manager with retry logic."""
    try:
        logger.info(f"Attempting to retrieve secret: {secret_id}")
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        logger.info(f"Successfully retrieved secret: {secret_id}")
        return response.payload.data.decode('utf-8')
    except google_exceptions.NotFound:
        logger.error(f"Secret not found: {secret_id}")
        raise NonRetryableError(f"Secret {secret_id} not found")
    except google_exceptions.PermissionDenied:
        logger.error(f"Permission denied accessing secret: {secret_id}")
        raise NonRetryableError(f"Permission denied accessing secret {secret_id}")
    except Exception as e:
        logger.error(f"Error accessing secret {secret_id}: {str(e)}")
        raise RetryableError(f"Error accessing secret: {str(e)}")

@retry_with_backoff(max_retries=3)
@log_operation("Obtain Chronicle Token")
def obtain_chronicle_token(service_account_json):
    """Obtain Chronicle token with retry logic."""
    try:
        logger.info("Attempting to obtain Chronicle token")
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=['https://www.googleapis.com/auth/chronicle']
        )
        credentials.refresh(Request())
        logger.info("Successfully obtained Chronicle token")
        return credentials.token
    except Exception as e:
        logger.error(f"Failed to obtain Chronicle token: {str(e)}")
        raise RetryableError(f"Failed to obtain Chronicle token: {str(e)}")

@retry_with_backoff(max_retries=3)
@log_operation("Fetch Box Events")
def fetch_box_events(box_client, stream_position=0, limit=100):
    """Fetch Box events with retry logic."""
    try:
        logger.info(f"Fetching Box events from position {stream_position} with limit {limit}")
        events = box_client.events().get_enterprise_events(
            stream_position=stream_position,
            limit=limit
        )
        event_count = len(events.get('entries', []))
        logger.info(f"Successfully fetched {event_count} Box events")
        return events
    except BoxAPIException as e:
        logger.error(f"Box API error (status {e.status}): {str(e)}")
        if e.status in [429, 500, 502, 503, 504]:  # Rate limit or server errors
            raise RetryableError(f"Box API error (status {e.status}): {str(e)}")
        raise NonRetryableError(f"Box API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching Box events: {str(e)}")
        raise RetryableError(f"Error fetching Box events: {str(e)}")

@retry_with_backoff(max_retries=3)
@log_operation("Send to Chronicle")
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
        logger.info(f"Sending {len(logs)} events to Chronicle")
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        logger.info(f'Successfully ingested {len(logs)} events to Chronicle')
    except requests.exceptions.HTTPError as e:
        logger.error(f"Chronicle API error (status {e.response.status_code}): {str(e)}")
        if e.response.status_code in [429, 500, 502, 503, 504]:
            raise RetryableError(f"Chronicle API error (status {e.response.status_code}): {str(e)}")
        raise NonRetryableError(f"Chronicle API error: {str(e)}")
    except RequestException as e:
        logger.error(f"Network error sending to Chronicle: {str(e)}")
        raise RetryableError(f"Network error sending to Chronicle: {str(e)}")

@retry_with_backoff(max_retries=3)
@log_operation("Get Stream Position")
def get_stream_position(firestore_client):
    """Retrieve last saved stream position from Firestore with retry logic."""
    try:
        logger.info("Retrieving stream position from Firestore")
        doc_ref = firestore_client.collection('state').document('box')
        doc = doc_ref.get()
        position = int(doc.get('stream_position', 0)) if doc.exists else 0
        logger.info(f"Retrieved stream position: {position}")
        return position
    except Exception as e:
        logger.error(f"Error getting stream position: {str(e)}")
        raise RetryableError(f"Error getting stream position: {str(e)}")

@retry_with_backoff(max_retries=3)
@log_operation("Save Stream Position")
def save_stream_position(firestore_client, position):
    """Persist stream position to Firestore with retry logic."""
    try:
        logger.info(f"Saving stream position {position} to Firestore")
        doc_ref = firestore_client.collection('state').document('box')
        doc_ref.set({'stream_position': int(position)}, merge=True)
        logger.info(f"Successfully saved stream position {position}")
    except Exception as e:
        logger.error(f"Error saving stream position: {str(e)}")
        raise RetryableError(f"Error saving stream position: {str(e)}")

@log_operation("Main Process")
def main(request=None):
    try:
        logger.info("Starting BoxChronicle integration process")
        
        # Load configuration
        config = load_config()
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            logger.error("GOOGLE_CLOUD_PROJECT environment variable not set")
            raise NonRetryableError("GOOGLE_CLOUD_PROJECT environment variable not set")

        # Validate configuration
        service_account_secret = config.get('CHRONICLE_SERVICE_ACCOUNT')
        box_client_secret_id = config.get('BOX_CLIENT_SECRET')
        
        if not all([service_account_secret, box_client_secret_id]):
            logger.error("Missing required configuration")
            raise NonRetryableError("Missing required configuration: CHRONICLE_SERVICE_ACCOUNT or BOX_CLIENT_SECRET")

        # Get secrets
        logger.info("Retrieving secrets from Secret Manager")
        service_account_json = get_secret(service_account_secret, project_id)
        box_client_secret = get_secret(box_client_secret_id, project_id)

        # Get Chronicle token
        chronicle_token = obtain_chronicle_token(service_account_json)

        # Initialize Box client
        logger.info("Initializing Box client")
        oauth = OAuth2(
            client_id=config['BOX_CLIENT_ID'],
            client_secret=box_client_secret
        )
        client = Client(oauth)

        # Process events
        firestore_client = firestore.Client()
        stream_position = get_stream_position(firestore_client)
        events = fetch_box_events(client, stream_position)
        
        if events and events.get('entries'):
            event_count = len(events['entries'])
            logger.info(f"Processing {event_count} events")
            
            send_to_chronicle(
                chronicle_token,
                config['CHRONICLE_REGION'],
                config['CHRONICLE_CUSTOMER_ID'],
                events
            )
            next_position = events.get('next_stream_position', stream_position)
            save_stream_position(firestore_client, next_position)
            logger.info(f"Successfully processed {event_count} events up to position {next_position}")
        else:
            logger.info("No new events to process")

        logger.info("BoxChronicle integration process completed successfully")
        return 'OK', 200

    except NonRetryableError as e:
        logger.error(f"Non-retryable error occurred: {str(e)}")
        return str(e), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return str(e), 500

if __name__ == '__main__':
    os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'my-project')
    main()
