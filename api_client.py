import requests
import time
from config import HEADERS

def get_api_data(url, max_retries=5):
    """Fetch JSON with built-in retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            # High timeout to handle large transaction lists
            response = requests.get(url, headers=HEADERS, timeout=45)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                time.sleep(wait_time)
            else:
                return None
