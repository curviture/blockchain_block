import requests
import time
from config import HEADERS

def get_api_data(url, max_retries=5):
    """Fetch JSON with built-in retries, 429 detection, and exponential backoff."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=45)
            
            # Dynamic 429 (Rate Limit) Detection
            if response.status_code == 429:
                # 1. Respect "Retry-After" header if the server provides it
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt * 5)
                
                print(f"\n   ⚠️ Rate Limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue # Retry the loop
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            print(f"\n   ❌ HTTP Error: {e}")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            print(f"\n   ❌ Connection Error: {e}")
            
        # Standard backoff for other errors
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 3
            time.sleep(wait_time)
            
    return None

