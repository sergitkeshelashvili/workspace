import requests
import time
import logging

# Configure a simple logger for the test results
test_logger = logging.getLogger('performance_test')
test_logger.setLevel(logging.INFO)
# Set up basic formatting if running this script directly
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

BASE_URL = "http://127.0.0.1:8000"


def measure_endpoint_time(endpoint: str):
    """
    Measures the total HTTP response time for a given endpoint.
    Returns the duration in milliseconds.
    """
    full_url = f"{BASE_URL}{endpoint}"

    try:
        start_time = time.time()
        response = requests.get(full_url, headers={"accept": "application/json"})
        end_time = time.time()

        duration_ms = (end_time - start_time) * 1000

        if response.status_code == 200:
            test_logger.info(
                f"Endpoint: {endpoint:<30} | STATUS: 200 OK | TIME: {duration_ms:.2f} ms"
            )
        else:
            test_logger.error(
                f"Endpoint: {endpoint:<30} | STATUS: {response.status_code} FAIL | TIME: {duration_ms:.2f} ms"
            )

        return duration_ms

    except requests.exceptions.ConnectionError:
        test_logger.critical(
            f"Endpoint: {endpoint:<30} | STATUS: CONNECTION REFUSED. Ensure FastAPI server is running."
        )
        return -1
