import logging
from functools import wraps
from time import sleep
from typing import List, Type


def retry(*, max_attempts: int, initial_delay=1.0, backoff_factor=2, expected_exceptions: List[Type[Exception]] = None):
    assert backoff_factor >= 1, "Backoff factor should be >= 1"
    expected_exceptions = tuple(expected_exceptions or (Exception,))  # freeze

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = initial_delay
            while True:
                try:
                    return f(*args, **kwargs)
                except expected_exceptions:  # accepts tuple, but not list
                    logging.warning(f"Error calling {f}", exc_info=True)
                    # run checks
                    if attempt >= max_attempts:
                        logging.error(f"Failed after {attempt} attempts, raising latest error")
                        raise
                    # wait and try again
                    logging.info(f"Sleeping for {delay} seconds")
                    sleep(delay)
                    delay *= backoff_factor
                    attempt += 1
                    logging.info(f"Initiating retry {attempt}/{max_attempts}")

        return wrapper

    return decorator
