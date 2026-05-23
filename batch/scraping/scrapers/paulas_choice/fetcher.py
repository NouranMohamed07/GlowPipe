"""
Page fetching logic for Paula's Choice Ingredient Dictionary scraper.

Responsibilities:
- Fetch ingredient detail pages using requests
- Apply timeout
- Apply retry handling
- Respect delays between requests
- Return structured fetch result
- Do not bypass anti-bot protections
"""

import random
import time
from dataclasses import dataclass
from typing import Optional

import requests

from config import (
    HEADERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    DELAY_MIN,
    DELAY_MAX,
    BACKOFF_FACTOR,
    RESPECTFUL_SCRAPING,
)
from utils import get_collected_at


@dataclass
class FetchResult:
    """
    Structured result returned by fetch_page().
    """

    url: str
    success: bool
    html: Optional[str]
    http_status: Optional[int]
    attempts: int
    error_type: Optional[str]
    error_message: Optional[str]
    collected_at: str


def respectful_delay(attempt_number=1):
    """
    Sleep between requests using random delay.

    Delay increases slightly with retry attempts.
    """

    if not RESPECTFUL_SCRAPING:
        return

    min_delay = DELAY_MIN * attempt_number
    max_delay = DELAY_MAX * attempt_number

    sleep_seconds = random.uniform(min_delay, max_delay)
    time.sleep(sleep_seconds)


def should_retry(status_code):
    """
    Decide whether an HTTP status code should be retried.
    """

    retryable_status_codes = {429, 500, 502, 503, 504}

    return status_code in retryable_status_codes


def fetch_page(url, logger=None):
    """
    Fetch a single page with retry handling.

    Parameters:
        url: ingredient page URL
        logger: optional logger instance

    Returns:
        FetchResult
    """

    last_error_type = None
    last_error_message = None
    last_status_code = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if logger:
                logger.info(f"Fetching URL | attempt={attempt} | url={url}")

            respectful_delay(attempt_number=attempt)

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            last_status_code = response.status_code

            if response.status_code == 200:
                html = response.text

                if not html or len(html.strip()) == 0:
                    return FetchResult(
                        url=url,
                        success=False,
                        html=None,
                        http_status=response.status_code,
                        attempts=attempt,
                        error_type="EmptyResponse",
                        error_message="Response HTML is empty.",
                        collected_at=get_collected_at(),
                    )

                return FetchResult(
                    url=url,
                    success=True,
                    html=html,
                    http_status=response.status_code,
                    attempts=attempt,
                    error_type=None,
                    error_message=None,
                    collected_at=get_collected_at(),
                )

            if response.status_code in {401, 403}:
                return FetchResult(
                    url=url,
                    success=False,
                    html=None,
                    http_status=response.status_code,
                    attempts=attempt,
                    error_type="AccessDenied",
                    error_message=(
                        f"Access denied with status code {response.status_code}. "
                        "Do not bypass anti-bot or access protections."
                    ),
                    collected_at=get_collected_at(),
                )

            if response.status_code == 404:
                return FetchResult(
                    url=url,
                    success=False,
                    html=None,
                    http_status=response.status_code,
                    attempts=attempt,
                    error_type="NotFound",
                    error_message="Page returned 404 Not Found.",
                    collected_at=get_collected_at(),
                )

            if should_retry(response.status_code):
                last_error_type = "RetryableHTTPError"
                last_error_message = f"Retryable HTTP status code: {response.status_code}"

                if logger:
                    logger.warning(
                        f"Retryable HTTP error | status={response.status_code} "
                        f"| attempt={attempt} | url={url}"
                    )

                backoff_sleep = BACKOFF_FACTOR ** attempt
                time.sleep(backoff_sleep)
                continue

            return FetchResult(
                url=url,
                success=False,
                html=None,
                http_status=response.status_code,
                attempts=attempt,
                error_type="HTTPError",
                error_message=f"Unexpected HTTP status code: {response.status_code}",
                collected_at=get_collected_at(),
            )

        except requests.exceptions.Timeout as exc:
            last_error_type = "Timeout"
            last_error_message = str(exc)

            if logger:
                logger.warning(f"Timeout | attempt={attempt} | url={url}")

            backoff_sleep = BACKOFF_FACTOR ** attempt
            time.sleep(backoff_sleep)

        except requests.exceptions.ConnectionError as exc:
            last_error_type = "ConnectionError"
            last_error_message = str(exc)

            if logger:
                logger.warning(f"Connection error | attempt={attempt} | url={url}")

            backoff_sleep = BACKOFF_FACTOR ** attempt
            time.sleep(backoff_sleep)

        except requests.exceptions.RequestException as exc:
            last_error_type = "RequestException"
            last_error_message = str(exc)

            if logger:
                logger.error(
                    f"Request exception | attempt={attempt} | url={url} | error={exc}"
                )

            backoff_sleep = BACKOFF_FACTOR ** attempt
            time.sleep(backoff_sleep)

        except Exception as exc:
            return FetchResult(
                url=url,
                success=False,
                html=None,
                http_status=last_status_code,
                attempts=attempt,
                error_type=type(exc).__name__,
                error_message=str(exc),
                collected_at=get_collected_at(),
            )

    return FetchResult(
        url=url,
        success=False,
        html=None,
        http_status=last_status_code,
        attempts=MAX_RETRIES,
        error_type=last_error_type or "MaxRetriesExceeded",
        error_message=last_error_message or "Max retries exceeded.",
        collected_at=get_collected_at(),
    )