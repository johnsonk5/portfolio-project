import os
import threading
import time
from urllib.parse import urljoin

import requests
from dagster import Array, Field, Float, Int, StringSource, resource
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from portfolio_project.defs.resources.env import load_local_env

SEC_MAX_FAIR_ACCESS_REQUESTS_PER_SECOND = 10.0
DEFAULT_SEC_TIMEOUT_SECONDS = 30.0
DEFAULT_SEC_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]


def _resolve_user_agent(configured_user_agent: str | None = None) -> str:
    user_agent = (configured_user_agent or os.getenv("SEC_USER_AGENT") or "").strip()
    if not user_agent:
        raise ValueError(
            "SEC_USER_AGENT must be set to a declared identity such as "
            "'Portfolio Project admin@example.com'. SEC fair-access guidance asks "
            "automated clients to identify themselves."
        )
    return user_agent


def _validate_positive_number(value: float, name: str) -> float:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


def _validate_non_negative_number(value: float, name: str) -> float:
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    return value


def _validate_non_negative_int(value: int, name: str) -> int:
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    return value


def _float_config(config: dict, name: str, env_var: str, default: float) -> float:
    value = config.get(name)
    if value is None:
        value = os.getenv(env_var)
    if value is None:
        return default
    return float(value)


def _int_config(config: dict, name: str, env_var: str, default: int) -> int:
    value = config.get(name)
    if value is None:
        value = os.getenv(env_var)
    if value is None:
        return default
    return int(value)


def _retry_status_codes_config(config: dict) -> list[int]:
    configured_value = config.get("retry_status_codes")
    if configured_value is not None:
        return list(configured_value)

    env_value = os.getenv("SEC_RETRY_STATUS_CODES")
    if env_value:
        return [int(status.strip()) for status in env_value.split(",") if status.strip()]

    return DEFAULT_SEC_RETRY_STATUS_CODES


class SecClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: float = DEFAULT_SEC_TIMEOUT_SECONDS,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        retry_status_codes: list[int] | None = None,
        base_url: str = "https://www.sec.gov/",
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = _validate_positive_number(timeout_seconds, "timeout_seconds")
        max_retries = _validate_non_negative_int(max_retries, "max_retries")
        backoff_factor = _validate_non_negative_number(backoff_factor, "backoff_factor")
        self.max_requests_per_second = SEC_MAX_FAIR_ACCESS_REQUESTS_PER_SECOND
        self.base_url = base_url.rstrip("/") + "/"
        self._min_request_interval_seconds = 1.0 / self.max_requests_per_second
        self._last_request_at: float | None = None
        self._rate_lock = threading.Lock()

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(
                retry_status_codes
                if retry_status_codes is not None
                else DEFAULT_SEC_RETRY_STATUS_CODES
            ),
            allowed_methods=frozenset(["GET", "HEAD"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json, text/plain, */*",
            }
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _rate_limit(self) -> None:
        with self._rate_lock:
            now = time.monotonic()
            if self._last_request_at is not None:
                wait_seconds = self._min_request_interval_seconds - (now - self._last_request_at)
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                    now = time.monotonic()
            self._last_request_at = now

    def _resolve_url(self, url_or_path: str) -> str:
        if url_or_path.startswith(("https://", "http://")):
            return url_or_path
        return urljoin(self.base_url, url_or_path.lstrip("/"))

    def get(self, url_or_path: str, **kwargs) -> requests.Response:
        self._rate_limit()
        kwargs.setdefault("timeout", self.timeout_seconds)
        response = self.session.get(self._resolve_url(url_or_path), **kwargs)
        response.raise_for_status()
        return response

    def get_json(self, url_or_path: str, **kwargs):
        return self.get(url_or_path, **kwargs).json()

    def get_bytes(self, url_or_path: str, **kwargs) -> bytes:
        return self.get(url_or_path, **kwargs).content


@resource(
    config_schema={
        "user_agent": Field(StringSource, is_required=False),
        "timeout_seconds": Field(Float, is_required=False),
        "max_retries": Field(Int, is_required=False),
        "backoff_factor": Field(Float, is_required=False),
        "retry_status_codes": Field(
            Array(Int),
            is_required=False,
        ),
        "base_url": Field(StringSource, is_required=False, default_value="https://www.sec.gov/"),
    }
)
def sec_resource(context) -> SecClient:
    load_local_env()
    config = context.resource_config
    return SecClient(
        user_agent=_resolve_user_agent(config.get("user_agent")),
        timeout_seconds=_float_config(
            config,
            "timeout_seconds",
            "SEC_TIMEOUT_SECONDS",
            DEFAULT_SEC_TIMEOUT_SECONDS,
        ),
        max_retries=_int_config(config, "max_retries", "SEC_MAX_RETRIES", 3),
        backoff_factor=_float_config(config, "backoff_factor", "SEC_BACKOFF_FACTOR", 1.0),
        retry_status_codes=_retry_status_codes_config(config),
        base_url=str(config.get("base_url", "https://www.sec.gov/")),
    )
