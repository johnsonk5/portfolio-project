import pytest
import requests
from dagster import DagsterResourceFunctionError, build_resources

import portfolio_project.defs.resources.sec as sec_resource_module
from portfolio_project.defs.resources.sec import SecClient, sec_resource


def test_sec_resource_requires_declared_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.setattr(sec_resource_module, "load_local_env", lambda: None)

    with pytest.raises(DagsterResourceFunctionError) as exc_info:
        with build_resources({"sec": sec_resource}):
            pass

    assert exc_info.value.__cause__ is not None
    assert "SEC_USER_AGENT must be set" in str(exc_info.value.__cause__)


def test_sec_resource_uses_configured_user_agent_and_safe_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.delenv("SEC_TIMEOUT_SECONDS", raising=False)

    with build_resources(
        {"sec": sec_resource.configured({"user_agent": "Portfolio Project admin@example.com"})}
    ) as resources:
        client = resources.sec

    assert client.user_agent == "Portfolio Project admin@example.com"
    assert client.session.headers["User-Agent"] == "Portfolio Project admin@example.com"
    assert client.timeout_seconds == 30.0
    assert client.max_requests_per_second == 10.0


def test_sec_resource_reads_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "Portfolio Project env-admin@example.com")
    monkeypatch.setenv("SEC_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("SEC_MAX_RETRIES", "4")
    monkeypatch.setenv("SEC_BACKOFF_FACTOR", "2")
    monkeypatch.setenv("SEC_RETRY_STATUS_CODES", "429,503")

    with build_resources({"sec": sec_resource}) as resources:
        client = resources.sec

    retry = client.session.adapters["https://"].max_retries
    assert client.user_agent == "Portfolio Project env-admin@example.com"
    assert client.timeout_seconds == 45.0
    assert client.max_requests_per_second == 10.0
    assert retry.total == 4
    assert retry.backoff_factor == 2.0
    assert set(retry.status_forcelist) == {429, 503}


def test_sec_client_retry_policy_retries_throttling_and_server_errors() -> None:
    client = SecClient(
        user_agent="Portfolio Project admin@example.com",
        max_retries=2,
        backoff_factor=0.5,
        retry_status_codes=[429, 503],
    )

    retry = client.session.adapters["https://"].max_retries
    assert retry.total == 2
    assert set(retry.status_forcelist) == {429, 503}
    assert retry.allowed_methods == {"GET", "HEAD"}
    assert retry.respect_retry_after_header is True
    assert retry.backoff_factor == 0.5


def test_sec_client_applies_timeout_and_raises_for_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SecClient(user_agent="Portfolio Project admin@example.com")
    calls = []

    class FakeResponse:
        content = b"payload"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("blocked")

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setattr(client.session, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        client.get("/files/company_tickers.json")

    assert calls == [
        (
            "https://www.sec.gov/files/company_tickers.json",
            {"timeout": 30.0},
        )
    ]


def test_sec_client_rate_limits_between_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SecClient(user_agent="Portfolio Project admin@example.com")
    monotonic_values = iter([100.0, 100.05, 100.1])
    sleeps = []

    class FakeResponse:
        content = b"payload"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(sec_resource_module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(sec_resource_module.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(client.session, "get", lambda url, **kwargs: FakeResponse())

    client.get("https://www.sec.gov/files/company_tickers.json")
    client.get("https://www.sec.gov/files/company_tickers_exchange.json")

    assert sleeps == [pytest.approx(0.05)]
