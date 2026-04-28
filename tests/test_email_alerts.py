from email.message import EmailMessage

from portfolio_project.defs.portfolio_db.observability import alerts
from portfolio_project.defs.portfolio_db.observability.alerts import (
    EmailAlertConfig,
    build_red_alert_email,
    is_red_observability_event,
    load_email_alert_config,
    send_red_observability_alerts,
)


def _config() -> EmailAlertConfig:
    return EmailAlertConfig(
        enabled=True,
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_username="user@example.com",
        smtp_password="secret",
        smtp_starttls=True,
        sender="alerts@example.com",
        recipients=("ops@example.com",),
        subject_prefix="[Test]",
    )


def test_load_email_alert_config_uses_recipient_env(monkeypatch) -> None:
    monkeypatch.setenv("PORTFOLIO_ALERT_EMAIL_TO", "ops@example.com; data@example.com")
    monkeypatch.setenv("PORTFOLIO_ALERT_EMAIL_FROM", "alerts@example.com")
    monkeypatch.setenv("PORTFOLIO_ALERT_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("PORTFOLIO_ALERT_SMTP_PORT", "2525")
    monkeypatch.setenv("PORTFOLIO_ALERT_SMTP_STARTTLS", "false")

    config = load_email_alert_config()

    assert config.enabled is True
    assert config.recipients == ("ops@example.com", "data@example.com")
    assert config.sender == "alerts@example.com"
    assert config.smtp_host == "smtp.example.com"
    assert config.smtp_port == 2525
    assert config.smtp_starttls is False


def test_load_email_alert_config_is_disabled_without_recipient(monkeypatch) -> None:
    monkeypatch.setenv("PORTFOLIO_ALERT_EMAIL_FROM", "alerts@example.com")
    monkeypatch.setenv("PORTFOLIO_ALERT_SMTP_HOST", "smtp.example.com")
    monkeypatch.delenv("PORTFOLIO_ALERT_EMAIL_TO", raising=False)

    assert load_email_alert_config().enabled is False


def test_is_red_observability_event_requires_red_actionable_status() -> None:
    assert is_red_observability_event({"severity": "RED", "status": "FAIL"}) is True
    assert is_red_observability_event({"severity": "RED", "status": "WARN"}) is True
    assert is_red_observability_event({"severity": "RED", "status": "SKIPPED"}) is True
    assert is_red_observability_event({"severity": "YELLOW", "status": "FAIL"}) is False
    assert is_red_observability_event({"severity": "RED", "status": "PASS"}) is False


def test_build_red_alert_email_includes_event_context() -> None:
    message = build_red_alert_email(
        events=[
            {
                "event_type": "data_quality",
                "severity": "RED",
                "status": "FAIL",
                "run_id": "run-1",
                "job_name": "daily_prices_job",
                "partition_key": "2026-02-13",
                "check_name": "dq_silver_prices_ranges",
                "measured_value": 2.0,
                "threshold_value": 0.0,
                "details_json": '{"bad_rows": 2}',
            }
        ],
        config=_config(),
    )

    assert message["Subject"].startswith("[Test] RED data_quality event")
    body = message.get_content()
    assert "daily_prices_job/dq_silver_prices_ranges" in body
    assert "run_id: run-1" in body
    assert 'details_json: {"bad_rows": 2}' in body


def test_send_red_observability_alerts_filters_and_sends(monkeypatch) -> None:
    sent_messages: list[EmailMessage] = []

    def fake_send(message: EmailMessage, config: EmailAlertConfig) -> None:
        sent_messages.append(message)

    monkeypatch.setattr(alerts, "send_email_alert", fake_send)

    count = send_red_observability_alerts(
        [
            {"severity": "YELLOW", "status": "FAIL", "event_type": "freshness"},
            {
                "severity": "RED",
                "status": "FAILURE",
                "event_type": "pipeline_failure",
                "job_name": "daily_prices_job",
            },
        ],
        config=_config(),
    )

    assert count == 1
    assert len(sent_messages) == 1
    assert "pipeline_failure" in sent_messages[0].get_content()


def test_send_red_observability_alerts_noops_when_disabled(monkeypatch) -> None:
    def fail_send(message: EmailMessage, config: EmailAlertConfig) -> None:
        raise AssertionError("send_email_alert should not be called")

    monkeypatch.setattr(alerts, "send_email_alert", fail_send)
    disabled_config = EmailAlertConfig(
        enabled=False,
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_username=None,
        smtp_password=None,
        smtp_starttls=True,
        sender="alerts@example.com",
        recipients=("ops@example.com",),
        subject_prefix="[Test]",
    )

    count = send_red_observability_alerts(
        [{"severity": "RED", "status": "FAIL", "event_type": "freshness"}],
        config=disabled_config,
    )

    assert count == 0
