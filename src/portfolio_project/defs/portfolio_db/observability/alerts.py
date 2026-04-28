import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

ALERT_STATUSES = {"FAIL", "FAILURE", "SKIPPED", "WARN"}


@dataclass(frozen=True)
class EmailAlertConfig:
    enabled: bool
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_password: str | None
    smtp_starttls: bool
    sender: str | None
    recipients: tuple[str, ...]
    subject_prefix: str


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _split_recipients(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    normalized = raw.replace(";", ",")
    return tuple(part.strip() for part in normalized.split(",") if part.strip())


def load_email_alert_config() -> EmailAlertConfig:
    recipients = _split_recipients(os.getenv("PORTFOLIO_ALERT_EMAIL_TO"))
    smtp_host = os.getenv("PORTFOLIO_ALERT_SMTP_HOST")
    smtp_username = os.getenv("PORTFOLIO_ALERT_SMTP_USERNAME")
    sender = os.getenv("PORTFOLIO_ALERT_EMAIL_FROM") or smtp_username
    explicitly_enabled = _env_bool("PORTFOLIO_ALERT_EMAIL_ENABLED", True)
    return EmailAlertConfig(
        enabled=explicitly_enabled and bool(smtp_host) and bool(sender) and bool(recipients),
        smtp_host=smtp_host,
        smtp_port=_env_int("PORTFOLIO_ALERT_SMTP_PORT", 587),
        smtp_username=smtp_username,
        smtp_password=os.getenv("PORTFOLIO_ALERT_SMTP_PASSWORD"),
        smtp_starttls=_env_bool("PORTFOLIO_ALERT_SMTP_STARTTLS", True),
        sender=sender,
        recipients=recipients,
        subject_prefix=os.getenv("PORTFOLIO_ALERT_EMAIL_SUBJECT_PREFIX", "[Portfolio Alert]"),
    )


def is_red_observability_event(event: dict[str, Any]) -> bool:
    severity = str(event.get("severity") or "").upper()
    status = str(event.get("status") or "").upper()
    return severity == "RED" and status in ALERT_STATUSES


def _event_label(event: dict[str, Any]) -> str:
    event_type = event.get("event_type") or "observability"
    check_name = event.get("check_name")
    job_name = event.get("job_name")
    if check_name and job_name:
        return f"{event_type}: {job_name}/{check_name}"
    if job_name:
        return f"{event_type}: {job_name}"
    if check_name:
        return f"{event_type}: {check_name}"
    return str(event_type)


def _format_value(value: Any) -> str:
    if value is None:
        return "-"
    return str(value)


def build_red_alert_email(
    *,
    events: list[dict[str, Any]],
    config: EmailAlertConfig,
) -> EmailMessage:
    first_event = events[0]
    subject = (
        f"{config.subject_prefix} RED {first_event.get('event_type', 'observability')} "
        f"event: {_event_label(first_event)}"
    )
    if len(events) > 1:
        subject = f"{subject} (+{len(events) - 1} more)"

    lines = [
        "RED observability event(s) were recorded.",
        "",
    ]
    for index, event in enumerate(events, start=1):
        lines.extend(
            [
                f"{index}. {_event_label(event)}",
                f"   status: {_format_value(event.get('status'))}",
                f"   severity: {_format_value(event.get('severity'))}",
                f"   run_id: {_format_value(event.get('run_id'))}",
                f"   partition_key: {_format_value(event.get('partition_key'))}",
                f"   measured_value: {_format_value(event.get('measured_value'))}",
                f"   threshold_value: {_format_value(event.get('threshold_value'))}",
            ]
        )
        error_message = event.get("error_message")
        if error_message:
            lines.append(f"   error_message: {error_message}")
        details_json = event.get("details_json")
        if details_json:
            lines.append(f"   details_json: {details_json}")
        lines.append("")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = str(config.sender)
    message["To"] = ", ".join(config.recipients)
    message.set_content("\n".join(lines).rstrip() + "\n")
    return message


def send_email_alert(message: EmailMessage, config: EmailAlertConfig) -> None:
    if config.smtp_host is None:
        return
    with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as smtp:
        if config.smtp_starttls:
            smtp.starttls()
        if config.smtp_username and config.smtp_password:
            smtp.login(config.smtp_username, config.smtp_password)
        smtp.send_message(message)


def send_red_observability_alerts(
    events: list[dict[str, Any]],
    *,
    logger=None,
    config: EmailAlertConfig | None = None,
) -> int:
    red_events = [event for event in events if is_red_observability_event(event)]
    if not red_events:
        return 0

    alert_config = config or load_email_alert_config()
    if not alert_config.enabled:
        if logger is not None:
            logger.info(
                "RED observability alert suppressed because email alerting is not configured."
            )
        return 0

    message = build_red_alert_email(events=red_events, config=alert_config)
    send_email_alert(message, alert_config)
    if logger is not None:
        logger.info("Sent RED observability email alert for %s event(s).", len(red_events))
    return len(red_events)
