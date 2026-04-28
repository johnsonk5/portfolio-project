import os
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any

from dagster import AssetExecutionContext, asset

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


def _table_exists(con, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        is not None
    )


def _fmt_int(value: Any) -> str:
    if value is None:
        return "-"
    return f"{int(value):,}"


def _fmt_seconds(value: Any) -> str:
    if value is None:
        return "-"
    minutes = float(value) / 60.0
    if minutes < 1:
        return f"{float(value):.0f}s"
    return f"{minutes:.1f}m"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100.0:+.1f}%"


def _fmt_plain_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100.0:.1f}%"


def _bar(value: float | None, *, width: int = 12) -> str:
    if value is None:
        return "[" + ("." * width) + "]"
    bounded = max(0.0, min(1.0, value))
    filled = round(bounded * width)
    return "[" + ("#" * filled) + ("." * (width - filled)) + "]"


def _as_iso_day(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _load_run_stats(con, window_start: date, window_end: date) -> dict[str, Any]:
    if not _table_exists(con, "observability", "run_log"):
        return {
            "available": False,
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "avg_duration_seconds": None,
            "assets_materialized_count": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
        }

    row = con.execute(
        """
        SELECT
            count(*) AS total_runs,
            count(*) FILTER (WHERE upper(status) = 'SUCCESS') AS successful_runs,
            count(*) FILTER (WHERE upper(status) = 'FAILURE') AS failed_runs,
            avg(duration_seconds) AS avg_duration_seconds,
            coalesce(sum(assets_materialized_count), 0) AS assets_materialized_count,
            coalesce(sum(rows_inserted), 0) AS rows_inserted,
            coalesce(sum(rows_updated), 0) AS rows_updated,
            coalesce(sum(rows_deleted), 0) AS rows_deleted
        FROM observability.run_log
        WHERE coalesce(end_time, logged_ts) >= ?
          AND coalesce(end_time, logged_ts) < ?
        """,
        [window_start, window_end],
    ).fetchone()
    return {
        "available": True,
        "total_runs": int(row[0] or 0),
        "successful_runs": int(row[1] or 0),
        "failed_runs": int(row[2] or 0),
        "avg_duration_seconds": row[3],
        "assets_materialized_count": int(row[4] or 0),
        "rows_inserted": int(row[5] or 0),
        "rows_updated": int(row[6] or 0),
        "rows_deleted": int(row[7] or 0),
    }


def _load_market_snapshot(con) -> dict[str, Any]:
    if not _table_exists(con, "gold", "prices"):
        return {
            "available": False,
            "latest_trade_date": None,
            "indices": [],
            "breadth": {},
        }

    latest_trade_date = con.execute("SELECT max(trade_date) FROM gold.prices").fetchone()[0]
    if latest_trade_date is None:
        return {
            "available": True,
            "latest_trade_date": None,
            "indices": [],
            "breadth": {},
        }

    index_rows = con.execute(
        """
        SELECT
            symbol,
            close,
            returns_5d,
            returns_21d,
            pct_below_52w_high
        FROM gold.prices
        WHERE trade_date = ?
          AND symbol IN ('SPY', 'QQQ', 'DIA', 'IWM')
        ORDER BY
            CASE symbol
                WHEN 'SPY' THEN 1
                WHEN 'QQQ' THEN 2
                WHEN 'DIA' THEN 3
                WHEN 'IWM' THEN 4
                ELSE 5
            END
        """,
        [latest_trade_date],
    ).fetchall()
    breadth_row = con.execute(
        """
        SELECT
            avg(CASE WHEN returns_5d > 0 THEN 1.0 ELSE 0.0 END) AS pct_positive_5d,
            median(returns_5d) AS median_return_5d,
            avg(CASE WHEN close > sma_50 THEN 1.0 ELSE 0.0 END) AS pct_above_sma_50,
            avg(CASE WHEN close > sma_200 THEN 1.0 ELSE 0.0 END) AS pct_above_sma_200,
            median(realized_vol_21d) AS median_realized_vol_21d
        FROM gold.prices
        WHERE trade_date = ?
          AND close IS NOT NULL
        """,
        [latest_trade_date],
    ).fetchone()
    return {
        "available": True,
        "latest_trade_date": latest_trade_date,
        "indices": [
            {
                "symbol": row[0],
                "close": row[1],
                "returns_5d": row[2],
                "returns_21d": row[3],
                "pct_below_52w_high": row[4],
            }
            for row in index_rows
        ],
        "breadth": {
            "pct_positive_5d": breadth_row[0],
            "median_return_5d": breadth_row[1],
            "pct_above_sma_50": breadth_row[2],
            "pct_above_sma_200": breadth_row[3],
            "median_realized_vol_21d": breadth_row[4],
        },
    }


def _load_discounts(con, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "gold", "prices"):
        return []
    rows = con.execute(
        """
        WITH latest AS (
            SELECT max(trade_date) AS trade_date
            FROM gold.prices
        )
        SELECT
            p.symbol,
            p.close,
            p.pct_below_52w_high,
            p.returns_5d,
            p.returns_21d,
            p.momentum_12_1,
            p.realized_vol_21d
        FROM gold.prices AS p
        INNER JOIN latest
            ON p.trade_date = latest.trade_date
        WHERE p.pct_below_52w_high IS NOT NULL
          AND p.close IS NOT NULL
        ORDER BY p.pct_below_52w_high DESC NULLS LAST
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [
        {
            "symbol": row[0],
            "close": row[1],
            "pct_below_52w_high": row[2],
            "returns_5d": row[3],
            "returns_21d": row[4],
            "momentum_12_1": row[5],
            "realized_vol_21d": row[6],
        }
        for row in rows
    ]


def _load_random_headlines(
    con,
    *,
    window_start: date,
    window_end: date,
    limit: int,
) -> list[dict[str, Any]]:
    if not _table_exists(con, "gold", "headlines"):
        return []
    rows = con.execute(
        """
        SELECT
            symbol,
            title,
            provider_publish_time,
            link,
            sentiment
        FROM gold.headlines
        WHERE provider_publish_time >= ?
          AND provider_publish_time < ?
          AND title IS NOT NULL
          AND trim(title) <> ''
        ORDER BY random()
        LIMIT ?
        """,
        [window_start, window_end, limit],
    ).fetchall()
    return [
        {
            "symbol": row[0],
            "title": row[1],
            "provider_publish_time": row[2],
            "link": row[3],
            "sentiment": row[4],
        }
        for row in rows
    ]


def collect_weekly_digest_data(
    con,
    *,
    as_of: date | None = None,
    discounts_limit: int = 10,
    news_limit: int = 5,
) -> dict[str, Any]:
    digest_date = as_of or datetime.now(timezone.utc).date()
    window_end = digest_date + timedelta(days=1)
    window_start = window_end - timedelta(days=7)
    return {
        "as_of": digest_date,
        "window_start": window_start,
        "window_end": window_end,
        "run_stats": _load_run_stats(con, window_start, window_end),
        "market": _load_market_snapshot(con),
        "discounts": _load_discounts(con, discounts_limit),
        "headlines": _load_random_headlines(
            con,
            window_start=window_start,
            window_end=window_end,
            limit=news_limit,
        ),
    }


def build_weekly_digest_email(
    *,
    digest: dict[str, Any],
    config: EmailAlertConfig,
) -> EmailMessage:
    as_of = digest["as_of"]
    window_start = digest["window_start"]
    window_end = digest["window_end"] - timedelta(days=1)
    subject = f"{config.subject_prefix} Weekly market digest: {as_of}"

    run_stats = digest["run_stats"]
    market = digest["market"]
    breadth = market.get("breadth") or {}
    success_rate = None
    if run_stats["total_runs"]:
        success_rate = run_stats["successful_runs"] / run_stats["total_runs"]

    lines = [
        f"Weekly market digest for {window_start} through {window_end}.",
        "",
        "KPIs",
        f"- Pipeline success rate: {_fmt_plain_pct(success_rate)} {_bar(success_rate)}",
        (
            "- Runs: "
            f"{_fmt_int(run_stats['total_runs'])} total, "
            f"{_fmt_int(run_stats['successful_runs'])} success, "
            f"{_fmt_int(run_stats['failed_runs'])} failed"
        ),
        f"- Average run duration: {_fmt_seconds(run_stats['avg_duration_seconds'])}",
        (
            "- Data mutations: "
            f"{_fmt_int(run_stats['rows_inserted'])} inserted, "
            f"{_fmt_int(run_stats['rows_updated'])} updated, "
            f"{_fmt_int(run_stats['rows_deleted'])} deleted"
        ),
        (
            "- Market breadth: "
            f"{_fmt_plain_pct(breadth.get('pct_positive_5d'))} positive over 5d "
            f"{_bar(breadth.get('pct_positive_5d'))}"
        ),
        f"- Median 5d return: {_fmt_pct(breadth.get('median_return_5d'))}",
        (
            "- Trend: "
            f"{_fmt_plain_pct(breadth.get('pct_above_sma_50'))} above 50d SMA, "
            f"{_fmt_plain_pct(breadth.get('pct_above_sma_200'))} above 200d SMA"
        ),
        (
            "- Median 21d realized volatility: "
            f"{_fmt_plain_pct(breadth.get('median_realized_vol_21d'))}"
        ),
        "",
        f"Market Performance ({_as_iso_day(market.get('latest_trade_date'))})",
    ]

    if market.get("indices"):
        for row in market["indices"]:
            lines.append(
                "- "
                f"{row['symbol']}: close {row['close']:.2f}, "
                f"5d {_fmt_pct(row['returns_5d'])}, "
                f"21d {_fmt_pct(row['returns_21d'])}, "
                f"below 52w high {_fmt_plain_pct(row['pct_below_52w_high'])}"
            )
    else:
        lines.append("- No index ETF rows found yet for SPY, QQQ, DIA, or IWM.")

    lines.extend(["", "Discounts"])
    if digest["discounts"]:
        for row in digest["discounts"]:
            lines.append(
                "- "
                f"{row['symbol']}: {row['close']:.2f}, "
                f"{_fmt_plain_pct(row['pct_below_52w_high'])} below 52w high, "
                f"5d {_fmt_pct(row['returns_5d'])}, "
                f"21d {_fmt_pct(row['returns_21d'])}, "
                f"12-1 mom {_fmt_pct(row['momentum_12_1'])}, "
                f"21d vol {_fmt_plain_pct(row['realized_vol_21d'])}"
            )
    else:
        lines.append("- No discount candidates found in gold.prices.")

    lines.extend(["", "Random News"])
    if digest["headlines"]:
        for row in digest["headlines"]:
            symbol = row.get("symbol") or "market"
            published = _as_iso_day(row.get("provider_publish_time"))
            sentiment = row.get("sentiment") or "unscored"
            lines.append(f"- {symbol} ({published}, {sentiment}): {row['title']}")
            if row.get("link"):
                lines.append(f"  {row['link']}")
    else:
        lines.append("- No recent headlines found in gold.headlines.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = str(config.sender)
    message["To"] = ", ".join(config.recipients)
    message.set_content("\n".join(lines).rstrip() + "\n")
    return message


def send_weekly_digest_email(
    con,
    *,
    logger=None,
    config: EmailAlertConfig | None = None,
    as_of: date | None = None,
) -> int:
    alert_config = config or load_email_alert_config()
    if not alert_config.enabled:
        if logger is not None:
            logger.info("Weekly digest email suppressed because email alerting is not configured.")
        return 0
    digest = collect_weekly_digest_data(con, as_of=as_of)
    message = build_weekly_digest_email(digest=digest, config=alert_config)
    send_email_alert(message, alert_config)
    if logger is not None:
        logger.info("Sent weekly digest email.")
    return 1


@asset(name="weekly_digest_email", required_resource_keys={"duckdb"})
def weekly_digest_email(context: AssetExecutionContext) -> None:
    """
    Send a weekly plain-text market and pipeline digest email.
    """
    error_message = None
    try:
        sent_count = send_weekly_digest_email(
            context.resources.duckdb,
            logger=context.log,
        )
    except Exception as exc:
        sent_count = 0
        error_message = str(exc)
        context.log.warning("Weekly digest email failed: %s", exc)
    metadata: dict[str, Any] = {
        "sent_count": sent_count,
        "email_enabled": bool(sent_count),
    }
    if error_message:
        metadata["error_message"] = error_message
    context.add_output_metadata(metadata)
