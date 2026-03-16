"""
main.py — Daily Leads Dashboard pipeline orchestrator.

Run manually:
    python main.py

Or schedule (see README for cron / GitHub Actions / Cloud Run examples).
"""

import os
import sys
import traceback
from datetime import datetime
from typing import Optional

from src.config import TIMEZONE
from src.logger import get_logger
from src.hubspot_client import fetch_all_deals
from src.data_processor import build_dataframe, compute_metrics
from src.renderer import render_html, render_png
from src.notifier import send_to_chat, send_text_fallback

log = get_logger("main")


def run_pipeline(image_host_url: Optional[str] = None) -> None:
    """
    End-to-end pipeline:
      1. Fetch deals from HubSpot
      2. Build DataFrame & compute metrics
      3. Render HTML dashboard
      4. Screenshot HTML → PNG
      5. (Optional) Upload PNG to hosting
      6. Send to Google Chat
    """
    log.info("=" * 60)
    log.info("Pipeline started at %s", datetime.now().isoformat())
    log.info("Timezone: %s", TIMEZONE)

    # Step 1: Fetch data
    log.info("Step 1/5 — Fetching HubSpot deals ...")
    raw_deals = fetch_all_deals()
    log.info("Raw deals fetched: %d", len(raw_deals))

    if raw_deals:
        sample_props = raw_deals[0].get("properties", {})
        log.info("Sample deal ID: %s", raw_deals[0].get("id"))
        log.info("Sample deal properties keys: %s", list(sample_props.keys()))
        log.info("Sample createdate: %s", sample_props.get("createdate"))
        log.info("Sample pipeline: %s", sample_props.get("pipeline"))
        log.info("Sample qualified_lead: %s", sample_props.get("qualified_lead"))
        log.info("Sample department_source: %s", sample_props.get("department_source"))
        log.info("Sample segment_official: %s", sample_props.get("segment_official"))

    # Step 2: Process data
    log.info("Step 2/5 — Processing %d deals ...", len(raw_deals))
    df = build_dataframe(raw_deals)

    log.info("DataFrame rows: %d", len(df))

    if not df.empty:
        log.info("DataFrame columns: %s", df.columns.tolist())

        try:
            log.info("Non-null create_date rows: %d", int(df["create_date"].notna().sum()))
        except Exception:
            pass

        try:
            log.info(
                "Unique pipeline values: %s",
                sorted(df["pipeline"].dropna().astype(str).unique().tolist())
            )
        except Exception:
            pass

        try:
            log.info(
                "Unique dept_source_raw values: %s",
                sorted(df["dept_source_raw"].dropna().astype(str).unique().tolist())[:30]
            )
        except Exception:
            pass

        try:
            log.info(
                "Unique dept_source values: %s",
                sorted(df["dept_source"].dropna().astype(str).unique().tolist())
            )
        except Exception:
            pass

        try:
            log.info(
                "Unique segment values: %s",
                sorted(df["segment"].dropna().astype(str).unique().tolist())
            )
        except Exception:
            pass

        try:
            log.info("Qualified rows count: %d", int(df["is_qualified"].sum()))
        except Exception:
            pass

        try:
            log.info("Sample dataframe head:\n%s", df.head(10).to_string())
        except Exception:
            pass
    else:
        log.warning("DataFrame is empty after build_dataframe().")

    metrics = compute_metrics(df)

    log.info(
        "Metrics: total_today=%d qualified=%d rate=%.1f%%",
        metrics["total_today"],
        metrics["qualified_today"],
        metrics["ql_rate_today"],
    )

    try:
        log.info("Daily table rows: %s", metrics.get("table_daily"))
        log.info("MTD table rows: %s", metrics.get("table_mtd"))
        log.info("QTD table rows: %s", metrics.get("table_qtd"))
    except Exception:
        pass

    # Step 3: Render HTML
    log.info("Step 3/5 — Rendering HTML dashboard ...")
    html_path = render_html(metrics)
    log.info("HTML rendered at: %s", html_path)

    # Step 4: Screenshot to PNG
    log.info("Step 4/5 — Screenshotting HTML to PNG ...")
    try:
        png_path = render_png(html_path)
        log.info("PNG rendered at: %s", png_path)
    except Exception as exc:
        log.warning("PNG render failed (%s) — will send text-only.", exc)
        send_text_fallback(metrics)
        return

    # Step 5: Send to Google Chat
    log.info("Step 5/5 — Sending to Google Chat ...")

    if image_host_url:
        log.info("Using hosted image URL: %s", image_host_url)
        send_to_chat(metrics, image_url=image_host_url)
    else:
        log.info("No IMAGE_HOST_URL configured — sending text-only message.")
        send_text_fallback(metrics)

    log.info("Pipeline completed successfully.")
    log.info("=" * 60)


if __name__ == "__main__":
    image_url = os.getenv("IMAGE_HOST_URL")
    try:
        run_pipeline(image_host_url=image_url)
    except Exception:
        log.critical("Pipeline FAILED:\n%s", traceback.format_exc())
        sys.exit(1)