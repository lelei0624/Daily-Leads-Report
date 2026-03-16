"""
notifier.py — Sends the daily dashboard to a Google Chat space.
"""

import json
from datetime import datetime

import requests

from config import GOOGLE_CHAT_WEBHOOK_URL
from logger import get_logger

log = get_logger("notifier")


def _build_text_payload(metrics: dict) -> dict:
    date  = metrics.get("report_date", "")
    label = metrics.get("report_label", "Reported Period")
    qualified = metrics.get("qualified_today", 0)
    rate  = metrics.get("ql_rate_today", 0)

    text = (
        f"📊 *Daily Leads Report — {date}*\n\n"
        f"• Qualified Leads ({label}): *{qualified}*\n"
        f"• Qualified Lead Rate: *{rate}%*\n\n"
        f"_Pipelines: [PH] Sales · Unified Channels · Upsell_"
    )
    return {"text": text}


def _build_card_payload(metrics: dict, image_url: str) -> dict:
    date  = metrics.get("report_date", "")
    label = metrics.get("report_label", "Reported Period")
    qualified = metrics.get("qualified_today", 0)
    rate  = metrics.get("ql_rate_today", 0)

    return {
        "cardsV2": [
            {
                "cardId": "daily-leads-report",
                "card": {
                    "header": {
                        "title": "Daily Leads Report",
                        "subtitle": date,
                        "imageUrl": "https://fonts.gstatic.com/s/i/short-term/release/materialsymbolsoutlined/analytics/default/48px.svg",
                        "imageType": "CIRCLE",
                    },
                    "sections": [
                        {
                            "header": f"{label} KPIs",
                            "widgets": [
                                {
                                    "columns": {
                                        "columnItems": [
                                            _kpi_widget(f"Qualified Leads ({label})", str(qualified)),
                                            _kpi_widget("QL Rate", f"{rate}%"),
                                        ]
                                    }
                                }
                            ],
                        },
                        {
                            "header": "Dashboard",
                            "widgets": [
                                {
                                    "image": {
                                        "imageUrl": image_url,
                                        "altText": "Daily Leads Dashboard",
                                    }
                                }
                            ],
                        },
                    ],
                },
            }
        ]
    }


def _kpi_widget(label: str, value: str) -> dict:
    return {
        "widgets": [
            {
                "decoratedText": {
                    "topLabel": label,
                    "text": f"<b>{value}</b>",
                }
            }
        ]
    }


def send_to_chat(metrics: dict, image_url: str = None) -> None:
    if image_url:
        payload = _build_card_payload(metrics, image_url)
    else:
        payload = _build_text_payload(metrics)
        log.warning("No image URL provided — sending text-only summary.")

    try:
        resp = requests.post(
            GOOGLE_CHAT_WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=15,
        )
        resp.raise_for_status()
        log.info("Google Chat message sent successfully (status %d)", resp.status_code)
    except requests.RequestException as exc:
        log.error("Failed to send Google Chat message: %s", exc)
        raise


def send_text_fallback(metrics: dict) -> None:
    send_to_chat(metrics, image_url=None)
