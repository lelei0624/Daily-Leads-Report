"""
hubspot_client.py — Pulls deal data from HubSpot using the v3 Search API.
"""

import time
from typing import Any

import requests

from config import HUBSPOT_API_KEY, TARGET_PIPELINES, DEAL_PROPERTIES
from logger import get_logger

log = get_logger("hubspot_client")

BASE_URL = "https://api.hubapi.com/crm/v3/objects/deals/search"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}

PAGE_SIZE = 100
MAX_PAGES = 1000
START_DATE = "1735689600000"  # 2025-01-01 00:00:00 UTC in milliseconds


def _build_payload(after: str | None = None) -> dict[str, Any]:
    filter_groups = [
        {
            "filters": [
                {
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": str(pipeline),
                },
                {
                    "propertyName": "createdate",
                    "operator": "GTE",
                    "value": START_DATE,
                },
            ]
        }
        for pipeline in TARGET_PIPELINES
    ]

    payload: dict[str, Any] = {
        "filterGroups": filter_groups,
        "properties": DEAL_PROPERTIES,
        "limit": PAGE_SIZE,
        "sorts": ["-createdate"],
    }

    if after:
        payload["after"] = after

    return payload


def fetch_all_deals() -> list[dict]:
    all_deals: list[dict] = []
    after: str | None = None
    page = 0

    while page < MAX_PAGES:
        payload = _build_payload(after)
        log.debug("Fetching page %d (after=%s)", page + 1, after)

        try:
            resp = requests.post(
                BASE_URL,
                headers=HEADERS,
                json=payload,
                timeout=30,
            )

            if not resp.ok:
                log.error("HubSpot API error on page %d", page + 1)
                log.error("Status code: %s", resp.status_code)
                log.error("Response body: %s", resp.text)

            resp.raise_for_status()

        except requests.RequestException as exc:
            log.error("HubSpot API error on page %d: %s", page + 1, exc)
            raise

        data = resp.json()
        results = data.get("results", [])
        all_deals.extend(results)

        log.info(
            "Page %d -> %d deals fetched (total so far: %d)",
            page + 1,
            len(results),
            len(all_deals),
        )

        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        page += 1

        if not after:
            break

        time.sleep(0.1)

    log.info("Done fetching. Total deals: %d", len(all_deals))
    return all_deals
