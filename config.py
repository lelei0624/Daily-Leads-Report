"""
config.py — Centralised configuration for the Daily Leads Dashboard pipeline.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── HubSpot ────────────────────────────────────────────────────────────────
HUBSPOT_API_KEY = os.environ["HUBSPOT_API_KEY"]

TARGET_PIPELINES = [
    "95256425",   # [PH] Sales Pipeline
    "143428957",  # [PH] Unified Channels Pipeline
    "784069965",  # [PH] Upsell Pipeline
]

DEAL_PROPERTIES = [
    "dealname",
    "pipeline",
    "dealstage",
    "createdate",
    "qualified_lead",
    "department_source",
    "amount",
    "segment_official",
]

# ── Department Source Mapping ──────────────────────────────────────────────
DEPT_SOURCE_MAP = {
    "MKT Inbound": "MKT Inbound",
    "In-house Account": "MKT Inbound",
    "CSM Inbound": "MKT Inbound",
    "LDU Outbound": "LDU Outbound",
    "SDR Outbound": "LDU Outbound",
    "Channels": "Channels",
    "Sales Outbound": "Sales Outbound",
}

DEPT_SOURCE_DISPLAY_ORDER = [
    "MKT Inbound",
    "LDU Outbound",
    "Channels",
    "Sales Outbound",
]

SEGMENTS = ["MICRO", "SME", "ENT", "TOTAL"]
QUALIFIED_VALUES = {"yes", "true", "1"}

# ── Targets ────────────────────────────────────────────────────────────────
TARGETS_GSHEET_CSV_URL = os.getenv("TARGETS_GSHEET_CSV_URL", "").strip()

FALLBACK_TARGETS = {
    "daily": {
        "MKT Inbound": 8,
        "LDU Outbound": 11,
        "Sales Outbound": 1,
        "Channels": 1,
    },
    "mtd": {
        "MKT Inbound": 169,
        "LDU Outbound": 230,
        "Sales Outbound": 22,
        "Channels": 26,
    },
    "qtd": {
        "MKT Inbound": 505,
        "LDU Outbound": 690,
        "Sales Outbound": 67,
        "Channels": 77,
    },
}

# ── Google Chat ────────────────────────────────────────────────────────────
GOOGLE_CHAT_WEBHOOK_URL = os.environ["GOOGLE_CHAT_WEBHOOK_URL"]

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(__file__)

TEMPLATE_PATH = os.path.join(PROJECT_ROOT, "templates", "dashboard.html.j2")
OUTPUT_HTML   = "/tmp/dashboard.html"
OUTPUT_PNG    = "/tmp/dashboard.png"
LOG_PATH      = os.path.join(PROJECT_ROOT, "logs", "pipeline.log")

# ── Timezone ───────────────────────────────────────────────────────────────
TIMEZONE = os.getenv("TIMEZONE", "Asia/Manila")
