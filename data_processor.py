"""
data_processor.py — Transforms raw HubSpot deal records into the metrics
required by the dashboard template.

All report/date logic uses the configured TIMEZONE so the numbers
match what your Manila-based team sees.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.config import (
    DEPT_SOURCE_MAP,
    DEPT_SOURCE_DISPLAY_ORDER,
    FALLBACK_TARGETS,
    QUALIFIED_VALUES,
    SEGMENTS,
    TARGETS_GSHEET_CSV_URL,
    TIMEZONE,
)
from src.logger import get_logger

log = get_logger("data_processor")
TZ = ZoneInfo(TIMEZONE)


def _hubspot_to_date(value):
    if not value:
        return None

    try:
        s = str(value).strip()

        if s.isdigit():
            dt = datetime.fromtimestamp(int(s) / 1000, tz=ZoneInfo("UTC"))
            return dt.astimezone(TZ).date()

        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")

        dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))

        return dt.astimezone(TZ).date()

    except Exception:
        return None


def _map_dept(raw):
    if not raw:
        return "Unknown"
    return DEPT_SOURCE_MAP.get(str(raw).strip(), "Unknown")


def _is_qualified(raw):
    if not raw:
        return False
    return str(raw).strip().lower() in QUALIFIED_VALUES


def _normalize_segment(raw):
    if not raw:
        return ""

    s = str(raw).strip().upper()
    mapping = {
        "MICRO": "MICRO",
        "SME": "SME",
        "ENT": "ENT",
        "ENTERPRISE": "ENT",
    }
    return mapping.get(s, s)


def _quarter_start(d: date) -> date:
    month = ((d.month - 1) // 3) * 3 + 1
    return d.replace(month=month, day=1)


def _load_targets():
    """
    Load targets from Google Sheet CSV.
    Expected columns: period, dept_source, target

    Falls back to FALLBACK_TARGETS if sheet is missing/unavailable.
    """
    if not TARGETS_GSHEET_CSV_URL:
        log.warning("TARGETS_GSHEET_CSV_URL not set. Using fallback targets.")
        return FALLBACK_TARGETS

    try:
        tdf = pd.read_csv(TARGETS_GSHEET_CSV_URL)

        required_cols = {"period", "dept_source", "target"}
        missing = required_cols - set(tdf.columns)
        if missing:
            raise ValueError("Missing target columns: %s" % ", ".join(sorted(missing)))

        tdf["period"] = tdf["period"].astype(str).str.strip().str.lower()
        tdf["dept_source"] = tdf["dept_source"].astype(str).str.strip()
        tdf["target"] = pd.to_numeric(tdf["target"], errors="coerce").fillna(0).astype(int)

        targets = {"daily": {}, "mtd": {}, "qtd": {}}

        for _, row in tdf.iterrows():
            period = row["period"]
            dept = row["dept_source"]
            target = int(row["target"])

            if period in targets:
                targets[period][dept] = target

        log.info("Targets loaded from Google Sheet successfully.")
        return targets

    except Exception as exc:
        log.warning("Failed to load targets from Google Sheet (%s). Using fallback targets.", exc)
        return FALLBACK_TARGETS


def build_dataframe(raw_deals):
    rows = []

    for deal in raw_deals:
        props = deal.get("properties", {})
        raw_segment = props.get("segment_official") or props.get("segment")

        rows.append({
            "deal_id": deal.get("id"),
            "create_date": _hubspot_to_date(props.get("createdate")),
            "pipeline": props.get("pipeline", ""),
            "dept_source_raw": props.get("department_source", ""),
            "dept_source": _map_dept(props.get("department_source")),
            "is_qualified": _is_qualified(props.get("qualified_lead")),
            "segment_raw": (raw_segment or "").strip(),
            "segment": _normalize_segment(raw_segment),
            "amount": float(props.get("amount") or 0),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        log.warning("DataFrame is empty after build_dataframe().")
        return df

    df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce").dt.normalize()

    log.info("DataFrame built: %d rows", len(df))
    log.info("Non-null create_date rows: %d", int(df["create_date"].notna().sum()))

    try:
        log.info("Raw segment values: %s", sorted(df["segment_raw"].dropna().astype(str).unique().tolist()))
        log.info("Normalized segment values: %s", sorted(df["segment"].dropna().astype(str).unique().tolist()))
        log.info("Qualified rows count: %d", int(df["is_qualified"].sum()))
    except Exception:
        pass

    return df


def _seg_breakdown(df):
    if df.empty:
        return {"MICRO": 0, "SME": 0, "ENT": 0, "TOTAL": 0}

    q_df = df[df["is_qualified"]].copy()

    result = {}
    for seg in ["MICRO", "SME", "ENT"]:
        result[seg] = int((q_df["segment"] == seg).sum())

    result["TOTAL"] = int(len(q_df))
    return result


def _build_table(df, target_map):
    rows = []

    for dept in DEPT_SOURCE_DISPLAY_ORDER:
        dept_df = df[df["dept_source"] == dept]
        breakdown = _seg_breakdown(dept_df)

        target = int(target_map.get(dept, 0))
        total = int(breakdown["TOTAL"])
        pct_to_goal = round((total / target) * 100, 0) if target else 0

        rows.append({
            "dept": dept,
            **breakdown,
            "Target": target,
            "%toGoal": f"{int(pct_to_goal)}%",
        })

    total_target = sum(int(target_map.get(dept, 0)) for dept in DEPT_SOURCE_DISPLAY_ORDER)
    totals = {seg: sum(r[seg] for r in rows) for seg in SEGMENTS}
    total_total = int(totals["TOTAL"])
    total_pct = round((total_total / total_target) * 100, 0) if total_target else 0

    rows.append({
        "dept": "Total",
        **totals,
        "Target": total_target,
        "%toGoal": f"{int(total_pct)}%",
    })

    return rows


def compute_metrics(df):
    """
    Daily  = based on reporting filter
             - Monday: Fri-Sun
             - Other days: Yesterday

    MTD    = from month start up to report_end
    QTD    = from quarter start up to report_end

    KPI cards use the SAME daily filter.
    """
    targets = _load_targets()

    if df.empty:
        today = datetime.now(tz=TZ).date()
        return {
            "report_label": "Yesterday",
            "report_date": today.strftime("%B %d, %Y"),
            "qualified_today": 0,
            "ql_rate_today": 0.0,
            "trend_14": [],
            "trend_14_dates": [],
            "trend_14_total": [],
            "trend_14_qualified": [],
            "dept_trend_14": {dept: [] for dept in DEPT_SOURCE_DISPLAY_ORDER},
            "dept_sources": DEPT_SOURCE_DISPLAY_ORDER,
            "rolling_months": [],
            "table_daily": _build_table(pd.DataFrame(columns=["dept_source", "is_qualified", "segment"]), targets["daily"]),
            "table_mtd": _build_table(pd.DataFrame(columns=["dept_source", "is_qualified", "segment"]), targets["mtd"]),
            "table_qtd": _build_table(pd.DataFrame(columns=["dept_source", "is_qualified", "segment"]), targets["qtd"]),
            "segments": SEGMENTS,
            "total_today": 0,
        }

    df = df.copy()
    df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce").dt.normalize()

    today = datetime.now(tz=TZ).date()
    today_pd = pd.Timestamp(today).normalize()
    weekday = today.weekday()

    # Reporting window
    if weekday == 0:
        report_start = (today_pd - pd.Timedelta(days=3)).normalize()  # Friday
        report_end = (today_pd - pd.Timedelta(days=1)).normalize()    # Sunday
        report_label = "Fri-Sun"
        report_date = f"{report_start.strftime('%B %d, %Y')} to {report_end.strftime('%B %d, %Y')}"
    else:
        report_start = (today_pd - pd.Timedelta(days=1)).normalize()
        report_end = report_start
        report_label = "Yesterday"
        report_date = report_end.strftime("%B %d, %Y")

    # Daily = based on reporting filter
    daily_df = df[
        (df["create_date"] >= report_start) &
        (df["create_date"] <= report_end)
    ]

    # KPI cards must use daily_df
    total_today = len(daily_df)
    qualified_today = int(daily_df["is_qualified"].sum())
    ql_rate_today = (qualified_today / total_today * 100) if total_today else 0

    # 14-day trend ending at report_end
    trend_14 = []
    trend_end = report_end

    for i in range(13, -1, -1):
        d = (trend_end - pd.Timedelta(days=i)).normalize()
        day_df = df[df["create_date"] == d]

        trend_14.append({
            "date": d.strftime("%b %d"),
            "total": len(day_df),
            "qualified": int(day_df["is_qualified"].sum()),
        })

    trend_14_dates = [x["date"] for x in trend_14]
    trend_14_total = [x["total"] for x in trend_14]
    trend_14_qualified = [x["qualified"] for x in trend_14]

    dept_trend_14 = {dept: [] for dept in DEPT_SOURCE_DISPLAY_ORDER}
    for i in range(13, -1, -1):
        d = (trend_end - pd.Timedelta(days=i)).normalize()
        for dept in DEPT_SOURCE_DISPLAY_ORDER:
            d_df = df[
                (df["create_date"] == d) &
                (df["dept_source"] == dept)
            ]
            dept_trend_14[dept].append(int(d_df["is_qualified"].sum()))

    # MTD = month start to report_end
    month_start = pd.Timestamp(year=report_end.year, month=report_end.month, day=1).normalize()
    mtd_df = df[
        (df["create_date"] >= month_start) &
        (df["create_date"] <= report_end)
    ]

    # QTD = quarter start to report_end
    qstart_date = _quarter_start(report_end.date())
    qtd_start = pd.Timestamp(qstart_date).normalize()
    qtd_df = df[
        (df["create_date"] >= qtd_start) &
        (df["create_date"] <= report_end)
    ]

    # Rolling 3 months = qualified leads by segment
    rolling_df = df.copy()
    rolling_df["month"] = rolling_df["create_date"].dt.to_period("M")

    months = sorted(rolling_df["month"].dropna().unique())[-3:]
    rolling_months = []

    for m in months:
        m_df = rolling_df[rolling_df["month"] == m]
        rolling_months.append({
            "label": m.strftime("%b %Y"),
            "total": _seg_breakdown(m_df),
        })

    # Debug
    log.info("REPORT START: %s", report_start)
    log.info("REPORT END: %s", report_end)
    log.info("DAILY qualified: %d", int(daily_df["is_qualified"].sum()))
    log.info("MTD qualified: %d", int(mtd_df["is_qualified"].sum()))
    log.info("QTD qualified: %d", int(qtd_df["is_qualified"].sum()))

    return {
        "report_label": report_label,
        "report_date": report_date,
        "qualified_today": qualified_today,
        "ql_rate_today": round(ql_rate_today, 1),
        "trend_14": trend_14,
        "trend_14_dates": trend_14_dates,
        "trend_14_total": trend_14_total,
        "trend_14_qualified": trend_14_qualified,
        "dept_trend_14": dept_trend_14,
        "dept_sources": DEPT_SOURCE_DISPLAY_ORDER,
        "rolling_months": rolling_months,
        "table_daily": _build_table(daily_df, targets["daily"]),
        "table_mtd": _build_table(mtd_df, targets["mtd"]),
        "table_qtd": _build_table(qtd_df, targets["qtd"]),
        "segments": SEGMENTS,
        "total_today": total_today,
    }