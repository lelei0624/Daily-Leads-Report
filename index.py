"""
index.py — Vercel serverless entry point.
Exposes both `app` (WSGI) and `handler` (BaseHTTPRequestHandler) so Vercel
can detect it regardless of which runtime it uses.
"""

import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(__file__))

from hubspot_client import fetch_all_deals
from data_processor import build_dataframe, compute_metrics
from renderer import render_html


def _get_dashboard_html() -> str:
    """Fetch data, compute metrics, render HTML and return as string."""
    raw_deals = fetch_all_deals()
    df        = build_dataframe(raw_deals)
    metrics   = compute_metrics(df)
    html_path = render_html(metrics)

    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


# ── WSGI app (Vercel Python runtime picks this up) ─────────────────────────
def app(environ, start_response):
    try:
        html = _get_dashboard_html()
        body = html.encode("utf-8")
        start_response("200 OK", [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Cache-Control", "no-store"),
            ("Content-Length", str(len(body))),
        ])
        return [body]

    except Exception:
        error = traceback.format_exc()
        error_html = f"""<!DOCTYPE html>
<html><body style="font-family:monospace;background:#0a0e1a;color:#f43f5e;padding:32px;">
  <h2>Pipeline Error</h2><pre>{error}</pre>
</body></html>""".encode("utf-8")
        start_response("500 Internal Server Error", [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(error_html))),
        ])
        return [error_html]


# ── BaseHTTPRequestHandler (fallback) ──────────────────────────────────────
from http.server import BaseHTTPRequestHandler

class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            html = _get_dashboard_html()
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        except Exception:
            error = traceback.format_exc()
            error_html = f"""<!DOCTYPE html>
<html><body style="font-family:monospace;background:#0a0e1a;color:#f43f5e;padding:32px;">
  <h2>Pipeline Error</h2><pre>{error}</pre>
</body></html>""".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(error_html)))
            self.end_headers()
            self.wfile.write(error_html)

    def log_message(self, format, *args):
        pass
