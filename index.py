"""
index.py — Vercel serverless entry point (root level, flat imports).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from http.server import BaseHTTPRequestHandler

from hubspot_client import fetch_all_deals
from data_processor import build_dataframe, compute_metrics
from renderer import render_html


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            raw_deals = fetch_all_deals()
            df = build_dataframe(raw_deals)
            metrics = compute_metrics(df)
            html_path = render_html(metrics)

            with open(html_path, "r", encoding="utf-8") as f:
                html = f.read()

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))

        except Exception as exc:
            import traceback
            error_html = f"""
            <html><body style="font-family:monospace;background:#0a0e1a;color:#f43f5e;padding:32px;">
                <h2>Pipeline Error</h2>
                <pre>{traceback.format_exc()}</pre>
            </body></html>
            """
            self.send_response(500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(error_html.encode("utf-8"))

    def log_message(self, format, *args):
        pass
