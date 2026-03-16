"""
renderer.py — Renders the HTML dashboard to a PNG screenshot using Playwright.
"""

import json
import os
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from src.config import OUTPUT_HTML, OUTPUT_PNG, TEMPLATE_PATH
from src.logger import get_logger

log = get_logger("renderer")


def render_html(context: dict) -> str:
    """Fill the Jinja2 template and write it to OUTPUT_HTML."""
    template_dir = os.path.dirname(TEMPLATE_PATH)
    template_file = os.path.basename(TEMPLATE_PATH)

    if not os.path.exists(TEMPLATE_PATH):
        raise FileNotFoundError(f"Template not found: {TEMPLATE_PATH}")

    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
    )

    # Register to_json filter so {{ variable | to_json }} works in the template
    env.filters["to_json"] = lambda v: json.dumps(v)

    template = env.get_template(template_file)
    html = template.render(**context)

    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    log.info("HTML written to %s", OUTPUT_HTML)
    return OUTPUT_HTML


def render_png(html_path: str = OUTPUT_HTML, png_path: str = OUTPUT_PNG) -> str:
    """
    Use Playwright (headless Chromium) to screenshot the dashboard.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Run: python -m pip install playwright "
            "and then: python -m playwright install chromium"
        ) from exc

    os.makedirs(os.path.dirname(png_path), exist_ok=True)
    abs_path = Path(html_path).resolve().as_uri()

    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.goto(abs_path, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(1500)
        page.screenshot(path=png_path, full_page=True)
        browser.close()

    log.info("PNG screenshot saved to %s", png_path)
    return png_path