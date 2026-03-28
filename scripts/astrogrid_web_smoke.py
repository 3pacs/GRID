#!/usr/bin/env python3
import json
import sys
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


URL = "http://127.0.0.1:8011"
ARTIFACT_DIR = Path("/tmp/astrogrid_web_smoke")
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
DATE_MATRIX = [
    "2026-03-20T12:00",
    "2025-06-21T12:00",
    "2024-11-05T12:00",
    "2020-03-20T12:00",
    "2012-12-21T12:00",
    "2008-09-15T12:00",
]


def build_driver():
    options = Options()
    options.binary_location = CHROME_BIN
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    return webdriver.Chrome(options=options)


def severe_logs(driver):
    return [
        entry for entry in driver.get_log("browser")
        if entry.get("level") == "SEVERE"
    ]


def collect_surface_snapshot(driver):
    return driver.execute_script("""
        const text = (sel) => Array.from(document.querySelectorAll(sel)).map((el) => el.textContent.trim()).filter(Boolean);
        return {
          summary_date: document.querySelector('.ag-summary-date')?.textContent?.trim() || '',
          seer_reading: document.querySelector('.seer-reading-hero')?.textContent?.trim() || '',
          event_count: document.querySelectorAll('.stage-side .panel:nth-child(2) .event-card').length,
          signal_count: document.querySelectorAll('.stage-side .panel:nth-child(3) .event-card').length,
          hypothesis_count: document.querySelectorAll('.hypothesis-card').length,
          event_texts: text('.stage-side .panel:nth-child(2) .event-card'),
          signal_texts: text('.stage-side .panel:nth-child(3) .event-card'),
          hypothesis_texts: text('.hypothesis-card'),
        };
    """)


def collect_atlas_snapshot(driver):
    return driver.execute_script("""
        const cards = Array.from(document.querySelectorAll('.hero-meta-card'));
        const byLabel = (label) => cards.find((el) => (el.querySelector('.section-label')?.textContent || '').trim().toLowerCase() === label);
        const lines = (node) => node ? node.textContent.split('\\n').map((part) => part.trim()).filter(Boolean) : [];
        const focusLines = lines(byLabel('focus'));
        const scoreLines = lines(byLabel('score'));
        const flowLines = lines(byLabel('attached flows'));
        return {
          focus_title: focusLines[1] || '',
          focus_score: scoreLines[1] || '',
          focus_signal: scoreLines[2] || '',
          flow_text: flowLines[1] || '',
        };
    """)


def open_page(driver, wait, page_id, selector):
    last_error = None
    for _ in range(4):
        try:
            button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f'[data-page="{page_id}"]')))
            driver.execute_script("arguments[0].click();", button)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            return
        except StaleElementReferenceException as exc:
            last_error = exc
    if last_error:
        raise last_error


def main() -> int:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    driver = build_driver()
    wait = WebDriverWait(driver, 30)

    try:
        driver.get(URL)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".brand-title")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".page-nav")))
        open_page(driver, wait, "oracle", ".seer-reading-hero")
        wait.until(lambda current: bool(collect_surface_snapshot(current)["summary_date"]))

        body = driver.find_element(By.TAG_NAME, "body").text
        if "BOOT FAULT" in body:
            raise AssertionError(body[:500])
        initial_date = collect_surface_snapshot(driver)["summary_date"]

        matrix_results = []
        for target_date in DATE_MATRIX:
            open_page(driver, wait, "chamber", "#dt-input")
            dt_input = driver.find_element(By.ID, "dt-input")
            driver.execute_script(
                "arguments[0].value=arguments[1]; arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                dt_input,
                target_date,
            )
            open_page(driver, wait, "oracle", ".seer-reading-hero")
            wait.until(lambda current: collect_surface_snapshot(current)["summary_date"] == target_date)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".seer-reading-hero")))
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".hypothesis-card")))
            snapshot_state = collect_surface_snapshot(driver)
            if not snapshot_state["seer_reading"]:
                raise AssertionError(f"Missing Seer reading for {target_date}")
            if snapshot_state["hypothesis_count"] < 1:
                raise AssertionError(f"No hypotheses rendered for {target_date}")
            if target_date.startswith("2008-09-15"):
                joined = " ".join(snapshot_state["signal_texts"] + snapshot_state["hypothesis_texts"]).lower()
                if "full moon active" not in joined and "through full moon" not in joined:
                    raise AssertionError(f"Full moon logic did not surface clearly for {target_date}: {joined}")
            if target_date.startswith("2026-03-20"):
                joined = " ".join(snapshot_state["hypothesis_texts"]).lower()
                if "through new moon" not in joined:
                    raise AssertionError(f"New moon action did not bind to the live event for {target_date}: {joined}")

            open_page(driver, wait, "atlas", ".ag-world-atlas svg")
            atlas_state = collect_atlas_snapshot(driver)
            if not atlas_state["focus_title"]:
                raise AssertionError(f"No atlas focus readout for {target_date}")
            if not atlas_state["focus_score"]:
                raise AssertionError(f"No atlas focus score for {target_date}")

            logs = severe_logs(driver)
            if logs:
                raise AssertionError(f"Severe browser logs after {target_date}: {logs}")
            matrix_results.append({
                "target": target_date,
                **snapshot_state,
                **atlas_state,
            })

        open_page(driver, wait, "observatory", ".ag-trajectory svg")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ag-spacetime svg")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ag-aspect-field svg")))

        open_page(driver, wait, "atlas", ".ag-world-atlas svg")
        driver.find_element(By.CSS_SELECTOR, '[data-world-node="moon"]').click()
        wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".hero-meta-card .hero-branch-line"), "Moon"))

        screenshot = ARTIFACT_DIR / "astrogrid_web_smoke.png"
        driver.save_screenshot(str(screenshot))

        final_severe_logs = severe_logs(driver)
        score_values = {item["focus_score"] for item in matrix_results if item["focus_score"]}
        if len(score_values) < 2:
            raise AssertionError(f"World score stayed static across archive dates: {sorted(score_values)}")

        result = {
            "url": URL,
            "title": driver.title,
            "screenshot": str(screenshot),
            "initial_date": initial_date,
            "updated_date": matrix_results[-1]["summary_date"] if matrix_results else initial_date,
            "severe_logs": final_severe_logs,
            "matrix_results": matrix_results,
            "body_excerpt": body[:600],
        }
        (ARTIFACT_DIR / "result.json").write_text(json.dumps(result, indent=2))

        if final_severe_logs:
            raise AssertionError(f"Severe browser logs: {final_severe_logs}")

        print(json.dumps(result, indent=2))
        return 0
    finally:
        driver.quit()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"astrogrid_web_smoke failed: {exc}", file=sys.stderr)
        raise
