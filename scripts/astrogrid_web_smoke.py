#!/usr/bin/env python3
import json
import sys
from pathlib import Path

from selenium import webdriver
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
    seer_reading = driver.find_element(By.CSS_SELECTOR, ".seer-reading-hero").text.strip()
    event_cards = driver.find_elements(By.CSS_SELECTOR, ".stage-side .panel:nth-child(2) .event-card")
    signal_cards = driver.find_elements(By.CSS_SELECTOR, ".stage-side .panel:nth-child(3) .event-card")
    hypothesis_cards = driver.find_elements(By.CSS_SELECTOR, ".hypothesis-card")
    summary_date = driver.find_element(By.CSS_SELECTOR, ".ag-summary-date").text.strip()
    focus_title = driver.find_element(By.CSS_SELECTOR, ".hero-meta-card .hero-branch-line").text.strip()
    return {
        "summary_date": summary_date,
        "seer_reading": seer_reading,
        "event_count": len(event_cards),
        "signal_count": len(signal_cards),
        "hypothesis_count": len(hypothesis_cards),
        "focus_title": focus_title,
    }


def main() -> int:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    driver = build_driver()
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(URL)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".brand-title")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".hero-grid")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ag-world-atlas svg")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ag-trajectory svg")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ag-spacetime svg")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ag-summary-date")))

        body = driver.find_element(By.TAG_NAME, "body").text
        if "BOOT FAULT" in body:
            raise AssertionError(body[:500])

        driver.find_element(By.CSS_SELECTOR, ".operator-summary").click()
        wait.until(EC.presence_of_element_located((By.ID, "api-base-input")))
        initial_date = driver.find_element(By.CSS_SELECTOR, ".ag-summary-date").text

        matrix_results = []
        for target_date in DATE_MATRIX:
            before = driver.find_element(By.CSS_SELECTOR, ".ag-summary-date").text
            dt_input = driver.find_element(By.ID, "dt-input")
            driver.execute_script(
                "arguments[0].value=arguments[1]; arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                dt_input,
                target_date,
            )
            wait.until(lambda current: current.find_element(By.CSS_SELECTOR, ".ag-summary-date").text != before)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".seer-reading-hero")))
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".hypothesis-card")))
            snapshot_state = collect_surface_snapshot(driver)
            if not snapshot_state["seer_reading"]:
                raise AssertionError(f"Missing Seer reading for {target_date}")
            if snapshot_state["event_count"] < 1:
                raise AssertionError(f"No events rendered for {target_date}")
            if snapshot_state["signal_count"] < 1:
                raise AssertionError(f"No signals rendered for {target_date}")
            if snapshot_state["hypothesis_count"] < 1:
                raise AssertionError(f"No hypotheses rendered for {target_date}")
            logs = severe_logs(driver)
            if logs:
                raise AssertionError(f"Severe browser logs after {target_date}: {logs}")
            matrix_results.append({
                "target": target_date,
                **snapshot_state,
            })

        driver.find_element(By.CSS_SELECTOR, '[data-world-node="moon"]').click()
        wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".hero-meta-card .hero-branch-line"), "Moon"))

        screenshot = ARTIFACT_DIR / "astrogrid_web_smoke.png"
        driver.save_screenshot(str(screenshot))

        final_severe_logs = severe_logs(driver)

        result = {
            "url": URL,
            "title": driver.title,
            "screenshot": str(screenshot),
            "initial_date": initial_date,
            "updated_date": driver.find_element(By.CSS_SELECTOR, ".ag-summary-date").text,
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
