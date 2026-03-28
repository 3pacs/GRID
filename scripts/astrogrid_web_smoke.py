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


def build_driver():
    options = Options()
    options.binary_location = CHROME_BIN
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    return webdriver.Chrome(options=options)


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
        dt_input = driver.find_element(By.ID, "dt-input")
        initial_date = driver.find_element(By.CSS_SELECTOR, ".ag-summary-date").text
        driver.execute_script(
            "arguments[0].value='2026-03-20T12:00'; arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            dt_input,
        )
        wait.until(lambda current: current.find_element(By.CSS_SELECTOR, ".ag-summary-date").text != initial_date)

        driver.find_element(By.CSS_SELECTOR, '[data-world-node="moon"]').click()
        wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".hero-meta-card .hero-branch-line"), "Moon"))

        screenshot = ARTIFACT_DIR / "astrogrid_web_smoke.png"
        driver.save_screenshot(str(screenshot))

        browser_logs = driver.get_log("browser")
        severe_logs = [
            entry for entry in browser_logs
            if entry.get("level") == "SEVERE"
        ]

        result = {
            "url": URL,
            "title": driver.title,
            "screenshot": str(screenshot),
            "initial_date": initial_date,
            "updated_date": driver.find_element(By.CSS_SELECTOR, ".ag-summary-date").text,
            "severe_logs": severe_logs,
            "body_excerpt": body[:600],
        }
        (ARTIFACT_DIR / "result.json").write_text(json.dumps(result, indent=2))

        if severe_logs:
            raise AssertionError(f"Severe browser logs: {severe_logs}")

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
