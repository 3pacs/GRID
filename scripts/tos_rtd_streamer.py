#!/usr/bin/env python3
"""ThinkOrSwim RTD Streamer Client.

Runs on the Windows Workstation hosting the ThinkOrSwim client and Microsoft Excel.
Polls the active Excel sheet containing TOS RTD formulas and streams updates to grid-svr.

Requires:
    pip install xlwings requests
"""

import sys
import time
import argparse
import requests

try:
    import xlwings as xw
except ImportError:
    print("Error: xlwings is not installed. Install it with: pip install xlwings")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="ThinkOrSwim RTD Streamer Client")
    parser.add_argument(
        "--file",
        default="tos_rtd_streamer.xlsx",
        help="Name of the Excel file containing ToS RTD formulas",
    )
    parser.add_argument(
        "--url",
        default="https://grid.stepdad.finance/api/v1/tradingview/webhook",
        help="Target GRID API endpoint URL for streaming signals",
    )
    parser.add_argument(
        "--key",
        default="",
        help="API Key for target webhook authentication",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--range",
        default="A2:C20",
        help="Excel range containing Ticker, Field, and Value columns (default: A2:C20)",
    )
    return parser.parse_args()


def stream_quotes(args):
    print(f"Connecting to Excel workbook: {args.file}...")
    try:
        # Connect to the active Excel workbook matching the filename
        wb = xw.Book(args.file)
        sheet = wb.sheets[0]
        print(f"Connected to sheet: '{sheet.name}' successfully.")
    except Exception as exc:
        print(f"Error connecting to workbook '{args.file}': {exc}")
        print("Please verify Excel is running and the workbook is open.")
        sys.exit(1)

    print(f"Streaming data from range {args.range} to {args.url} every {args.interval}s...")
    print("Press Ctrl+C to stop.")

    headers = {}
    if args.key:
        headers["X-API-Key"] = args.key

    last_values = {}

    while True:
        try:
            # Read range (returns list of lists)
            rows = sheet.range(args.range).value
            
            for row in rows:
                if not row or len(row) < 3:
                    continue
                
                ticker = row[0]
                field = row[1]
                val = row[2]

                # Skip empty lines or header rows
                if not ticker or not field or val is None:
                    continue

                # Clean fields
                ticker = str(ticker).strip().upper()
                field = str(field).strip().upper()
                try:
                    val = float(val)
                except ValueError:
                    # Skip if the RTD value is not yet loaded / is a string error
                    continue

                key = f"{ticker}:{field}"
                
                # Check for changes to avoid redundant posts
                if last_values.get(key) == val:
                    continue

                last_values[key] = val
                print(f"[{time.strftime('%H:%M:%S')}] {ticker} - {field}: {val}")

                # Format payload similar to Pine Script webhook alerts
                payload = {
                    "ticker": ticker,
                    "signal_type": field,
                    "value": val,
                    "source": "thinkorswim_rtd",
                    "timestamp": time.time(),
                }

                try:
                    resp = requests.post(args.url, json=payload, headers=headers, timeout=5)
                    if resp.status_code != 200:
                        print(f"Warning: Failed to post {key} ({resp.status_code}): {resp.text}")
                except Exception as post_exc:
                    print(f"Connection error posting {key}: {post_exc}")

            time.sleep(args.interval)

        except KeyboardInterrupt:
            print("\nStreaming stopped by user.")
            break
        except Exception as loop_exc:
            print(f"Error during streaming loop: {loop_exc}")
            time.sleep(5)  # Back off before retrying


if __name__ == "__main__":
    stream_quotes(parse_args())
