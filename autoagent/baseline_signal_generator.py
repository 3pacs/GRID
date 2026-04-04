"""
Baseline EOG Signal Generator — Starting Point for AutoAgent.

This is what iteration 0 looks like. The meta-agent will iterate on agent.py
to produce better versions of this script. Included here as a reference
and for manual testing.

Usage:
    python baseline_signal_generator.py
"""

from __future__ import annotations

import sys
sys.path.insert(0, "/app/files")

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler

from grid_bridge import GridBridge


def main():
    bridge = GridBridge()

    # --- 1. Pull data ---
    eog = bridge.get_eog_prices(start_date="2005-01-01")
    eog = eog.set_index("obs_date")

    # Core features: oil, credit, vol, rates, sector
    feature_names = [
        "cl_close",               # Crude oil
        "vix_spot",               # Volatility
        "ofr_financial_stress",   # Financial stress index
        "tlt_full",               # Treasury (rate expectations)
        "hyg_full",               # High yield credit
        "lqd_full",               # Investment grade credit
        "xle_full",               # Energy sector ETF
        "gld_full",               # Gold (risk proxy)
        "uso_full",               # Oil ETF
        "dvn_full",               # Devon Energy (peer)
    ]

    features = bridge.get_features(feature_names, start_date="2005-01-01")

    # --- 2. Merge and engineer features ---
    df = eog[["close"]].join(features, how="inner")
    df = df.sort_index()

    # Forward return target (78 calendar days ≈ 55 trading days)
    df["fwd_return"] = df["close"].shift(-55) / df["close"] - 1
    df["target"] = (df["fwd_return"] > 0).astype(int)

    # Feature engineering
    for col in feature_names:
        if col in df.columns:
            # 1-day lag (prevent look-ahead)
            df[col] = df[col].shift(1)

            # Rolling z-score (63-day window)
            rolling_mean = df[col].rolling(63).mean()
            rolling_std = df[col].rolling(63).std()
            df[f"{col}_zscore"] = (df[col] - rolling_mean) / rolling_std.replace(0, np.nan)

            # 21-day momentum
            df[f"{col}_mom21"] = df[col].pct_change(21)

    # Credit spread proxy
    if "hyg_full" in df.columns and "lqd_full" in df.columns:
        df["credit_spread"] = df["hyg_full"] / df["lqd_full"]
        df["credit_spread_zscore"] = (
            (df["credit_spread"] - df["credit_spread"].rolling(63).mean())
            / df["credit_spread"].rolling(63).std().replace(0, np.nan)
        )

    # EOG relative to sector
    if "xle_full" in df.columns:
        df["eog_vs_xle"] = df["close"].shift(1) / df["xle_full"]
        df["eog_vs_xle_zscore"] = (
            (df["eog_vs_xle"] - df["eog_vs_xle"].rolling(63).mean())
            / df["eog_vs_xle"].rolling(63).std().replace(0, np.nan)
        )

    # Drop rows with NaN
    df = df.dropna()

    # Select model features (exclude raw prices, target, forward return)
    model_features = [c for c in df.columns if c.endswith(("_zscore", "_mom21")) or c == "credit_spread"]
    model_features = model_features[:25]  # Cap at 25

    # --- 3. Walk-forward validation ---
    train_window = 504  # ~2 years
    test_window = 63    # ~1 quarter
    predictions = []

    i = train_window
    while i + test_window <= len(df):
        train = df.iloc[:i]
        test = df.iloc[i:i + test_window]

        X_train = train[model_features].values
        y_train = train["target"].values
        X_test = test[model_features].values

        # Fit model
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        model = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            subsample=0.8,
            random_state=42,
        )
        model.fit(X_train_scaled, y_train)

        # Predict
        proba = model.predict_proba(X_test_scaled)[:, 1]

        for idx, (date, row) in enumerate(test.iterrows()):
            predictions.append({
                "obs_date": date.strftime("%Y-%m-%d"),
                "signal": "BUY" if proba[idx] >= 0.5 else "NO_BUY",
                "confidence": round(float(proba[idx]), 4),
                "predicted_return": round(float((proba[idx] - 0.5) * 0.1), 4),
            })

        # Expanding window: move forward by test_window
        i += test_window

    # --- 4. Output ---
    pred_df = pd.DataFrame(predictions)
    pred_df.to_csv("/app/predictions.csv", index=False)

    print(f"Generated {len(pred_df)} predictions")
    print(f"BUY signals: {(pred_df['signal'] == 'BUY').sum()}")
    print(f"Date range: {pred_df['obs_date'].min()} to {pred_df['obs_date'].max()}")
    print(f"Features used: {len(model_features)}")


if __name__ == "__main__":
    main()
