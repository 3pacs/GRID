"""
GRID entity mapping module.

Maps raw series identifiers (e.g. FRED codes, yfinance ticker fields) to
canonical feature names in the ``feature_registry``.  Provides fuzzy matching
to suggest mappings for unmapped series.
"""

from __future__ import annotations

import difflib
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Hardcoded seed mappings: raw series_id -> feature_registry.name
SEED_MAPPINGS: dict[str, str] = {
    "T10Y2Y": "yld_curve_2s10s",
    "T10Y3M": "yld_curve_3m10y",
    "DFF": "fed_funds_rate",
    "VIXCLS": "vix_spot",
    "USSLIND": "conf_board_lei",
    "CPIAUCSL": "cpi_index",       # Raw CPI index level (~327), NOT YoY change
    "YF:^GSPC:close": "sp500_full",   # TYPO-FIX: sp500_close not in registry, sp500_full exists
    "YF:^VIX:close": "vix_spot",      # TYPO-FIX: vix_spot_yf not in registry, vix_spot exists
    "YF:HG=F:close": "copper",        # TYPO-FIX: copper_futures_close not in registry, copper exists
    "YF:GC=F:close": "gold_full",     # TYPO-FIX: gold_futures_close not in registry, gold_full exists
    # CONFLICT-FIX: V2 maps these to *_full (which exists in registry with data).
    # SEED previously mapped to *_close (not in registry). V2 is correct.
    "YF:HYG:close": "hyg_full",     # was hyg_close (not in registry)
    "YF:LQD:close": "lqd_full",     # was lqd_close (not in registry)
    "YF:TLT:close": "tlt_full",     # was tlt_close (not in registry)
    "YF:UUP:close": "uup_etf_close",      # UUP ETF price (~$27), NOT DXY
    "YF:DX-Y.NYB:close": "dxy_index",     # Actual US Dollar Index (~104)
    # Sector-map proxy ETFs and tickers
    "YF:TSM:close": "tsm_full",       # TYPO-FIX: tsm_close not in registry, tsm_full exists
    "YF:SMH:close": "smh_close",
    "YF:KRE:close": "kre_full",       # TYPO-FIX: kre_close not in registry, kre_full exists
    "YF:ICLN:close": "icln_close",
    "YF:LIT:close": "lit_close",
    "YF:XBI:close": "xbi_full",       # TYPO-FIX: xbi_close not in registry, xbi_full exists
    "YF:ITA:close": "ita",            # TYPO-FIX: ita_close not in registry, ita exists

    # ── Unmapped FRED macro series ──────────────────────────────────────────
    "PAYEMS": "nonfarm_payrolls",
    "UNRATE": "unemployment_rate",
    "HOUST": "housing_starts",
    "DSPIC96": "real_disp_income",
    "M2SL": "m2_money_supply",
    "WALCL": "fed_balance_sheet",
    "BAMLH0A0HYM2": "hy_oas_spread",
    "BAMLC0A0CM": "ig_oas_spread",
    "TEDRATE": "ted_spread",
    "T5YIE": "breakeven_5y",
    "T10YIE": "yc_breakeven_10y",
    "DGS1": "yc_1y",
    "DGS2": "yc_2y",
    "DGS5": "yc_5y",
    "DGS10": "yc_10y",
    "DGS30": "yc_30y",
    "DFII10": "yc_real_10y",
    "UMCSENT": "umich_sentiment",
    "ICSA": "initial_claims",
    "RETAILSMNSA": "retail_sales_nsa",
    "INDPRO": "industrial_production",
    "RSAFS": "retail_sales_sa",
    "BOPGTB": "trade_balance",
    "WTREGEN": "treasury_general_acct",
    "PERMIT": "building_permits",
    "CCSA": "continued_claims",
    "PCEPI": "pce_deflator",
    "PCEPILFE": "core_pce",
    "TCU": "capacity_utilization",
    "NAPM": "ism_pmi_mfg",
    "MANEMP": "manufacturing_employment",

    # ── Unmapped FRED FX series ─────────────────────────────────────────────
    "DEXUSEU": "eurusd_fred",
    "DEXJPUS": "usdjpy_fred",
    "DEXCAUS": "usdcad_fred",
    "DEXSZUS": "usdchf_fred",
    "DEXUSUK": "gbpusd_fred",

    # ── Unmapped YFinance FX pairs ──────────────────────────────────────────
    "YF:EURUSD=X:close": "eurusd",
    "YF:GBPUSD=X:close": "gbpusd",
    "YF:USDJPY=X:close": "usdjpy",
    "YF:AUDUSD=X:close": "audusd",
    "YF:USDCHF=X:close": "usdchf",
    "YF:USDCAD=X:close": "usdcad",
    "YF:NZDUSD=X:close": "nzdusd",

    # ── Yield curve series (stored with feature name as series_id) ──────────
    "yc_1y": "yc_1y",
    "yc_5y": "yc_5y",
    "yc_30y": "yc_30y",
    "yc_real_10y": "yc_real_10y",
    "yc_breakeven_10y": "yc_breakeven_10y",
    "yc_term_premium": "yc_term_premium",
    "THREEFYTP10": "yc_term_premium",
    "yc_5s30s_spread": "yc_5s30s_spread",
    "yc_butterfly_2_5_10": "yc_butterfly_2_5_10",

    # ── Repo/money market series (stored with feature name as series_id) ────
    "sofr_rate": "sofr_rate",
    "reverse_repo_usage": "reverse_repo_usage",
    "sofr_spread_to_ffr": "sofr_spread_to_ffr",
    "rrp_as_pct_of_peak": "rrp_as_pct_of_peak",
    "treasury_bill_spread": "treasury_bill_spread",

    # ── Systemic/OFR series (stored with feature name as series_id) ─────────
    # NEEDS_REGISTRY: ofr_fsm_* features need to be added to feature_registry
    # "ofr_fsm_credit": "ofr_fsm_credit",       # DEAD: no matching feature in registry
    # "ofr_fsm_funding": "ofr_fsm_funding",      # DEAD: no matching feature in registry
    # "ofr_fsm_leverage": "ofr_fsm_leverage",    # DEAD: no matching feature in registry
    # "ofr_fsm_composite": "ofr_fsm_composite",  # DEAD: no matching feature in registry
    # ── Systemic derived features (computed from existing data) ────────────
    "systemic_stress_composite": "systemic_stress_composite",
    "systemic_credit_stress": "systemic_credit_stress",
    "systemic_funding_stress": "systemic_funding_stress",

    # ── Trade series (stored with feature name as series_id) ────────────────
    "eci_usa": "eci_usa",
    "eci_china": "eci_china",
    # "eci_global_dispersion": "eci_global_dispersion",  # DEAD: no matching feature in registry
    "trade_volume_yoy": "trade_volume_yoy",
    "us_china_trade_balance": "us_china_trade_balance",
    # "wiod_gvc_participation": "wiod_gvc_participation",  # DEAD: no matching feature in registry
    "korea_exports_total": "korea_exports_total",
    "korea_semi_exports": "korea_semi_exports",

    # ── Alternative series (stored with feature name as series_id) ──────────
    "viirs_us_lights": "viirs_us_lights",
    "viirs_china_lights": "viirs_china_lights",
    "viirs_em_lights": "viirs_em_lights",

    # ── Celestial/Lunar series (stored with feature name as series_id) ─────
    "lunar_phase": "lunar_phase",
    "lunar_illumination": "lunar_illumination",
    "days_to_new_moon": "days_to_new_moon",
    "days_to_full_moon": "days_to_full_moon",
    "lunar_eclipse_proximity": "lunar_eclipse_proximity",
    "solar_eclipse_proximity": "solar_eclipse_proximity",

    # ── Celestial/Planetary series ─────────────────────────────────────────
    "mercury_retrograde": "mercury_retrograde",
    "jupiter_saturn_angle": "jupiter_saturn_angle",
    "mars_volatility_index": "mars_volatility_index",
    "planetary_stress_index": "planetary_stress_index",
    "venus_cycle_phase": "venus_cycle_phase",

    # ── Celestial/Solar series ─────────────────────────────────────────────
    "sunspot_number": "sunspot_number",
    "solar_flux_10_7cm": "solar_flux_10_7cm",
    "geomagnetic_kp_index": "geomagnetic_kp_index",
    "geomagnetic_ap_index": "geomagnetic_ap_index",
    "solar_wind_speed": "solar_wind_speed",
    "solar_storm_probability": "solar_storm_probability",
    "solar_cycle_phase": "solar_cycle_phase",

    # ── Celestial/Vedic series ─────────────────────────────────────────────
    "nakshatra_index": "nakshatra_index",
    "nakshatra_quality": "nakshatra_quality",
    "tithi": "tithi",
    "rahu_ketu_axis": "rahu_ketu_axis",
    "dasha_cycle_phase": "dasha_cycle_phase",

    # ── Celestial/Chinese series ───────────────────────────────────────────
    "chinese_zodiac_year": "chinese_zodiac_year",
    "chinese_element": "chinese_element",
    "chinese_yin_yang": "chinese_yin_yang",
    "feng_shui_flying_star": "feng_shui_flying_star",
    "chinese_lunar_month": "chinese_lunar_month",
    "iching_hexagram_of_day": "iching_hexagram_of_day",

    # ── Patent velocity series (stored with feature name as series_id) ─────
    "patent_velocity_software": "patent_velocity_software",
    "patent_velocity_cleanenergy": "patent_velocity_cleanenergy",
    "patent_velocity_biotech": "patent_velocity_biotech",
    "patent_velocity_electrical": "patent_velocity_electrical",
    "patent_velocity_pharma": "patent_velocity_pharma",
    "patent_velocity_therapeutic": "patent_velocity_therapeutic",
    "patent_velocity_mechanical_energy": "patent_velocity_mechanical_energy",
    "patent_velocity_telecom": "patent_velocity_telecom",
    "patent_velocity_auto": "patent_velocity_auto",
    "innovation_composite": "innovation_composite",

    # ── OI (Opportunity Insights) series ───────────────────────────────────
    "oi_consumer_spend": "oi_consumer_spend",
    "oi_employment_overall": "oi_employment_overall",
    "oi_spend_low_income": "oi_spend_low_income",
    "oi_spend_high_income": "oi_spend_high_income",
    "oi_k_shape_ratio": "oi_k_shape_ratio",

    # ── AIS port arrivals (stored with feature name as series_id) ──────────
    "ais_port_arrivals_la": "ais_port_arrivals_la",
    "ais_port_arrivals_rotterdam": "ais_port_arrivals_rotterdam",
    "ais_port_arrivals_shanghai": "ais_port_arrivals_shanghai",
    "ais_port_arrivals_singapore": "ais_port_arrivals_singapore",
    "global_port_congestion": "global_port_congestion",

    # ── News sentiment series (stored with feature name as series_id) ──────
    "news_avg_confidence": "news_avg_confidence",
    "news_bearish_ratio": "news_bearish_ratio",
    "news_bullish_ratio": "news_bullish_ratio",
    "news_volume_daily": "news_volume_daily",

    # ── Volatility of volatility (stored with feature name as series_id) ──
    "vvix": "vvix",

    # ── GDELT series (stored with feature name as series_id) ───────────────
    "gdelt_recession_tone": "gdelt_recession_tone",
    "gdelt_fed_tone": "gdelt_fed_tone",
    "gdelt_trade_conflict_volume": "gdelt_trade_conflict_volume",
    "gdelt_tone_usa": "gdelt_tone_usa",
    "gdelt_event_count": "gdelt_event_count",

    # ── Supply chain series (stored with feature name as series_id) ────────
    "supply_chain.ism_deliveries": "supply_chain.ism_deliveries",
    "supply_chain.ism_backlog": "supply_chain.ism_backlog",
    "supply_chain.ism_inventories": "supply_chain.ism_inventories",
    "supply_chain.ism_prices": "supply_chain.ism_prices",
    "container_index": "container_index",

    # ── Baltic shipping series ─────────────────────────────────────────────
    "baltic.bdi": "baltic.bdi",
    "baltic.capesize": "baltic.capesize",
    "baltic.panamax": "baltic.panamax",
    "baltic.supramax": "baltic.supramax",

    # ── CFTC COT positioning series (pattern: cftc.{CONTRACT}.{metric}) ───
    # Major contracts: SP500, DJIA, NASDAQ, NOTE10Y, NOTE5Y, NOTE2Y,
    # EURODOLLAR, GOLD, SILVER, CRUDE_OIL, NATGAS, COPPER, CORN,
    # SOYBEANS, WHEAT, VIX
    # (These use dynamic series_id like cftc.SP500.net_speculative)

    # ── Comtrade series (puller writes labels directly as series_id) ───────
    "us_exports_total": "us_exports_total",
    "us_exports_total_yoy": "trade_volume_yoy",
    "us_china_bilateral": "us_china_trade_balance",
    "us_china_bilateral_yoy": "us_china_bilateral_yoy",
    "global_semi_trade": "global_semi_trade",
    "global_iron_trade": "global_iron_trade",
    "global_crude_trade": "global_crude_trade",
    "global_wheat_trade": "global_wheat_trade",

    # ── VIIRS divergence (stored with feature name as series_id) ───────────
    "china_viirs_macro_divergence": "china_viirs_macro_divergence",

    # ── USDA NASS (stored with feature name as series_id) ──────────────────
    "corn_yield_forecast": "corn_yield_forecast",
    "wheat_planted_acres": "wheat_planted_acres",
    "soybean_yield_forecast": "soybean_yield_forecast",
    "crop_progress_corn": "crop_progress_corn",
    "cattle_inventory": "cattle_inventory",

    # ── Eurostat series (stored with feature name as series_id) ───────────
    "eurozone_hicp_yoy": "eurozone_hicp_yoy",
    "eurozone_unemployment": "eurozone_unemployment",
    "eu_industrial_output": "eu_industrial_output",

    # ── Fed speeches / FOMC computed series ───────────────────────────────
    "fomc_hawkish_score": "fomc_hawkish_score",
    "fomc_days_since_meeting": "fomc_days_since_meeting",
    "fomc_days_to_meeting": "fomc_days_to_meeting",
    "fed_speech_frequency": "fed_speech_frequency",
    "fed_tone_7d_avg": "fed_tone_7d_avg",

    # ── Google Trends series (stored with feature name as series_id) ──────
    "gt_recession_interest": "gt_recession_interest",
    "gt_unemployment_interest": "gt_unemployment_interest",
    "gt_inflation_interest": "gt_inflation_interest",
    "gt_stock_market_crash": "gt_stock_market_crash",
    "gt_housing_bubble": "gt_housing_bubble",
    "gt_fed_rate_cut": "gt_fed_rate_cut",
    "gt_economic_composite": "gt_economic_composite",

    # ── AAII sentiment (prefix: aaii.) ────────────────────────────────────
    "aaii.bullish_pct": "aaii_bullish_pct",
    "aaii.bearish_pct": "aaii_bearish_pct",
    "aaii.neutral_pct": "aaii_neutral_pct",
    "aaii.bull_bear_spread": "aaii_bull_bear_spread",

    # ── CNN Fear & Greed (prefix: feargreed.) ─────────────────────────────
    "feargreed.cnn_value": "feargreed_cnn_value",
    "feargreed.cnn_previous_close": "feargreed_cnn_previous_close",
    "feargreed.crypto_value": "feargreed_crypto_value",

    # ── Philadelphia Fed ADS Index ────────────────────────────────────────
    "ads.business_conditions_index": "ads_business_conditions",

    # ── NY Fed series (prefix: nyfed.) ────────────────────────────────────
    "nyfed.nowcast_gdp_q1": "nyfed_nowcast_gdp_q1",
    "nyfed.nowcast_gdp_q2": "nyfed_nowcast_gdp_q2",
    "nyfed.soma_total_par_bn": "nyfed_soma_total",
    "nyfed.soma_treasury_par_bn": "nyfed_soma_treasury",
    "nyfed.soma_mbs_par_bn": "nyfed_soma_mbs",
    "nyfed.tsy_ops_total_bn": "nyfed_tsy_ops_total",

    # ── FINRA ATS (prefix: finra.) ────────────────────────────────────────
    "finra.ats_total_volume": "finra_ats_total_volume",
    "finra.ats_dark_pct": "finra_ats_dark_pct",
    "finra.short_interest_total": "finra_short_interest_total",

    # ── WorldNews series (wn_*) ──────────────────────────────────────────
    "wn_geopolitical_article_count": "wn_geopolitical_article_count",
    "wn_geopolitical_sentiment_avg": "wn_geopolitical_sentiment_avg",
    "wn_geopolitical_global_spread": "wn_geopolitical_global_spread",
    "wn_monetary_article_count": "wn_monetary_article_count",
    "wn_monetary_sentiment_avg": "wn_monetary_sentiment_avg",
    "wn_monetary_global_spread": "wn_monetary_global_spread",
    "wn_fiscal_article_count": "wn_fiscal_article_count",
    "wn_fiscal_sentiment_avg": "wn_fiscal_sentiment_avg",
    "wn_fiscal_global_spread": "wn_fiscal_global_spread",
    "wn_trade_article_count": "wn_trade_article_count",
    "wn_trade_sentiment_avg": "wn_trade_sentiment_avg",
    "wn_trade_global_spread": "wn_trade_global_spread",
    "wn_energy_article_count": "wn_energy_article_count",
    "wn_energy_sentiment_avg": "wn_energy_sentiment_avg",
    "wn_energy_global_spread": "wn_energy_global_spread",
    "wn_labor_article_count": "wn_labor_article_count",
    "wn_labor_sentiment_avg": "wn_labor_sentiment_avg",
    "wn_labor_global_spread": "wn_labor_global_spread",
    "wn_financial_article_count": "wn_financial_article_count",
    "wn_financial_sentiment_avg": "wn_financial_sentiment_avg",
    "wn_financial_global_spread": "wn_financial_global_spread",
    "wn_technology_article_count": "wn_technology_article_count",
    "wn_technology_sentiment_avg": "wn_technology_sentiment_avg",
    "wn_technology_global_spread": "wn_technology_global_spread",
    "wn_climate_article_count": "wn_climate_article_count",
    "wn_climate_sentiment_avg": "wn_climate_sentiment_avg",
    "wn_climate_global_spread": "wn_climate_global_spread",
    "wn_health_article_count": "wn_health_article_count",
    "wn_health_sentiment_avg": "wn_health_sentiment_avg",
    "wn_health_global_spread": "wn_health_global_spread",
    "wn_total_volume": "wn_total_volume",
    "wn_fear_index": "wn_fear_index",
    "wn_breadth": "wn_breadth",
}

# V2 mappings for international, trade, physical, and alternative data
NEW_MAPPINGS_V2: dict[str, str] = {
    # BLS (raw BLS series IDs from bls.py)
    "CES0000000001": "bls_nonfarm_payrolls",
    "LNS14000000": "bls_unemployment_rate",
    "CUUR0000SA0": "bls_cpi_u_all",
    "PRS85006092": "bls_nonfarm_productivity",
    "JTS00000000JOL": "bls_jolts_openings",

    # ECB
    "ECB:BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.A": "ecb_m3_yoy",
    "ECB:FM.M.DE.EUR.FR.BB.GVT.YLD.10Y": "euro_bund_10y",

    # OECD CLI
    "OECD:CLI:USA": "oecd_cli_us",
    "OECD:CLI:G-7": "oecd_cli_g7",
    "OECD:CLI:CHN": "oecd_cli_china",

    # BIS
    "BIS:total_credit:USA": "bis_credit_gdp_gap_us",
    "BIS:total_credit:CHN": "bis_credit_gdp_gap_cn",

    # AKShare China
    "AK:macro_china_money_supply:M2": "china_m2_yoy",
    "AK:macro_china_new_financial_credit:TSF": "china_tss_yoy",
    "AK:macro_china_industrial_production_yoy": "china_indpro_yoy",
    "AK:macro_china_pmi_yearly:NBS": "china_pmi_mfg",

    # BCB Brazil
    "BCB:11": "brazil_selic_rate",
    "BCB:13522": "brazil_ipca_yoy",
    "BCB:20539": "brazil_credit_growth",

    # KOSIS Korea
    "KOSIS:exports:total": "korea_exports_total",
    "KOSIS:exports:semi": "korea_semi_exports",

    # MAS Singapore
    "MAS:sora": "singapore_sora",

    # Opportunity Insights
    "OI:spend_all": "oi_consumer_spend",
    "OI:spend_all_q1": "oi_spend_low_income",
    "OI:spend_all_q4": "oi_spend_high_income",
    "OI:emp_combined": "oi_employment_overall",

    # OFR — NEEDS_REGISTRY: ofr_fsm_* features need to be added to feature_registry
    # "OFR:fsm_credit": "ofr_fsm_credit",       # DEAD: no matching feature in registry
    # "OFR:fsm_funding": "ofr_fsm_funding",      # DEAD: no matching feature in registry
    # "OFR:fsm_composite": "ofr_fsm_composite",  # DEAD: no matching feature in registry

    # USDA NASS
    "NASS:CORN:YIELD": "corn_yield_forecast",
    "NASS:WHEAT:PLANTED": "wheat_planted_acres",

    # ECI
    "ATLAS:ECI:USA": "eci_usa",
    "ATLAS:ECI:CHN": "eci_china",

    # Comtrade
    "COMTRADE:842:0:TOTAL": "trade_volume_yoy",
    "COMTRADE:842:156:TOTAL": "us_china_trade_balance",

    # VIIRS
    "VIIRS:us": "viirs_us_lights",
    "VIIRS:china": "viirs_china_lights",

    # Patents
    "USPTO:G06": "patent_velocity_software",
    "USPTO:Y02": "patent_velocity_cleanenergy",

    # Options signals (from ingestion/options.py)
    "OPT:SPY:pcr": "spy_pcr",
    "OPT:SPY:max_pain": "spy_max_pain",
    "OPT:SPY:iv_skew": "spy_iv_skew",
    "OPT:SPY:total_oi": "spy_total_oi",
    "OPT:SPY:opt_vol": "spy_opt_vol",
    "OPT:SPY:iv_atm": "spy_iv_atm",
    "OPT:SPY:iv_25d_put": "spy_iv_25d_put",
    "OPT:SPY:iv_25d_call": "spy_iv_25d_call",
    "OPT:SPY:term_slope": "spy_term_slope",
    "OPT:SPY:oi_conc": "spy_oi_conc",
    "OPT:QQQ:pcr": "qqq_pcr",
    "OPT:QQQ:iv_skew": "qqq_iv_skew",
    "OPT:QQQ:iv_atm": "qqq_iv_atm",
    "OPT:IWM:pcr": "iwm_pcr",
    "OPT:IWM:iv_skew": "iwm_iv_skew",
    "OPT:IWM:iv_atm": "iwm_iv_atm",

    # EIA energy data
    "EIA:eia_crude_price": "eia_crude_price",
    "EIA:eia_crude_refinery_input": "eia_crude_refinery_input",
    "EIA:eia_distillate_production": "eia_distillate_production",
    "EIA:eia_distillate_stocks": "eia_distillate_stocks",
    "EIA:eia_gasoline_production": "eia_gasoline_production",
    "EIA:eia_jet_fuel_stocks": "eia_jet_fuel_stocks",
    "EIA:eia_natgas_futures_1m": "eia_natgas_futures_1m",
    "EIA:eia_natgas_futures_4m": "eia_natgas_futures_4m",
    "EIA:eia_natgas_henry_hub": "eia_natgas_henry_hub",
    "EIA:eia_electricity_coal": "eia_electricity_coal",
    "EIA:eia_electricity_demand": "eia_electricity_demand",
    "EIA:eia_electricity_natgas": "eia_electricity_natgas",
    "EIA:eia_electricity_nuclear": "eia_electricity_nuclear",
    "EIA:eia_electricity_solar": "eia_electricity_solar",
    "EIA:eia_electricity_total": "eia_electricity_total",
    "EIA:eia_electricity_wind": "eia_electricity_wind",

    # Weather degree days (Open-Meteo)
    "METEO:weather_nyc_hdd": "weather_nyc_hdd",
    "METEO:weather_nyc_cdd": "weather_nyc_cdd",
    "METEO:weather_chicago_hdd": "weather_chicago_hdd",
    "METEO:weather_chicago_cdd": "weather_chicago_cdd",
    "METEO:weather_houston_hdd": "weather_houston_hdd",
    "METEO:weather_houston_cdd": "weather_houston_cdd",
    "METEO:weather_london_hdd": "weather_london_hdd",
    "METEO:weather_london_cdd": "weather_london_cdd",
    "METEO:weather_tokyo_hdd": "weather_tokyo_hdd",
    "METEO:weather_tokyo_cdd": "weather_tokyo_cdd",

    # OFR Financial Stress Monitor — NEEDS_REGISTRY
    # "OFR:ofr_fsm_composite": "ofr_fsm_composite",  # DEAD: no matching feature in registry
    # "OFR:ofr_fsm_credit": "ofr_fsm_credit",        # DEAD: no matching feature in registry
    # "OFR:ofr_fsm_funding": "ofr_fsm_funding",       # DEAD: no matching feature in registry

    # GDELT
    "GDELT:gdelt_avg_tone": "gdelt_avg_tone",

    # Stablecoins (CoinGecko)
    "CG:usdt_supply": "usdt_supply",
    "CG:usdc_supply": "usdc_supply",

    # Analyst ratings
    "ANALYST:ci_analyst_buy": "ci_analyst_buy",
    "ANALYST:ci_analyst_sell": "ci_analyst_sell",
    "ANALYST:ci_analyst_hold": "ci_analyst_hold",
    "ANALYST:cmcsa_analyst_buy": "cmcsa_analyst_buy",
    "ANALYST:cmcsa_analyst_sell": "cmcsa_analyst_sell",
    "ANALYST:cmcsa_analyst_hold": "cmcsa_analyst_hold",
    "ANALYST:dvn_analyst_buy": "dvn_analyst_buy",
    "ANALYST:dvn_analyst_sell": "dvn_analyst_sell",
    "ANALYST:dvn_analyst_hold": "dvn_analyst_hold",
    "ANALYST:eog_analyst_buy": "eog_analyst_buy",
    "ANALYST:eog_analyst_sell": "eog_analyst_sell",
    "ANALYST:eog_analyst_hold": "eog_analyst_hold",
    "ANALYST:gd_analyst_buy": "gd_analyst_buy",
    "ANALYST:gd_analyst_sell": "gd_analyst_sell",
    "ANALYST:gd_analyst_hold": "gd_analyst_hold",
    "ANALYST:intc_analyst_buy": "intc_analyst_buy",
    "ANALYST:intc_analyst_sell": "intc_analyst_sell",
    "ANALYST:intc_analyst_hold": "intc_analyst_hold",
    "ANALYST:pypl_analyst_buy": "pypl_analyst_buy",
    "ANALYST:pypl_analyst_sell": "pypl_analyst_sell",
    "ANALYST:pypl_analyst_hold": "pypl_analyst_hold",
    "ANALYST:rtx_analyst_buy": "rtx_analyst_buy",
    "ANALYST:rtx_analyst_sell": "rtx_analyst_sell",
    "ANALYST:rtx_analyst_hold": "rtx_analyst_hold",
    # NEEDS_REGISTRY: analyst ratings for ETFs, crypto, and thematic tickers
    # These mappings are correct but feature_registry entries don't exist yet.
    # "ANALYST:spy_analyst_buy": "spy_analyst_buy",       # DEAD: no matching feature in registry
    # "ANALYST:spy_analyst_sell": "spy_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:spy_analyst_hold": "spy_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:qqq_analyst_buy": "qqq_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:qqq_analyst_sell": "qqq_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:qqq_analyst_hold": "qqq_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:iwm_analyst_buy": "iwm_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:iwm_analyst_sell": "iwm_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:iwm_analyst_hold": "iwm_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:xle_analyst_buy": "xle_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:xle_analyst_sell": "xle_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:xle_analyst_hold": "xle_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:xlf_analyst_buy": "xlf_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:xlf_analyst_sell": "xlf_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:xlf_analyst_hold": "xlf_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:ita_analyst_buy": "ita_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:ita_analyst_sell": "ita_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:ita_analyst_hold": "ita_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:tlt_analyst_buy": "tlt_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:tlt_analyst_sell": "tlt_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:tlt_analyst_hold": "tlt_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:gld_analyst_buy": "gld_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:gld_analyst_sell": "gld_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:gld_analyst_hold": "gld_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:ura_analyst_buy": "ura_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:ura_analyst_sell": "ura_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:ura_analyst_hold": "ura_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:btc_analyst_buy": "btc_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:btc_analyst_sell": "btc_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:btc_analyst_hold": "btc_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:eth_analyst_buy": "eth_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:eth_analyst_sell": "eth_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:eth_analyst_hold": "eth_analyst_hold",      # DEAD: no matching feature in registry
    # "ANALYST:sol_analyst_buy": "sol_analyst_buy",        # DEAD: no matching feature in registry
    # "ANALYST:sol_analyst_sell": "sol_analyst_sell",      # DEAD: no matching feature in registry
    # "ANALYST:sol_analyst_hold": "sol_analyst_hold",      # DEAD: no matching feature in registry

    # ── Fed liquidity equation (fed_liquidity.py) ────────────────────────
    "RRPONTSYD": "overnight_reverse_repo",
    "WSHOSHO": "fed_treasury_holdings",
    "SWPT": "central_bank_liq_swaps",
    "H8B1023NCBCMG": "bank_reserves_at_fed",
    "TOTRESNS": "total_reserves",
    "COMPUTED:fed_net_liquidity": "fed_net_liquidity",
    "COMPUTED:fed_net_liquidity_change_1w": "fed_net_liquidity_change_1w",
    "COMPUTED:fed_net_liquidity_change_1m": "fed_net_liquidity_change_1m",
    "COMPUTED:reverse_repo_pct_of_peak": "reverse_repo_pct_of_peak",
    "COMPUTED:tga_drawdown": "tga_drawdown",

    # ── ETF flow proxies (institutional_flows.py) ──────────────────────
    "ETF_FLOW:SPY:5d": "etf_flow_spy_5d",
    "ETF_FLOW:SPY:20d": "etf_flow_spy_20d",
    "ETF_FLOW:SPY:accel": "etf_flow_spy_accel",
    "ETF_FLOW:QQQ:5d": "etf_flow_qqq_5d",
    "ETF_FLOW:QQQ:20d": "etf_flow_qqq_20d",
    "ETF_FLOW:QQQ:accel": "etf_flow_qqq_accel",
    "ETF_FLOW:IWM:5d": "etf_flow_iwm_5d",
    "ETF_FLOW:IWM:20d": "etf_flow_iwm_20d",
    "ETF_FLOW:IWM:accel": "etf_flow_iwm_accel",
    "ETF_FLOW:TLT:5d": "etf_flow_tlt_5d",
    "ETF_FLOW:TLT:20d": "etf_flow_tlt_20d",
    "ETF_FLOW:TLT:accel": "etf_flow_tlt_accel",
    "ETF_FLOW:HYG:5d": "etf_flow_hyg_5d",
    "ETF_FLOW:HYG:20d": "etf_flow_hyg_20d",
    "ETF_FLOW:HYG:accel": "etf_flow_hyg_accel",
    "ETF_FLOW:GLD:5d": "etf_flow_gld_5d",
    "ETF_FLOW:GLD:20d": "etf_flow_gld_20d",
    "ETF_FLOW:GLD:accel": "etf_flow_gld_accel",
    "ETF_FLOW:EEM:5d": "etf_flow_eem_5d",
    "ETF_FLOW:EEM:20d": "etf_flow_eem_20d",
    "ETF_FLOW:EEM:accel": "etf_flow_eem_accel",
    "ETF_FLOW:XLK:5d": "etf_flow_xlk_5d",

    # Ephemeris engine (AstroGrid canonical history)
    "ephemeris.mercury.longitude": "ephemeris_mercury_longitude",
    "ephemeris.venus.longitude": "ephemeris_venus_longitude",
    "ephemeris.mars.longitude": "ephemeris_mars_longitude",
    "ephemeris.jupiter.longitude": "ephemeris_jupiter_longitude",
    "ephemeris.saturn.longitude": "ephemeris_saturn_longitude",
    "ephemeris.mercury_retrograde": "ephemeris_mercury_retrograde",
    "ephemeris.venus_retrograde": "ephemeris_venus_retrograde",
    "ephemeris.mars_retrograde": "ephemeris_mars_retrograde",
    "ephemeris.jupiter_retrograde": "ephemeris_jupiter_retrograde",
    "ephemeris.saturn_retrograde": "ephemeris_saturn_retrograde",
    "ephemeris.aspect_count": "ephemeris_aspect_count",
    "ephemeris.hard_aspect_count": "ephemeris_hard_aspect_count",
    "ephemeris.soft_aspect_count": "ephemeris_soft_aspect_count",
    "ephemeris.lunar_phase": "ephemeris_lunar_phase",
    "ephemeris.lunar_illumination": "ephemeris_lunar_illumination",
    "ephemeris.lunar_age_days": "ephemeris_lunar_age_days",
    "ephemeris.tithi_index": "ephemeris_tithi_index",
    "ephemeris.phase_bucket": "ephemeris_phase_bucket",
    "ephemeris.nakshatra_index": "ephemeris_nakshatra_index",
    "ephemeris.nakshatra_pada": "ephemeris_nakshatra_pada",
    "ETF_FLOW:XLK:20d": "etf_flow_xlk_20d",
    "ETF_FLOW:XLK:accel": "etf_flow_xlk_accel",
    "ETF_FLOW:XLF:5d": "etf_flow_xlf_5d",
    "ETF_FLOW:XLF:20d": "etf_flow_xlf_20d",
    "ETF_FLOW:XLF:accel": "etf_flow_xlf_accel",
    "ETF_FLOW:XLE:5d": "etf_flow_xle_5d",
    "ETF_FLOW:XLE:20d": "etf_flow_xle_20d",
    "ETF_FLOW:XLE:accel": "etf_flow_xle_accel",
    "ETF_FLOW:XLV:5d": "etf_flow_xlv_5d",
    "ETF_FLOW:XLV:20d": "etf_flow_xlv_20d",
    "ETF_FLOW:XLV:accel": "etf_flow_xlv_accel",

    # Computed/derived features
    "COMPUTED:copper_gold_ratio": "copper_gold_ratio",
    "COMPUTED:copper_gold_slope": "copper_gold_slope",
    "COMPUTED:vix_1m_chg": "vix_1m_chg",
    "COMPUTED:vix_3m_ratio": "vix_3m_ratio",
    "COMPUTED:sp500_mom_3m": "sp500_mom_3m",
    "COMPUTED:sp500_mom_12_1": "sp500_mom_12_1",
    "COMPUTED:dxy_3m_chg": "dxy_3m_chg",
    "COMPUTED:hy_spread_3m_chg": "hy_spread_3m_chg",
    "COMPUTED:fed_funds_3m_chg": "fed_funds_3m_chg",
    "COMPUTED:spy_macd": "spy_macd",
    "COMPUTED:conf_board_lei_slope": "conf_board_lei_slope",

    # Crypto volumes (yfinance)
    "YF:ETH-USD:volume": "eth_total_volume",
    "YF:SOL-USD:volume": "sol_total_volume",
    "YF:BTC-USD:volume": "btc_total_volume",
    "YF:TAO-USD:close": "tao_chain_market_cap",
    "YF:TAO-USD:volume": "tao_chain_total_volume",
    "YF:BRK-B:close": "brk-b_full",

    # CBOE indices (bulk CSV)
    "CBOE:VIX": "vix_spot",
    "CBOE:VIX3M": "vix3m_spot",
    "CBOE:VIX9D": "vix9d_spot",
    "CBOE:SKEW": "skew_index",

    # Binance crypto (bulk ZIP)
    "BINANCE:BTCUSDT:close": "btc_full",       # TYPO-FIX: btc_close not in registry, btc_full exists
    "BINANCE:BTCUSDT:volume": "btc_total_volume",
    "BINANCE:ETHUSDT:close": "eth_full",       # TYPO-FIX: eth_close not in registry, eth_full exists
    "BINANCE:ETHUSDT:volume": "eth_total_volume",
    "BINANCE:SOLUSDT:close": "sol_full",       # TYPO-FIX: sol_close not in registry, sol_full exists
    "BINANCE:SOLUSDT:volume": "sol_total_volume",
    "BINANCE:TAOUSDT:close": "tao_chain_market_cap",
    "BINANCE:TAOUSDT:volume": "tao_chain_total_volume",

    # CoinGecko bulk
    "CG:bitcoin:close": "btc_full",            # TYPO-FIX: btc_close not in registry, btc_full exists
    "CG:bitcoin:volume": "btc_total_volume",
    "CG:ethereum:close": "eth_full",           # TYPO-FIX: eth_close not in registry, eth_full exists
    "CG:ethereum:volume": "eth_total_volume",
    "CG:solana:close": "sol_full",             # TYPO-FIX: sol_close not in registry, sol_full exists
    "CG:solana:volume": "sol_total_volume",
    "CG:bittensor:close": "tao_chain_market_cap",
    "CG:bittensor:volume": "tao_chain_total_volume",

    # DeFi (DeFiLlama + DexScreener)
    "DEFILLAMA:solana_dex_volume": "dex_sol_volume_24h",
    "DEFILLAMA:solana_tvl": "dex_sol_liquidity",
    "DEXSCR:sol_txn_count": "dex_sol_txn_count_24h",
    "DEXSCR:sol_buy_sell_ratio": "dex_sol_buy_sell_ratio",
    "DEXSCR:sol_momentum_24h": "dex_sol_momentum_24h",
    "DEXSCR:sol_boosted_tokens": "dex_sol_boosted_tokens",

    # Polymarket
    "POLYMARKET:btc": "polymarket_btc",

    # ── Self-mappings for international pullers (write feature name directly) ─
    # ECB (puller writes feature name as series_id)
    "ecb_m3_yoy": "ecb_m3_yoy",
    "ecb_bank_lending_yoy": "ecb_bank_lending_yoy",
    "euro_bund_10y": "euro_bund_10y",
    "euro_btp_bund_spread": "euro_btp_bund_spread",
    "eurusd_ecb_daily": "eurusd_ecb_daily",

    # OECD CLI (puller writes feature name as series_id)
    "oecd_cli_us": "oecd_cli_us",
    "oecd_cli_g7": "oecd_cli_g7",
    "oecd_cli_china": "oecd_cli_china",

    # BIS (puller writes feature name as series_id)
    "bis_credit_gdp_gap_us": "bis_credit_gdp_gap_us",
    "bis_credit_gdp_gap_cn": "bis_credit_gdp_gap_cn",
    "bis_credit_gdp_gap_de": "bis_credit_gdp_gap_de",
    "bis_global_cbflow_usd": "bis_global_cbflow_usd",
    "bis_property_prices_g20": "bis_property_prices_g20",

    # AKShare China (puller writes feature name as series_id)
    "china_m2_yoy": "china_m2_yoy",
    "china_tss_yoy": "china_tss_yoy",
    "china_indpro_yoy": "china_indpro_yoy",
    "china_pmi_mfg": "china_pmi_mfg",
    "china_credit_impulse": "china_credit_impulse",

    # BCB Brazil (puller writes feature name as series_id)
    "brazil_selic_rate": "brazil_selic_rate",
    "brazil_ipca_yoy": "brazil_ipca_yoy",
    "brazil_credit_growth": "brazil_credit_growth",

    # RBI India (puller writes feature name as series_id)
    "india_repo_rate": "india_repo_rate",
    "india_iip_yoy": "india_iip_yoy",
    "india_fx_reserves": "india_fx_reserves",

    # ABS Australia (puller writes feature name as series_id)
    "australia_rba_rate": "australia_rba_rate",
    "australia_cpi_qoq": "australia_cpi_qoq",
    "australia_unemployment": "australia_unemployment",
    "australia_iron_exports": "australia_iron_exports",
    "australia_gdp_qoq": "australia_gdp_qoq",

    # KOSIS Korea (puller writes feature name as series_id)
    # NOTE: korea_exports_total -> korea_exports_yoy is in SEED_MAPPINGS
    "korea_auto_exports": "korea_auto_exports",
    "korea_iip_yoy": "korea_iip_yoy",
    "korea_cpi_yoy": "korea_cpi_yoy",

    # MAS Singapore (puller writes feature name as series_id)
    "singapore_sora": "singapore_sora",

    # ECI Atlas (puller writes feature name as series_id)
    "eci_usa": "eci_usa",
    "eci_china": "eci_china",
    # "eci_global_dispersion": "eci_global_dispersion",  # DEAD: no matching feature in registry

    # WIOD (puller writes feature name as series_id)
    # "wiod_gvc_participation": "wiod_gvc_participation",  # DEAD: no matching feature in registry

    # OFR (puller writes feature name as series_id — duplicate for resolution)
    # NEEDS_REGISTRY: ofr_fsm_* features need to be added to feature_registry
    # "ofr_fsm_credit": "ofr_fsm_credit",       # DEAD: no matching feature in registry
    # "ofr_fsm_funding": "ofr_fsm_funding",      # DEAD: no matching feature in registry
    # "ofr_fsm_leverage": "ofr_fsm_leverage",    # DEAD: no matching feature in registry
    # "ofr_fsm_composite": "ofr_fsm_composite",  # DEAD: no matching feature in registry
    "ofr_fsi": "ofr_financial_stress",
    "ofr_repo_volume": "ofr_repo_volume",
    "ofr_repo_rate_1d": "ofr_repo_rate_1d",

    # Yield curve puller (stores as treasury_Xy, sofr, reverse_repo)
    "sofr": "sofr_rate",
    "reverse_repo": "reverse_repo_usage",
    "treasury_10y": "yc_10y",
    "treasury_2y": "yc_2y",
    "treasury_30y": "yc_30y",

    # AstroGrid scorecard universe — canonical close prices
    "YF:QQQ:close": "qqq_full",
    "YF:CL=F:close": "cl_close",

    # ── CBOE vol self-mappings (puller writes feature name as series_id) ──
    "put_call_ratio": "put_call_ratio",
    "correlation_index": "correlation_index",

    # ── Pump.fun mappings (puller writes PUMP:{signal} as series_id) ──────
    "PUMP:pump_new_tokens_count": "pump_fun_daily_deploys",
    "PUMP:pump_graduated_count": "pump_fun_daily_grads",
    "PUMP:pump_grad_rate": "pump_fun_grad_rate",
    "PUMP:pump_koth_mcap": "pump_fun_koth_mcap",
    "PUMP:pump_latest_avg_mcap": "pump_fun_latest_avg_mcap",
    "PUMP:pump_live_token_count": "pump_fun_live_token_count",
    "PUMP:pump_koth_reply_count": "pump_fun_koth_reply_count",
    "PUMP:pump_graduated_avg_mcap": "pump_fun_graduated_avg_mcap",

    # ── GDELT avg_tone self-mapping (parse_gdelt.py writes directly) ──────
    "gdelt_avg_tone": "gdelt_avg_tone",

    # ── FRED proxy mappings (fill_missing_features.py writes FRED:{name}) ─
    "FRED:bis_credit_gap_us": "bis_credit_gap_us",
    "FRED:repo_volume": "repo_volume",
    "FRED:hy_spread_proxy": "hy_spread_proxy",
    "FRED:real_ffr": "real_ffr",
    "FRED:dxy_index": "dxy_index",
    "FRED:dxy_3m_chg": "dxy_3m_chg",
    "FRED:vix_1m_chg": "vix_1m_chg",
    "FRED:vix_3m_ratio": "vix_3m_ratio",
    "FRED:conf_board_lei_slope": "conf_board_lei_slope",
    "FRED:hy_spread_3m_chg": "hy_spread_3m_chg",
    "FRED:fed_funds_3m_chg": "fed_funds_3m_chg",

    # ── Self-mappings for features where puller writes feature name ────────
    "bis_credit_gap_us": "bis_credit_gap_us",
    "repo_volume": "repo_volume",

    # ── WEB scraper mappings (web_scraper.py writes WEB:{name}) ───────────
    "WEB:bis_credit_gap_us": "bis_credit_gap_us",
    "WEB:repo_volume": "repo_volume",
    "WEB:ism_pmi_new_orders": "ism_pmi_new_orders",

    # ── Tiingo/yfinance ticker → _full features ────────────────────
    # PREFER adj_close (split-adjusted) over raw close.
    # Both map to the same _full feature; resolver priority picks the winner.
    # adj_close entries come first so they take precedence.
    #
    # Mega caps — adj_close (split-adjusted, no manual split detection needed)
    **{f"YF:{t}:adj_close": f"{t.lower()}_full" for t in [
        "SPY", "QQQ", "IWM", "DIA",
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
        "JPM", "V", "MA", "UNH", "JNJ", "PG", "HD", "BAC",
        "AVGO", "COST", "LLY", "MRK", "PFE", "ABBV", "TMO", "PEP", "KO",
        "CRM", "AMD", "INTC", "GS", "BLK", "LMT", "RTX", "GD",
        "CVX", "XOM", "EOG", "DVN", "PYPL", "CMCSA", "SIRI",
        "NFLX", "DIS", "CSCO", "ORCL", "ADBE", "ACN", "IBM", "TXN", "QCOM",
        "WMT", "LOW", "TGT", "SBUX", "MCD", "NKE",
        "CAT", "DE", "MMM", "HON", "GE", "BA",
        "AXP", "C", "WFC", "MS", "SCHW",
        "T", "VZ", "TMUS",
        "NEE", "DUK", "SO", "D",
        "AMT", "PLD", "CCI", "SPG",
        "COIN", "MSTR",
    ]},
    # Mega caps — raw close as FALLBACK (when adj_close is missing)
    **{f"YF:{t}:close": f"{t.lower()}_full" for t in [
        "SPY", "QQQ", "IWM", "DIA",
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
        "JPM", "V", "MA", "UNH", "JNJ", "PG", "HD", "BAC",
        "AVGO", "COST", "LLY", "MRK", "PFE", "ABBV", "TMO", "PEP", "KO",
        "CRM", "AMD", "INTC", "GS", "BLK", "LMT", "RTX", "GD",
        "CVX", "XOM", "EOG", "DVN", "PYPL", "CMCSA", "SIRI",
        "NFLX", "DIS", "CSCO", "ORCL", "ADBE", "ACN", "IBM", "TXN", "QCOM",
        "WMT", "LOW", "TGT", "SBUX", "MCD", "NKE",
        "CAT", "DE", "MMM", "HON", "GE", "BA",
        "AXP", "C", "WFC", "MS", "SCHW",
        "T", "VZ", "TMUS",
        "NEE", "DUK", "SO", "D",
        "AMT", "PLD", "CCI", "SPG",
        "COIN", "MSTR",
    ]},
    # Sector ETFs — adj_close preferred
    **{f"YF:{t}:adj_close": f"{t.lower()}_full" for t in [
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC",
    ]},
    **{f"YF:{t}:close": f"{t.lower()}_full" for t in [
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC",
    ]},
    # Bond/commodity ETFs — adj_close preferred
    **{f"YF:{t}:adj_close": f"{t.lower()}_full" for t in [
        "GLD", "SLV", "USO", "UNG",
        "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "EMB", "MUB",
    ]},
    **{f"YF:{t}:close": f"{t.lower()}_full" for t in [
        "GLD", "SLV", "USO", "UNG",
        "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "EMB", "MUB",
    ]},
}


class EntityMap:
    """Maps raw series identifiers to feature registry entries.

    Uses the hardcoded SEED_MAPPINGS as a base and resolves
    feature_registry IDs from the database.

    Attributes:
        engine: SQLAlchemy engine for database lookups.
        _feature_cache: Cached mapping of feature name -> feature_registry.id.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the entity mapper.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        self._feature_cache: dict[str, int] = {}
        self._feature_freshness: dict[str, Any] = {}
        self._load_feature_cache()
        self.load_v2_mappings()
        self._detect_duplicate_mappings()
        log.info(
            "EntityMap initialised — {n} features cached, {m} seed mappings",
            n=len(self._feature_cache),
            m=len(SEED_MAPPINGS),
        )

    def _load_feature_cache(self) -> None:
        """Load all feature_registry entries into an in-memory cache.

        Populates ``_feature_cache`` with {name: id} pairs.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry")
            ).fetchall()
        self._feature_cache = {row[1]: row[0] for row in rows}
        log.debug("Feature cache loaded: {n} entries", n=len(self._feature_cache))

    def get_feature_id(self, series_id: str) -> int | None:
        """Resolve a raw series_id to a feature_registry.id.

        Parameters:
            series_id: Raw series identifier (e.g. 'T10Y2Y', 'YF:^GSPC:close').

        Returns:
            int: The feature_registry.id if a mapping exists, None otherwise.
        """
        feature_name = SEED_MAPPINGS.get(series_id)
        if feature_name is None:
            log.debug("No mapping found for series_id={sid}", sid=series_id)
            return None

        feature_id = self._feature_cache.get(feature_name)
        if feature_id is None:
            # Refresh cache in case new features were added
            self._load_feature_cache()
            feature_id = self._feature_cache.get(feature_name)

        if feature_id is None:
            log.warning(
                "Mapping exists ({sid} -> {fn}) but feature not in registry",
                sid=series_id,
                fn=feature_name,
            )
            return feature_id

        # Warn if the resolved feature has no recent data (cached check)
        self._check_feature_freshness(feature_name, feature_id)

        return feature_id

    def _detect_duplicate_mappings(self) -> None:
        """Log warnings for raw_ids that appear in both SEED and V2 with
        different target feature names.  Runs once at startup."""
        dupes_found = 0
        for raw_id, v2_target in NEW_MAPPINGS_V2.items():
            seed_target = SEED_MAPPINGS.get(raw_id)
            # After load_v2_mappings, SEED_MAPPINGS may already contain v2
            # entries.  Compare against the original seed value if it existed
            # before merge (seed_target != v2_target means conflict).
            if seed_target is not None and seed_target != v2_target:
                log.warning(
                    "SANITY duplicate mapping: raw_id={rid} -> "
                    "SEED={st} vs V2={vt}",
                    rid=raw_id, st=seed_target, vt=v2_target,
                )
                dupes_found += 1
        if dupes_found:
            log.warning(
                "SANITY: {n} duplicate SEED/V2 mappings detected",
                n=dupes_found,
            )

    def _check_feature_freshness(
        self, feature_name: str, feature_id: int
    ) -> None:
        """Log a warning if a feature has no data in the last 14 days.

        Uses a lightweight in-memory cache to avoid repeated DB queries.
        Cache is populated lazily and refreshed every 1000 lookups.
        """
        # Lazy-load freshness cache
        if not self._feature_freshness:
            self._load_freshness_cache()

        status = self._feature_freshness.get(feature_name)
        if status == "stale":
            log.debug(
                "SANITY freshness: feature {fn} (id={fid}) has no recent data",
                fn=feature_name, fid=feature_id,
            )

    def _load_freshness_cache(self) -> None:
        """Bulk-load freshness status for all features (stale if >14 days)."""
        from datetime import date as _date, timedelta as _td
        cutoff = _date.today() - _td(days=14)
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT fr.name, MAX(rs.obs_date) as latest "
                    "FROM feature_registry fr "
                    "LEFT JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "GROUP BY fr.name"
                )).fetchall()
                for row in rows:
                    name = row[0]
                    latest = row[1]
                    if latest is None or latest < cutoff:
                        self._feature_freshness[name] = "stale"
                    else:
                        self._feature_freshness[name] = "fresh"
        except Exception as exc:
            log.debug(
                "EntityMap freshness cache load failed: {e}", e=str(exc)
            )

    def get_all_mappings(self) -> dict[str, str]:
        """Return the full mapping dictionary.

        Returns:
            dict: Mapping of raw series_id -> feature name.
        """
        return dict(SEED_MAPPINGS)

    def load_v2_mappings(self) -> None:
        """Merge NEW_MAPPINGS_V2 into SEED_MAPPINGS and refresh feature cache.

        Safe to call multiple times — existing mappings are not overwritten.
        """
        merged_count = 0
        for raw_id, feature_name in NEW_MAPPINGS_V2.items():
            if raw_id not in SEED_MAPPINGS:
                SEED_MAPPINGS[raw_id] = feature_name
                merged_count += 1
        self._load_feature_cache()
        log.info(
            "V2 mappings loaded — {n} new mappings merged, {total} total",
            n=merged_count,
            total=len(SEED_MAPPINGS),
        )

    def suggest_mapping(self, series_id: str) -> list[str]:
        """Suggest possible feature names for an unmapped series_id.

        Uses ``difflib.SequenceMatcher`` to find the top 3 closest matches
        among registered feature names.

        Parameters:
            series_id: Raw series identifier to find matches for.

        Returns:
            list[str]: Up to 3 feature name suggestions, ordered by similarity.
        """
        all_names = list(self._feature_cache.keys())
        if not all_names:
            return []

        # Normalise the query for better matching
        query = series_id.lower().replace(":", "_").replace("^", "").replace("=", "")

        scored: list[tuple[float, str]] = []
        for name in all_names:
            ratio = difflib.SequenceMatcher(None, query, name.lower()).ratio()
            scored.append((ratio, name))

        scored.sort(key=lambda x: x[0], reverse=True)
        suggestions = [name for _, name in scored[:3]]

        log.debug(
            "Suggestions for '{sid}': {s}",
            sid=series_id,
            s=suggestions,
        )
        return suggestions


if __name__ == "__main__":
    from db import get_engine

    em = EntityMap(db_engine=get_engine())
    print("All mappings:")
    for sid, fname in em.get_all_mappings().items():
        fid = em.get_feature_id(sid)
        print(f"  {sid:25s} -> {fname:25s} (id={fid})")

    print("\nSuggestions for 'SP500':")
    for s in em.suggest_mapping("SP500"):
        print(f"  {s}")
