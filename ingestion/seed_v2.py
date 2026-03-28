"""
GRID v2 database seed data.

Populates source_catalog and feature_registry with entries for all
international, trade, physical, and alternative data sources added in v2.
Safe to run multiple times — uses INSERT ... ON CONFLICT DO NOTHING.
"""

from __future__ import annotations

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ─── Source catalog INSERT statements ─────────────────────────────────────────

SOURCE_CATALOG_SQL = """
INSERT INTO source_catalog
  (name, base_url, license_type, update_frequency,
   has_vintage_data, revision_policy, data_quality, priority, model_eligible)
VALUES
  ('ECB_SDW',     'https://sdw-wsrest.ecb.europa.eu/service', 'FREE', 'DAILY',   TRUE,  'RARE',   'HIGH', 10, TRUE),
  ('OECD_SDMX',   'https://stats.oecd.org/SDMX-JSON',         'FREE', 'MONTHLY', TRUE,  'RARE',   'HIGH', 11, TRUE),
  ('BIS',         'https://stats.bis.org/api/v1',              'FREE', 'QUARTERLY',TRUE, 'RARE',   'HIGH', 12, TRUE),
  ('Eurostat',    'https://ec.europa.eu/eurostat',             'FREE', 'MONTHLY', TRUE,  'RARE',   'HIGH', 13, TRUE),
  ('IMF_IFS',     'https://www.imf.org/external/datamapper',  'FREE', 'MONTHLY', FALSE, 'RARE',   'HIGH', 14, TRUE),
  ('IMF_WEO',     'https://www.imf.org/en/Publications/WEO',  'FREE', 'BIANNUAL',FALSE, 'NEVER',  'HIGH', 15, TRUE),
  ('RBI',         'https://rbi.org.in/Scripts/DataReleases',  'FREE', 'MONTHLY', FALSE, 'RARE',   'HIGH', 16, TRUE),
  ('ABS_AU',      'https://api.data.abs.gov.au',              'FREE', 'MONTHLY', FALSE, 'RARE',   'HIGH', 17, TRUE),
  ('KOSIS',       'https://kosis.kr/openapi',                 'FREE', 'MONTHLY', FALSE, 'RARE',   'HIGH', 18, TRUE),
  ('BCB_BR',      'https://api.bcb.gov.br/dados/serie',       'FREE', 'DAILY',   FALSE, 'NEVER',  'HIGH', 19, TRUE),
  ('MAS_SG',      'https://eservices.mas.gov.sg/api',         'FREE', 'MONTHLY', FALSE, 'RARE',   'MED',  20, TRUE),
  ('AKShare',     'https://akshare.akfamily.xyz',             'FREE', 'DAILY',   FALSE, 'NEVER',  'MED',  21, TRUE),
  ('EDINET',      'https://api.edinet-fsa.go.jp/api/v2',      'FREE', 'DAILY',   FALSE, 'NEVER',  'HIGH', 22, FALSE),
  ('JQuants',     'https://api.jquants.com/v1',               'FREE', 'DAILY',   FALSE, 'NEVER',  'HIGH', 23, TRUE),
  ('Comtrade',    'https://comtradeapi.un.org/public/v1',     'FREE', 'MONTHLY', FALSE, 'RARE',   'HIGH', 24, TRUE),
  ('CEPII_BACI',  'https://www.cepii.fr/CEPII/en/bdd_modele','FREE', 'ANNUAL',  FALSE, 'RARE',   'HIGH', 25, TRUE),
  ('Atlas_ECI',   'https://atlas.cid.harvard.edu',            'FREE', 'ANNUAL',  FALSE, 'NEVER',  'HIGH', 26, TRUE),
  ('WIOD',        'https://www.wiod.org/database/wiots16',    'FREE', 'ANNUAL',  FALSE, 'NEVER',  'HIGH', 27, TRUE),
  ('VIIRS',       'https://eogdata.mines.edu/nighttime_light','FREE', 'MONTHLY', FALSE, 'NEVER',  'HIGH', 28, TRUE),
  ('EU_KLEMS',    'https://euklems.eu',                       'FREE', 'ANNUAL',  FALSE, 'NEVER',  'HIGH', 29, TRUE),
  ('DBnomics',    'https://db.nomics.world/api/v22',          'FREE', 'VARIES',  FALSE, 'RARE',   'MED',  30, TRUE),
  ('USPTO_PV',    'https://patentsview.org/download',         'FREE', 'ANNUAL',  FALSE, 'NEVER',  'HIGH', 31, TRUE),
  ('USDA_NASS',   'https://quickstats.nass.usda.gov/api',     'FREE', 'WEEKLY',  FALSE, 'RARE',   'HIGH', 32, TRUE),
  ('OFR',         'https://financialresearch.gov/data',       'FREE', 'WEEKLY',  FALSE, 'NEVER',  'HIGH', 33, TRUE),
  ('OppInsights', 'https://github.com/OpportunityInsights',  'FREE', 'WEEKLY',  FALSE, 'RARE',   'HIGH', 34, TRUE),
  ('NOAA_AIS',    'https://marinecadastre.gov/ais',           'FREE', 'ANNUAL',  FALSE, 'NEVER',  'HIGH', 35, TRUE),
  ('GDELT',       'https://api.gdeltproject.org/api/v2',      'FREE', 'DAILY',   FALSE, 'NEVER',  'MED',  36, FALSE)
ON CONFLICT (name) DO NOTHING;
"""

# ─── Feature registry INSERT statements ───────────────────────────────────────

FEATURE_REGISTRY_SQL = """
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
VALUES
  -- ECB / Euro Area
  ('ecb_m3_yoy',            'macro',  'Euro area M3 money supply YoY growth',
   'ECB SDW series BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.A',
   10, 0, 'ZSCORE', 'FORWARD_FILL', '1999-01-01', TRUE),
  ('ecb_bank_lending_yoy',  'macro',  'Euro area bank lending to private sector YoY',
   'ECB SDW series BSI.M.U2.Y.U.A20.A.1.U2.2250.Z01.A',
   10, 0, 'ZSCORE', 'FORWARD_FILL', '1999-01-01', TRUE),
  ('ecb_target2_flows',     'macro',  'ECB TARGET2 net position proxy (DE vs IT/ES)',
   'Derived from ECB SDW cross-border balance data',
   10, 0, 'ZSCORE', 'FORWARD_FILL', '2008-01-01', TRUE),
  ('euro_bund_10y',         'rates',       'German 10Y Bund yield',
   'ECB SDW series FM.M.DE.EUR.FR.BB.GVT.YLD.10Y',
   10, 0, 'RAW', 'FORWARD_FILL', '1999-01-01', TRUE),
  ('euro_btp_bund_spread',  'credit',      'Italy BTP vs German Bund 10Y spread (stress proxy)',
   'BTP 10Y minus Bund 10Y from ECB SDW',
   10, 0, 'ZSCORE', 'FORWARD_FILL', '1999-01-01', TRUE),

  -- OECD CLI
  ('oecd_cli_g7',           'macro',       'OECD CLI for G7 aggregate',
   'OECD MEI series MEI_CLI.LOLITONOSM.G-7.M',
   11, 0, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),
  ('oecd_cli_us',           'macro',       'OECD CLI for United States',
   'OECD MEI series MEI_CLI.LOLITONOSM.USA.M',
   11, 0, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),
  ('oecd_cli_china',        'macro',       'OECD CLI for China',
   'OECD MEI series MEI_CLI.LOLITONOSM.CHN.M',
   11, 0, 'ZSCORE', 'FORWARD_FILL', '1996-01-01', TRUE),
  ('oecd_cli_slope_g7',     'macro',       '3-month slope of OECD G7 CLI',
   'rolling_slope(oecd_cli_g7, 63)',
   11, 63, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),

  -- BIS
  ('bis_credit_gdp_gap_us', 'credit',      'BIS credit-to-GDP gap for United States',
   'BIS early warning indicator series',
   12, 0, 'RAW', 'FORWARD_FILL', '1970-01-01', TRUE),
  ('bis_credit_gdp_gap_cn', 'credit',      'BIS credit-to-GDP gap for China',
   'BIS CBS Q-series for China',
   12, 0, 'RAW', 'FORWARD_FILL', '1985-01-01', TRUE),
  ('bis_cbfunds_rate_shadow','rates',       'BIS shadow rate for Fed (Wu-Xia)',
   'Estimated effective rate at zero lower bound from BIS',
   12, 0, 'RAW', 'FORWARD_FILL', '1990-01-01', TRUE),
  ('bis_global_cbflow',     'flows',       'BIS cross-border banking claims aggregate',
   'BIS LBS total cross-border claims USD',
   12, 0, 'ZSCORE', 'FORWARD_FILL', '1977-01-01', TRUE),

  -- China macro
  ('china_m2_yoy',          'macro', 'China M2 money supply YoY growth rate',
   'akshare macro_china_money_supply -> M2 YoY',
   21, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),
  ('china_tss_yoy',         'macro', 'China Total Social Financing YoY',
   'akshare macro_china_new_financial_credit -> TSF YoY',
   21, 0, 'ZSCORE', 'FORWARD_FILL', '2002-01-01', TRUE),
  ('china_indpro_yoy',      'macro', 'China industrial production YoY',
   'akshare macro_china_industrial_production_yoy',
   21, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
  ('china_pmi_mfg',         'macro', 'China NBS Manufacturing PMI',
   'akshare macro_china_pmi_yearly -> NBS Mfg PMI',
   21, 0, 'RAW', 'FORWARD_FILL', '2005-01-01', TRUE),
  ('china_pmi_caixin',      'macro', 'China Caixin Manufacturing PMI',
   'akshare macro_china_pmi_yearly -> Caixin PMI',
   21, 0, 'RAW', 'FORWARD_FILL', '2010-01-01', TRUE),
  ('china_credit_impulse',  'macro', 'China credit impulse (12m change in TSF/GDP)',
   'Derived from TSS and GDP series',
   21, 252, 'ZSCORE', 'FORWARD_FILL', '2002-01-01', TRUE),

  -- India
  ('india_repo_rate',       'rates',       'RBI repo rate',
   'RBI DRR series', 16, 0, 'RAW', 'FORWARD_FILL', '2000-01-01', TRUE),
  ('india_iip_yoy',         'macro',    'India Industrial Production Index YoY',
   'RBI / MOSPI IIP series', 16, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),
  ('india_fx_reserves',     'macro',    'India FX reserves USD billions',
   'RBI weekly FX reserves', 16, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),

  -- Brazil
  ('brazil_selic_rate',     'rates',       'Brazil SELIC overnight rate',
   'BCB SGS series 11', 19, 0, 'RAW', 'FORWARD_FILL', '1994-01-01', TRUE),
  ('brazil_ipca_yoy',       'macro',    'Brazil IPCA inflation YoY',
   'BCB SGS series 13522', 19, 0, 'ZSCORE', 'FORWARD_FILL', '1980-01-01', TRUE),
  ('brazil_credit_growth',  'macro',    'Brazil total credit operations YoY',
   'BCB SGS series 20539', 19, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),

  -- Korea
  ('korea_exports_yoy',     'trade',       'South Korea monthly exports YoY',
   'KOSIS TDN006 export series', 18, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
  ('korea_semi_exports',    'trade',       'South Korea semiconductor exports USD',
   'KOSIS TDN006 HS85 semiconductor category', 18, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),

  -- Singapore
  ('singapore_sora',        'rates',       'Singapore SORA overnight rate',
   'MAS daily SORA series', 20, 0, 'RAW', 'FORWARD_FILL', '2005-01-01', TRUE),
  ('singapore_fx_reserves', 'flows',       'Singapore official foreign reserves USD',
   'MAS monthly FX reserves', 20, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),

  -- Australia
  ('australia_rba_rate',    'rates',       'RBA cash rate target',
   'ABS / RBA official cash rate', 17, 0, 'RAW', 'FORWARD_FILL', '1990-01-01', TRUE),
  ('australia_iron_exports','trade',       'Australia iron ore export value AUD',
   'ABS merchandise trade HS2601', 17, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),

  -- Trade / Complexity
  ('eci_usa',               'trade',       'USA Economic Complexity Index score',
   'Harvard Atlas ECI annual', 26, 0, 'RAW', 'FORWARD_FILL', '1964-01-01', TRUE),
  ('eci_china',             'trade',       'China Economic Complexity Index score',
   'Harvard Atlas ECI annual', 26, 0, 'RAW', 'FORWARD_FILL', '1964-01-01', TRUE),
  ('eci_global_dispersion', 'trade',       'Standard deviation of ECI scores across countries',
   'Harvard Atlas ECI cross-sectional std dev annual', 26, 0, 'ZSCORE', 'FORWARD_FILL', '1964-01-01', TRUE),
  ('trade_volume_yoy',      'trade',       'World trade volume YoY from Comtrade aggregate',
   'UN Comtrade global trade value USD annual YoY', 24, 0, 'ZSCORE', 'FORWARD_FILL', '1962-01-01', TRUE),
  ('us_china_trade_balance','trade',       'US-China bilateral trade balance USD',
   'UN Comtrade bilateral flow USA-CHN', 24, 0, 'ZSCORE', 'FORWARD_FILL', '1992-01-01', TRUE),
  ('wiod_gvc_participation','trade',       'Global value chain participation index from WIOD',
   'WIOD derived GVC integration', 27, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),

  -- Physical economy
  ('viirs_us_lights',       'alternative', 'VIIRS nighttime light intensity US aggregate',
   'NOAA VIIRS monthly composite', 28, 0, 'ZSCORE', 'FORWARD_FILL', '2012-01-01', TRUE),
  ('viirs_china_lights',    'alternative', 'VIIRS nighttime light intensity China aggregate',
   'NOAA VIIRS monthly composite', 28, 0, 'ZSCORE', 'FORWARD_FILL', '2012-01-01', TRUE),
  ('viirs_em_lights',       'alternative', 'VIIRS nighttime light EM composite',
   'NOAA VIIRS monthly weighted EM basket', 28, 0, 'ZSCORE', 'FORWARD_FILL', '2012-01-01', TRUE),

  -- EU KLEMS
  ('euklems_labor_prod_us', 'macro',       'US total economy labor productivity annual',
   'EU KLEMS GO per hour worked', 29, 0, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),
  ('euklems_tfp_eu',        'macro',       'EU total factor productivity annual',
   'EU KLEMS TFP growth EU aggregate', 29, 0, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),

  -- Patents
  ('patent_velocity_tech',  'alternative', 'USPTO patent application velocity tech/software',
   'PatentsView CPC G06 H01-H04', 31, 0, 'ZSCORE', 'FORWARD_FILL', '1976-01-01', TRUE),
  ('patent_velocity_energy','alternative', 'USPTO patent velocity clean energy CPC Y02',
   'PatentsView CPC Y02', 31, 0, 'ZSCORE', 'FORWARD_FILL', '1990-01-01', TRUE),
  ('patent_velocity_bio',   'alternative', 'USPTO patent velocity biotech CPC A61',
   'PatentsView CPC A61K/A61P', 31, 0, 'ZSCORE', 'FORWARD_FILL', '1976-01-01', TRUE),

  -- USDA NASS
  ('corn_yield_forecast',   'commodity',   'USDA NASS US corn yield forecast bu/acre',
   'NASS August crop report corn yield', 32, 0, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),
  ('wheat_planted_acres',   'commodity',   'USDA NASS US winter wheat planted acres',
   'NASS prospective plantings', 32, 0, 'ZSCORE', 'FORWARD_FILL', '1970-01-01', TRUE),
  ('crop_progress_corn',    'commodity',   'USDA NASS corn crop condition good/excellent pct',
   'NASS weekly crop progress report', 32, 0, 'RAW', 'FORWARD_FILL', '1986-01-01', FALSE),

  -- OFR
  ('ofr_fsm_credit',        'systemic',   'OFR Financial Stability Monitor credit category',
   'OFR FSM credit component score', 33, 0, 'RAW', 'FORWARD_FILL', '2000-01-01', TRUE),
  ('ofr_fsm_funding',       'systemic',   'OFR Financial Stability Monitor funding category',
   'OFR FSM funding/liquidity score', 33, 0, 'RAW', 'FORWARD_FILL', '2000-01-01', TRUE),
  ('ofr_fsm_leverage',      'systemic',   'OFR Financial Stability Monitor leverage category',
   'OFR FSM leverage score', 33, 0, 'RAW', 'FORWARD_FILL', '2000-01-01', TRUE),
  ('ofr_fsm_composite',     'systemic',   'OFR Financial Stability Monitor composite score',
   'OFR FSM overall vulnerability index', 33, 0, 'ZSCORE', 'FORWARD_FILL', '2000-01-01', TRUE),

  -- Opportunity Insights
  ('oi_consumer_spend',     'alternative', 'Consumer spending index vs Jan 2020 baseline',
   'OppInsights EconomicTracker', 34, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-13', FALSE),
  ('oi_employment_overall', 'alternative', 'Employment level index vs Jan 2020',
   'OppInsights EconomicTracker', 34, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-13', FALSE),
  ('oi_spend_low_income',   'alternative', 'Low income quintile consumer spend index',
   'OppInsights income Q1', 34, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-13', FALSE),
  ('oi_spend_high_income',  'alternative', 'High income quintile consumer spend index',
   'OppInsights income Q4', 34, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-13', FALSE),
  ('oi_k_shape_ratio',      'alternative', 'K-shape divergence: high minus low income spend',
   'oi_spend_high minus oi_spend_low', 34, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-13', FALSE),

  -- NOAA AIS
  ('ais_port_arrivals_la',  'alternative', 'Monthly vessel arrivals at Port of LA/LB',
   'NOAA AIS vessel track aggregation', 35, 0, 'ZSCORE', 'FORWARD_FILL', '2009-01-01', TRUE),
  ('ais_port_arrivals_rotterdam','alternative','Monthly vessel arrivals at Port of Rotterdam',
   'NOAA AIS vessel track aggregation', 35, 0, 'ZSCORE', 'FORWARD_FILL', '2009-01-01', TRUE),
  ('ais_tanker_utilization', 'alternative', 'Global tanker utilization rate from AIS speed data',
   'Avg speed of tanker class MMSI', 35, 0, 'ZSCORE', 'FORWARD_FILL', '2009-01-01', TRUE),

  -- GDELT
  ('gdelt_tone_usa',        'sentiment',   'GDELT average tone of US-focused events',
   'GDELT GKG ToneCharts', 36, 0, 'ZSCORE', 'FORWARD_FILL', '1979-01-01', FALSE),
  ('gdelt_conflict_global', 'sentiment',   'GDELT global conflict event count rolling 30d',
   'GDELT CAMEO event codes 14-20', 36, 0, 'ZSCORE', 'FORWARD_FILL', '1979-01-01', FALSE)

ON CONFLICT (name) DO NOTHING;
"""


def run_seed_v2(db_engine: Engine) -> None:
    """Execute v2 seed data inserts.

    Safe to run multiple times — ON CONFLICT DO NOTHING prevents duplicates.

    Parameters:
        db_engine: SQLAlchemy engine connected to the GRID database.
    """
    log.info("Running GRID v2 seed data inserts")

    with db_engine.begin() as conn:
        # Insert source catalog entries
        log.info("Inserting v2 source_catalog entries")
        conn.execute(text(SOURCE_CATALOG_SQL))

        # Insert feature registry entries
        log.info("Inserting v2 feature_registry entries")
        conn.execute(text(FEATURE_REGISTRY_SQL))

    log.info("GRID v2 seed data complete")


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    run_seed_v2(engine)
    print("v2 seed data inserted successfully")
