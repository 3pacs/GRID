"""Prepared, UNRUN review-overlay regressions; no live DB/provider required.

Native correction/original bodies and publication projections come from the
pinned POST-REPAIR-READONLY.jsonl receipt. Publication fixtures are projections,
NOT recomputed full-record attestations. FakeEngine models the trusted native
PostgreSQL full-record SHA result; it does not prove PostgreSQL serialization.
"""
import asyncio
import copy
import json
from datetime import date
from types import SimpleNamespace

import pytest
from intelligence import market_diary as diary

_RECEIPT = json.loads(r'''{"corrections":[{"kind":"correction","row":{"id":1958031151,"value":7811.54,"obs_date":"2026-10-09","series_id":"CORRECTION:SP500:close","source_id":1,"pull_status":"QUARANTINED","raw_payload":{"basis":"cash_price_index_daily_close_excluding_dividends","interval":"1d","obs_date":"2026-10-09","repair_id":"yf_close_reference_correction_20261017_v1","benchmark_URL":"https://fred.stlouisfed.org/data/SP500","evidence_sha256":"8075a955096d53d5bca130a77614f43d0a1b9eb3a9c05c9dd3d82916a9bbe219","original_row_id":1957381504,"repair_contract":"public_sp500_reference_correction_v1","benchmark_source":"S&P Dow Jones Indices via FRED","is_yahoo_capture":false,"consumer_adoption":"NOT_AUTHORIZED_BY_THIS_TRANSACTION","original_series_id":"YF:^GSPC:close","benchmark_series_id":"SP500","database_appended_at":"2026-10-17T03:50:03.199022+00:00","original_source_name":"yfinance","consumption_admission":"WITHHELD_REFERENCE_EVIDENCE_ONLY","original_publications":[{"id":14,"date":"2026-10-14","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"3fb5fe54fefc157de2e410b93fedb0f0","original_generated_at":"2026-10-14T22:34:55.125807+00:00","original_record_sha256":"acb937ee66b148d9609c2b9fafd2ddbcf259998362be42b11b88fd730cf27c62","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7718.94,"counterfactual_reference_prior":7805.45,"counterfactual_reference_change":-86.51,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957833018,1957797135],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true},{"id":15,"date":"2026-10-15","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"c40b9deab55d1711d449665fb87704bc","original_generated_at":"2026-10-15T22:34:32.430266+00:00","original_record_sha256":"671c264139652f9eccc6b2c98b18599f47c90db030ac7a94dc2853879ce18ac7","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7744.06,"counterfactual_reference_prior":7718.94,"counterfactual_reference_change":25.12,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957902402,1957833018],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true}],"original_row_snapshots":[{"id":1957381504,"sha256":"030b9f10900e488f36bed438a3a8cb681d4a3a0dc9940611e2c74eeb8d5e0e48","obs_date":"2026-10-09"},{"id":1957631102,"sha256":"cbcde1d46bf3f325cf184f4949d61241d2afbe043c8b6546135e591ee9d8652d","obs_date":"2026-10-12"},{"id":1957797135,"sha256":"282d8f6ef37b0149c401883119c8e00ce5b2ff5541252a88dfa1a8cf72e6d81b","obs_date":"2026-10-13"},{"id":1957833018,"sha256":"c16274de7ed32a7f6c8fbb65c5d15f28e9b297261ae955d6c14d49bc2a7f934d","obs_date":"2026-10-14"},{"id":1957902402,"sha256":"ed35597a1f3b63bf81d2647d300fce0ca1e9f0ba8c258d34e5eba82d922aecb9","obs_date":"2026-10-15"}],"yahoo_policy_qualified":false,"native_row_export_sha256":"449ebea6478ca898b3630799930e3ccf218fb705e7ad2503d95b4c9e798db732","provider_certified_final":false,"benchmark_series_updated_at":"2026-10-17T00:02:00Z","native_schema_export_sha256":"4c28518d9aba973107eab38b50bbeccee09b19941ba0005b47e45cf4fc2d8a31","public_evidence_observed_at":"2026-10-17T03:34:01+00:00","benchmark_first_publication_time":"NOT_ESTABLISHED","native_history_lineage_export_sha256":"23ada2cf14ea4972bf45aaec16988d65d39d2d98c30b39253d6d93cc08b9ef49"},"pull_timestamp":"2026-10-17T03:50:03.199022+00:00"},"row_sha256":"d6c4a75cebeb9254c58c0aabd9865a5d85b2de8ef318cce44b707d6ec088755f"},{"kind":"correction","row":{"id":1958031152,"value":7807.24,"obs_date":"2026-10-12","series_id":"CORRECTION:SP500:close","source_id":1,"pull_status":"QUARANTINED","raw_payload":{"basis":"cash_price_index_daily_close_excluding_dividends","interval":"1d","obs_date":"2026-10-12","repair_id":"yf_close_reference_correction_20261017_v1","benchmark_URL":"https://fred.stlouisfed.org/data/SP500","evidence_sha256":"8075a955096d53d5bca130a77614f43d0a1b9eb3a9c05c9dd3d82916a9bbe219","original_row_id":1957631102,"repair_contract":"public_sp500_reference_correction_v1","benchmark_source":"S&P Dow Jones Indices via FRED","is_yahoo_capture":false,"consumer_adoption":"NOT_AUTHORIZED_BY_THIS_TRANSACTION","original_series_id":"YF:^GSPC:close","benchmark_series_id":"SP500","database_appended_at":"2026-10-17T03:50:03.199022+00:00","original_source_name":"yfinance","consumption_admission":"WITHHELD_REFERENCE_EVIDENCE_ONLY","original_publications":[{"id":14,"date":"2026-10-14","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"3fb5fe54fefc157de2e410b93fedb0f0","original_generated_at":"2026-10-14T22:34:55.125807+00:00","original_record_sha256":"acb937ee66b148d9609c2b9fafd2ddbcf259998362be42b11b88fd730cf27c62","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7718.94,"counterfactual_reference_prior":7805.45,"counterfactual_reference_change":-86.51,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957833018,1957797135],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true},{"id":15,"date":"2026-10-15","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"c40b9deab55d1711d449665fb87704bc","original_generated_at":"2026-10-15T22:34:32.430266+00:00","original_record_sha256":"671c264139652f9eccc6b2c98b18599f47c90db030ac7a94dc2853879ce18ac7","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7744.06,"counterfactual_reference_prior":7718.94,"counterfactual_reference_change":25.12,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957902402,1957833018],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true}],"original_row_snapshots":[{"id":1957381504,"sha256":"030b9f10900e488f36bed438a3a8cb681d4a3a0dc9940611e2c74eeb8d5e0e48","obs_date":"2026-10-09"},{"id":1957631102,"sha256":"cbcde1d46bf3f325cf184f4949d61241d2afbe043c8b6546135e591ee9d8652d","obs_date":"2026-10-12"},{"id":1957797135,"sha256":"282d8f6ef37b0149c401883119c8e00ce5b2ff5541252a88dfa1a8cf72e6d81b","obs_date":"2026-10-13"},{"id":1957833018,"sha256":"c16274de7ed32a7f6c8fbb65c5d15f28e9b297261ae955d6c14d49bc2a7f934d","obs_date":"2026-10-14"},{"id":1957902402,"sha256":"ed35597a1f3b63bf81d2647d300fce0ca1e9f0ba8c258d34e5eba82d922aecb9","obs_date":"2026-10-15"}],"yahoo_policy_qualified":false,"native_row_export_sha256":"449ebea6478ca898b3630799930e3ccf218fb705e7ad2503d95b4c9e798db732","provider_certified_final":false,"benchmark_series_updated_at":"2026-10-17T00:02:00Z","native_schema_export_sha256":"4c28518d9aba973107eab38b50bbeccee09b19941ba0005b47e45cf4fc2d8a31","public_evidence_observed_at":"2026-10-17T03:34:01+00:00","benchmark_first_publication_time":"NOT_ESTABLISHED","native_history_lineage_export_sha256":"23ada2cf14ea4972bf45aaec16988d65d39d2d98c30b39253d6d93cc08b9ef49"},"pull_timestamp":"2026-10-17T03:50:03.199022+00:00"},"row_sha256":"f0e72ed4c8da249bd9682c6fc5c68167f909234cc3392d7a23c53bfdfb24d83b"},{"kind":"correction","row":{"id":1958031153,"value":7718.94,"obs_date":"2026-10-14","series_id":"CORRECTION:SP500:close","source_id":1,"pull_status":"QUARANTINED","raw_payload":{"basis":"cash_price_index_daily_close_excluding_dividends","interval":"1d","obs_date":"2026-10-14","repair_id":"yf_close_reference_correction_20261017_v1","benchmark_URL":"https://fred.stlouisfed.org/data/SP500","evidence_sha256":"8075a955096d53d5bca130a77614f43d0a1b9eb3a9c05c9dd3d82916a9bbe219","original_row_id":1957833018,"repair_contract":"public_sp500_reference_correction_v1","benchmark_source":"S&P Dow Jones Indices via FRED","is_yahoo_capture":false,"consumer_adoption":"NOT_AUTHORIZED_BY_THIS_TRANSACTION","original_series_id":"YF:^GSPC:close","benchmark_series_id":"SP500","database_appended_at":"2026-10-17T03:50:03.199022+00:00","original_source_name":"yfinance","consumption_admission":"WITHHELD_REFERENCE_EVIDENCE_ONLY","original_publications":[{"id":14,"date":"2026-10-14","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"3fb5fe54fefc157de2e410b93fedb0f0","original_generated_at":"2026-10-14T22:34:55.125807+00:00","original_record_sha256":"acb937ee66b148d9609c2b9fafd2ddbcf259998362be42b11b88fd730cf27c62","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7718.94,"counterfactual_reference_prior":7805.45,"counterfactual_reference_change":-86.51,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957833018,1957797135],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true},{"id":15,"date":"2026-10-15","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"c40b9deab55d1711d449665fb87704bc","original_generated_at":"2026-10-15T22:34:32.430266+00:00","original_record_sha256":"671c264139652f9eccc6b2c98b18599f47c90db030ac7a94dc2853879ce18ac7","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7744.06,"counterfactual_reference_prior":7718.94,"counterfactual_reference_change":25.12,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957902402,1957833018],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true}],"original_row_snapshots":[{"id":1957381504,"sha256":"030b9f10900e488f36bed438a3a8cb681d4a3a0dc9940611e2c74eeb8d5e0e48","obs_date":"2026-10-09"},{"id":1957631102,"sha256":"cbcde1d46bf3f325cf184f4949d61241d2afbe043c8b6546135e591ee9d8652d","obs_date":"2026-10-12"},{"id":1957797135,"sha256":"282d8f6ef37b0149c401883119c8e00ce5b2ff5541252a88dfa1a8cf72e6d81b","obs_date":"2026-10-13"},{"id":1957833018,"sha256":"c16274de7ed32a7f6c8fbb65c5d15f28e9b297261ae955d6c14d49bc2a7f934d","obs_date":"2026-10-14"},{"id":1957902402,"sha256":"ed35597a1f3b63bf81d2647d300fce0ca1e9f0ba8c258d34e5eba82d922aecb9","obs_date":"2026-10-15"}],"yahoo_policy_qualified":false,"native_row_export_sha256":"449ebea6478ca898b3630799930e3ccf218fb705e7ad2503d95b4c9e798db732","provider_certified_final":false,"benchmark_series_updated_at":"2026-10-17T00:02:00Z","native_schema_export_sha256":"4c28518d9aba973107eab38b50bbeccee09b19941ba0005b47e45cf4fc2d8a31","public_evidence_observed_at":"2026-10-17T03:34:01+00:00","benchmark_first_publication_time":"NOT_ESTABLISHED","native_history_lineage_export_sha256":"23ada2cf14ea4972bf45aaec16988d65d39d2d98c30b39253d6d93cc08b9ef49"},"pull_timestamp":"2026-10-17T03:50:03.199022+00:00"},"row_sha256":"110a9e51aab0a6b956c7beff8b3726229dffe1ea21340da2bc28fec961f99c37"},{"kind":"correction","row":{"id":1958031154,"value":7744.06,"obs_date":"2026-10-15","series_id":"CORRECTION:SP500:close","source_id":1,"pull_status":"QUARANTINED","raw_payload":{"basis":"cash_price_index_daily_close_excluding_dividends","interval":"1d","obs_date":"2026-10-15","repair_id":"yf_close_reference_correction_20261017_v1","benchmark_URL":"https://fred.stlouisfed.org/data/SP500","evidence_sha256":"8075a955096d53d5bca130a77614f43d0a1b9eb3a9c05c9dd3d82916a9bbe219","original_row_id":1957902402,"repair_contract":"public_sp500_reference_correction_v1","benchmark_source":"S&P Dow Jones Indices via FRED","is_yahoo_capture":false,"consumer_adoption":"NOT_AUTHORIZED_BY_THIS_TRANSACTION","original_series_id":"YF:^GSPC:close","benchmark_series_id":"SP500","database_appended_at":"2026-10-17T03:50:03.199022+00:00","original_source_name":"yfinance","consumption_admission":"WITHHELD_REFERENCE_EVIDENCE_ONLY","original_publications":[{"id":14,"date":"2026-10-14","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"3fb5fe54fefc157de2e410b93fedb0f0","original_generated_at":"2026-10-14T22:34:55.125807+00:00","original_record_sha256":"acb937ee66b148d9609c2b9fafd2ddbcf259998362be42b11b88fd730cf27c62","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7718.94,"counterfactual_reference_prior":7805.45,"counterfactual_reference_change":-86.51,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957833018,1957797135],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true},{"id":15,"date":"2026-10-15","table":"market_diary","lineage_basis":"reconstructed date/source/value/arithmetic consistency; no original input-row receipt","original_content_md5":"c40b9deab55d1711d449665fb87704bc","original_generated_at":"2026-10-15T22:34:32.430266+00:00","original_record_sha256":"671c264139652f9eccc6b2c98b18599f47c90db030ac7a94dc2853879ce18ac7","old_publication_preserved":true,"correction_notice_published_at":null,"counterfactual_reference_close":7744.06,"counterfactual_reference_prior":7718.94,"counterfactual_reference_change":25.12,"original_input_row_receipt_present":false,"reconstructed_current_prior_row_ids":[1957902402,1957833018],"new_evidence_available_at_original_publication":"NOT_ESTABLISHED","counterfactual_is_new_evidence_not_old_publication":true}],"original_row_snapshots":[{"id":1957381504,"sha256":"030b9f10900e488f36bed438a3a8cb681d4a3a0dc9940611e2c74eeb8d5e0e48","obs_date":"2026-10-09"},{"id":1957631102,"sha256":"cbcde1d46bf3f325cf184f4949d61241d2afbe043c8b6546135e591ee9d8652d","obs_date":"2026-10-12"},{"id":1957797135,"sha256":"282d8f6ef37b0149c401883119c8e00ce5b2ff5541252a88dfa1a8cf72e6d81b","obs_date":"2026-10-13"},{"id":1957833018,"sha256":"c16274de7ed32a7f6c8fbb65c5d15f28e9b297261ae955d6c14d49bc2a7f934d","obs_date":"2026-10-14"},{"id":1957902402,"sha256":"ed35597a1f3b63bf81d2647d300fce0ca1e9f0ba8c258d34e5eba82d922aecb9","obs_date":"2026-10-15"}],"yahoo_policy_qualified":false,"native_row_export_sha256":"449ebea6478ca898b3630799930e3ccf218fb705e7ad2503d95b4c9e798db732","provider_certified_final":false,"benchmark_series_updated_at":"2026-10-17T00:02:00Z","native_schema_export_sha256":"4c28518d9aba973107eab38b50bbeccee09b19941ba0005b47e45cf4fc2d8a31","public_evidence_observed_at":"2026-10-17T03:34:01+00:00","benchmark_first_publication_time":"NOT_ESTABLISHED","native_history_lineage_export_sha256":"23ada2cf14ea4972bf45aaec16988d65d39d2d98c30b39253d6d93cc08b9ef49"},"pull_timestamp":"2026-10-17T03:50:03.199022+00:00"},"row_sha256":"3c53e7f386262c057f1d3d195e8f52f05db2efffe6a0601d5060aec0707b6b78"}],"originals":[{"kind":"raw_row","row":{"id":1957381504,"value":7796.990234375,"obs_date":"2026-10-09","series_id":"YF:^GSPC:close","source_id":2,"pull_status":"SUCCESS","raw_payload":null,"pull_timestamp":"2026-10-09T16:02:19.914863+00:00"},"row_json_md5":"a204082ca358ac948b85c5a47b064a50"},{"kind":"raw_row","row":{"id":1957631102,"value":7816.14013671875,"obs_date":"2026-10-12","series_id":"YF:^GSPC:close","source_id":2,"pull_status":"SUCCESS","raw_payload":null,"pull_timestamp":"2026-10-12T13:31:50.360701+00:00"},"row_json_md5":"a17e592f4c91cd0581848a5badef8816"},{"kind":"raw_row","row":{"id":1957797135,"value":7805.4501953125,"obs_date":"2026-10-13","series_id":"YF:^GSPC:close","source_id":2,"pull_status":"SUCCESS","raw_payload":null,"pull_timestamp":"2026-10-13T22:02:50.180217+00:00"},"row_json_md5":"b0a49ffb3e2b3f9b34257f060283c0be"},{"kind":"raw_row","row":{"id":1957833018,"value":7770.97021484375,"obs_date":"2026-10-14","series_id":"YF:^GSPC:close","source_id":2,"pull_status":"SUCCESS","raw_payload":null,"pull_timestamp":"2026-10-14T13:32:09.072165+00:00"},"row_json_md5":"670b47028c02a286629bba38395b675c"},{"kind":"raw_row","row":{"id":1957902402,"value":7681.47998046875,"obs_date":"2026-10-15","series_id":"YF:^GSPC:close","source_id":2,"pull_status":"SUCCESS","raw_payload":null,"pull_timestamp":"2026-10-15T13:32:13.667514+00:00"},"row_json_md5":"17511ae32b3a11b5a77554a2943baaf3"}],"publications":[{"kind":"diary","id":14,"date":"2026-10-14","generated_at":"2026-10-14T22:34:55.125807+00:00","market_moves":{"indices":{"VIX":{"close":15.75,"change":0.17,"source":"yfinance","obs_date":"2026-10-14","change_pct":1.09,"price_basis":"raw_close"},"Nasdaq":{"close":27016.04,"change":-112.13,"source":"yfinance","obs_date":"2026-10-14","change_pct":-0.41,"price_basis":"raw_close"},"S&P 500":{"close":7770.97,"change":-34.48,"source":"yfinance","obs_date":"2026-10-14","change_pct":-0.44,"price_basis":"raw_close"},"Dow Jones":{"close":51658.18,"change":-634.52,"source":"yfinance","obs_date":"2026-10-14","change_pct":-1.21,"price_basis":"raw_close"},"Russell 2000":{"close":2737.47,"change":-37.36,"source":"yfinance","obs_date":"2026-10-14","change_pct":-1.35,"price_basis":"raw_close"}},"notable":[{"asset":"Gold","close":4106.3,"source":"yfinance","obs_date":"2026-10-14","change_pct":-2.43,"price_basis":"raw_close"},{"asset":"Crude Oil","close":95.43,"source":"yfinance","obs_date":"2026-10-14","change_pct":3.51,"price_basis":"raw_close"},{"asset":"Dollar (UUP)","close":29.18,"source":"yfinance","obs_date":"2026-10-14","change_pct":0.55,"price_basis":"raw_close"},{"asset":"Long Bonds (TLT)","close":77.77,"source":"yfinance","obs_date":"2026-10-14","change_pct":-0.99,"price_basis":"raw_close"}],"price_basis":"raw_close","price_source":"yfinance","prices_enabled":true,"sector_leaders":[{"etf":"XLE","sector":"Energy","source":"yfinance","obs_date":"2026-10-14","change_pct":1.97,"price_basis":"raw_close"},{"etf":"XLU","sector":"Utilities","source":"yfinance","obs_date":"2026-10-14","change_pct":0.46,"price_basis":"raw_close"},{"etf":"XLI","sector":"Industrials","source":"yfinance","obs_date":"2026-10-14","change_pct":0.12,"price_basis":"raw_close"}],"sector_laggards":[{"etf":"XLY","sector":"Consumer Disc","source":"yfinance","obs_date":"2026-10-14","change_pct":-0.68,"price_basis":"raw_close"},{"etf":"XLRE","sector":"Real Estate","source":"yfinance","obs_date":"2026-10-14","change_pct":-0.75,"price_basis":"raw_close"},{"etf":"XLF","sector":"Financials","source":"yfinance","obs_date":"2026-10-14","change_pct":-2.27,"price_basis":"raw_close"}]},"narrative_model":"oracle","narrative_fallback":false,"content_bytes":2385,"content_md5":"3fb5fe54fefc157de2e410b93fedb0f0","content":"**WHAT HAPPENED**\nThe S&P 500 fell 0.44% to 7770.97, leading a broad risk-off session. The Dow Jones Industrial Average dropped 1.21% to 51658.18, while the Nasdaq Composite declined 0.41% to 27016.04. Small caps lagged significantly, with the Russell 2000 down 1.35% to 2737.47. Volatility rose, with the VIX increasing 1.09% to 15.75. Sector performance was bifurcated: Energy (XLE) led gains at +1.97%, while Financials (XLF) suffered the steepest decline at -2.27%. Real Estate (XLRE) fell 0.75% and Consumer Discretionary (XLY) dropped 0.68%. In commodities, crude oil surged 3.51% to $95.43, contrasting sharply with a 2.43% drop in gold to $4,106.30. The dollar strengthened 0.55% (UUP at 29.18), and long-duration bonds (TLT) declined 0.99% to 77.77.\n\n**WHY**\nCause not identified. The supplied data contains no specific actor, corporate action, or macro signal explicitly named as the driver of the day's moves. While the simultaneous rise in crude oil and fall in financials suggests a potential energy shock or rate sensitivity, the data does not confirm a specific causal event. Conditions such as volatility and sector rotation are observed but cannot be presented as the cause without a named driver.\n\n**RIGHT**\nNo pre-open thesis to grade. The data explicitly states \"Pre-open thesis: none (no pre-open snapshot).\"\n\n**WRONG**\nNo pre-open thesis to grade.\n\n**WATCH TOMORROW**\nNo specific catalysts with ticker, time, and expected impact are present in the supplied context. The active hypotheses list untested convergence signals for DVN, MESO, MLTX, TGTX, and IBRX (all leaning PUT with 0.85 confidence), but these are model states, not scheduled market catalysts. Recent kill postmortems indicate that previous hypotheses linking signal spikes to LLM task anomalies failed, suggesting caution in relying on similar correlation-based predictions.\n\n\n---\n\n## Data Appendix\n\n| Index | Close | Change |\n|-------|-------|--------|\n| S&P 500 | 7770.97 | -0.44% |\n| Dow Jones | 51658.18 | -1.21% |\n| Nasdaq | 27016.04 | -0.41% |\n| Russell 2000 | 2737.47 | -1.35% |\n| VIX | 15.75 | +1.09% |\n\n| Source | Ticker | Direction | Date |\n|--------|--------|-----------|------|\n| Insider | NDSN | SELL | 2026-10-14 |\n| Insider | NRIX | UNUSUAL_SELL | 2026-10-14 |\n| Insider | PINS | UNUSUAL_SELL | 2026-10-14 |\n| Insider | XYZ | SELL | 2026-10-14 |\n| Insider | PB | SELL | 2026-10-14 |\n","content_truncated":false},{"kind":"diary","id":15,"date":"2026-10-15","generated_at":"2026-10-15T22:34:32.430266+00:00","market_moves":{"indices":{"VIX":{"close":17.51,"change":1.76,"source":"yfinance","obs_date":"2026-10-15","change_pct":11.17,"price_basis":"raw_close"},"Nasdaq":{"close":26339.08,"change":-676.96,"source":"yfinance","obs_date":"2026-10-15","change_pct":-2.51,"price_basis":"raw_close"},"S&P 500":{"close":7681.48,"change":-89.49,"source":"yfinance","obs_date":"2026-10-15","change_pct":-1.15,"price_basis":"raw_close"},"Dow Jones":{"close":51921.68,"change":263.5,"source":"yfinance","obs_date":"2026-10-15","change_pct":0.51,"price_basis":"raw_close"},"Russell 2000":{"close":2752.01,"change":14.54,"source":"yfinance","obs_date":"2026-10-15","change_pct":0.53,"price_basis":"raw_close"}},"notable":[{"asset":"Gold","close":4177.7,"source":"yfinance","obs_date":"2026-10-15","change_pct":1.74,"price_basis":"raw_close"},{"asset":"Crude Oil","close":94.54,"source":"yfinance","obs_date":"2026-10-15","change_pct":-0.93,"price_basis":"raw_close"},{"asset":"Dollar (UUP)","close":29,"source":"yfinance","obs_date":"2026-10-15","change_pct":-0.63,"price_basis":"raw_close"},{"asset":"Long Bonds (TLT)","close":80.34,"source":"yfinance","obs_date":"2026-10-15","change_pct":3.3,"price_basis":"raw_close"}],"price_basis":"raw_close","price_source":"yfinance","prices_enabled":true,"sector_leaders":[{"etf":"XLV","sector":"Health Care","source":"yfinance","obs_date":"2026-10-15","change_pct":3.87,"price_basis":"raw_close"},{"etf":"XLP","sector":"Consumer Staples","source":"yfinance","obs_date":"2026-10-15","change_pct":3.83,"price_basis":"raw_close"},{"etf":"XLB","sector":"Materials","source":"yfinance","obs_date":"2026-10-15","change_pct":1.25,"price_basis":"raw_close"}],"sector_laggards":[{"etf":"XLC","sector":"Communications","source":"yfinance","obs_date":"2026-10-15","change_pct":-1.41,"price_basis":"raw_close"},{"etf":"XLI","sector":"Industrials","source":"yfinance","obs_date":"2026-10-15","change_pct":-1.52,"price_basis":"raw_close"},{"etf":"XLK","sector":"Technology","source":"yfinance","obs_date":"2026-10-15","change_pct":-3.82,"price_basis":"raw_close"}]},"narrative_model":"oracle","narrative_fallback":false,"content_bytes":1685,"content_md5":"c40b9deab55d1711d449665fb87704bc","content":"**WHAT HAPPENED**\nThe market experienced a sharp divergence, led by a 2.51% plunge in the Nasdaq (26,339.08) and a 1.15% drop in the S&P 500 (7,681.48). Technology (XLK) was the primary driver of this decline, falling 3.82%, while Industrials (XLI) and Communications (XLC) also lagged. In contrast, the Dow Jones (+0.51%) and Russell 2000 (+0.53%) posted gains. Defensive sectors dominated, with Health Care (XLV) surging 3.87% and Consumer Staples (XLP) rising 3.83%. Volatility spiked as the VIX jumped 11.17% to 17.51. Safe-haven assets rallied, with Gold up 1.74% to 4,177.7 and Long Bonds (TLT) gaining 3.30%. Crude Oil slipped 0.93% to 94.54, and the Dollar (UUP) declined 0.63%.\n\n**WHY**\nCause not identified. The supplied data does not name a specific actor or signal responsible for the day's move. The observed shift into defensives and rising volatility may reflect changing sentiment or positioning, but no explicit driver is present in the data.\n\n**RIGHT**\nNo pre-open thesis to grade.\n\n**WRONG**\nNo pre-open thesis to grade.\n\n**WATCH TOMORROW**\nNo specific catalysts with ticker, time, and expected impact are present in the supplied context to list.\n\n\n---\n\n## Data Appendix\n\n| Index | Close | Change |\n|-------|-------|--------|\n| S&P 500 | 7681.48 | -1.15% |\n| Dow Jones | 51921.68 | +0.51% |\n| Nasdaq | 26339.08 | -2.51% |\n| Russell 2000 | 2752.01 | +0.53% |\n| VIX | 17.51 | +11.17% |\n\n| Source | Ticker | Direction | Date |\n|--------|--------|-----------|------|\n| Insider | CRESY | BUY | 2026-10-15 |\n| Insider | RSKD | UNUSUAL_SELL | 2026-10-15 |\n| Insider | NRIX | SELL | 2026-10-15 |\n| Insider | CSHR | SELL | 2026-10-14 |\n| Insider | BETA | SELL | 2026-10-14 |\n","content_truncated":false}]}''')
_AS_OF = "2026-10-17T03:50:03.199022Z"
_PUB_HASHES = {
    14: "acb937ee66b148d9609c2b9fafd2ddbcf259998362be42b11b88fd730cf27c62",
    15: "671c264139652f9eccc6b2c98b18599f47c90db030ac7a94dc2853879ce18ac7",
}
_ORIG_HASHES = {item["id"]: item["sha256"] for item in
                _RECEIPT["corrections"][0]["row"]["raw_payload"]["original_row_snapshots"]}


class FakeEngine:
    def __init__(self):
        self.rows = []
        for publication in _RECEIPT["publications"]:
            body = {k: copy.deepcopy(publication[k]) for k in (
                "id", "date", "generated_at", "market_moves", "narrative_model",
                "narrative_fallback", "content")}
            self.rows.append(("publication", body, _PUB_HASHES[body["id"]], "Etc/UTC"))
        self.rows += [("originals", copy.deepcopy(x["row"]), _ORIG_HASHES[x["row"]["id"]], "Etc/UTC")
                      for x in _RECEIPT["originals"]]
        self.rows += [("corrections", copy.deepcopy(x["row"]), x["row_sha256"], "Etc/UTC")
                      for x in _RECEIPT["corrections"]]
        self.calls = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, statement, params):
        sql = str(statement)
        self.calls.append((sql, copy.deepcopy(params)))
        assert sql.lstrip().startswith("WITH publication AS")
        assert "UNION ALL" in sql
        assert "r.raw_payload ->> 'repair_id' = :repair" in sql
        assert "r.obs_date = ANY(CAST(:scoped_dates AS date[]))" in sql
        assert params["scoped_dates"] == [date(2026, 10, d) for d in (9, 12, 13, 14, 15)]
        assert len(params["original_ids"]) == 5 and len(params["correction_ids"]) == 4
        # Model bounded result membership; SQL execution/serialization is a
        # separate future integration check, never claimed by this fake.
        rows = [r for r in self.rows if (
            r[0] == "publication" and r[1]["id"] == params["id"]
            or r[0] != "publication" and params["reference_visible"])]
        return SimpleNamespace(fetchall=lambda: copy.deepcopy(rows))


def review(engine, day=15, cutoff=_AS_OF):
    return diary.get_diary_reference_review(engine, date(2026, 10, day), reference_as_of=cutoff)


def unavailable(result):
    assert result["reference"]["status"] == "unavailable"
    assert "sp500" not in result["reference"]
    assert result["review_only"] is True and result["scoring_admission"] == "held"


@pytest.mark.parametrize("day,close,prior,change", [
    (14, 7718.94, 7805.45, -86.51), (15, 7744.06, 7718.94, 25.12)])
def test_exact_reference_keeps_original_separate(day, close, prior, change):
    engine = FakeEngine()
    before = copy.deepcopy(engine.rows)
    result = review(engine, day)
    assert result["reference"]["status"] == "available"
    assert result["reference"]["sp500"] == {
        "obs_date": f"2026-10-{day}", "close": close, "prior_close": prior, "change": change}
    assert result["original"] == next(r[1] for r in before if r[0] == "publication" and r[1]["id"] == day)
    assert engine.rows == before and len(engine.calls) == 1
    assert result["reference"]["original_input_row_receipt_present"] is False
    assert result["reference"]["correction_notice_published_at"] is None
    assert result["reference"]["status_of_stored_rows"] == "QUARANTINED"
    assert result["reference"]["oct13_agreement"]["numeric_correction_appended"] is False
    assert result["reference"]["database_appended_at"] == "2026-10-17T03:50:03.199022+00:00"


@pytest.mark.parametrize("cutoff", [
    "2026-10-15T22:34:32.430266Z", "2026-10-17T03:34:00.999999Z",
    "2026-10-17T03:34:01Z", "2026-10-17T03:50:03.199021Z"])
def test_old_publication_evidence_and_append_cutoffs(cutoff):
    engine = FakeEngine()
    result = review(engine, cutoff=cutoff)
    unavailable(result)
    assert result["original"]["id"] == 15
    assert engine.calls[0][1]["reference_visible"] is False


def test_before_original_publication_does_not_disclose_original():
    engine = FakeEngine()
    result = review(engine, cutoff="2026-10-15T22:34:32.430265Z")
    unavailable(result)
    assert result["original"] is None and not engine.calls


def test_equivalent_aware_offset_is_accepted():
    assert review(FakeEngine(), cutoff="2026-10-17T05:50:03.199022+02:00")["reference"]["status"] == "available"


@pytest.mark.parametrize("cutoff", [None, "", "bad", "2026-10-17T04:00:00"])
def test_invalid_or_naive_asof_is_rejected(cutoff):
    engine = FakeEngine()
    with pytest.raises(ValueError):
        review(engine, cutoff=cutoff)
    assert not engine.calls


@pytest.mark.parametrize("day", [9, 12, 13, 16])
def test_only_two_original_publications_in_scope(day):
    engine = FakeEngine()
    unavailable(review(engine, day=day))
    assert not engine.calls


@pytest.mark.parametrize("kind", ["publication", "originals", "corrections"])
def test_missing_snapshot_refuses(kind):
    engine = FakeEngine()
    index = next(i for i, r in enumerate(engine.rows) if r[0] == kind and (kind != "publication" or r[1]["id"] == 15))
    engine.rows.pop(index)
    unavailable(review(engine))


@pytest.mark.parametrize("kind", ["publication", "originals", "corrections"])
def test_changed_full_native_preimage_refuses(kind):
    engine = FakeEngine()
    i = next(i for i, r in enumerate(engine.rows) if r[0] == kind and (kind != "publication" or r[1]["id"] == 15))
    tag, body, digest, tz = engine.rows[i]
    body["unexpected_extra_field"] = True
    engine.rows[i] = (tag, body, "0" * 64, tz)
    unavailable(review(engine))


@pytest.mark.parametrize("foreign_repair,foreign_date", [
    ("other_repair", "2026-10-15"), ("other_repair", "2026-10-13"),
    ("yf_close_reference_correction_20261017_v1", "2026-10-16")])
def test_extra_conflicting_scoped_or_same_repair_member_refuses(foreign_repair, foreign_date):
    engine = FakeEngine()
    body = copy.deepcopy(next(r[1] for r in engine.rows if r[0] == "corrections"))
    body.update(id=1958031155, obs_date=foreign_date)
    body["raw_payload"]["repair_id"] = foreign_repair
    engine.rows.append(("corrections", body, "0" * 64, "Etc/UTC"))
    unavailable(review(engine))


def test_unrelated_date_foreign_repair_is_outside_query_scope():
    engine = FakeEngine()
    review(engine)
    sql, params = engine.calls[0]
    # Intentional finite SQL scope: an unrelated foreign repair/date is not a
    # reason to disable this historical review; it is not fetched by the query.
    assert date(2026, 10, 16) not in params["scoped_dates"]
    assert "r.series_id = :series" in sql
    assert params["repair"] == "yf_close_reference_correction_20261017_v1"


@pytest.mark.parametrize("field,value", [
    ("source_id", 2), ("series_id", "YF:^GSPC:close"),
    ("pull_status", "SUCCESS"), ("obs_date", "2026-10-16"), ("value", 0)])
def test_record_binding_refuses_even_with_stubbed_matching_digest(field, value):
    engine = FakeEngine()
    next(r[1] for r in engine.rows if r[0] == "corrections")[field] = value
    unavailable(review(engine))


@pytest.mark.parametrize("field,value", [
    ("is_yahoo_capture", True), ("yahoo_policy_qualified", True),
    ("consumption_admission", "SUCCESS"), ("original_row_id", 0),
    ("public_evidence_observed_at", "2026-10-18T00:00:00Z"),
    ("database_appended_at", "2026-10-18T00:00:00Z")])
def test_payload_binding_refuses_even_with_stubbed_matching_digest(field, value):
    engine = FakeEngine()
    next(r[1] for r in engine.rows if r[0] == "corrections")["raw_payload"][field] = value
    unavailable(review(engine))


def test_non_utc_native_serialization_refuses():
    engine = FakeEngine()
    engine.rows = [(tag, body, digest, "America/New_York") for tag, body, digest, _ in engine.rows]
    unavailable(review(engine))


def test_read_failure_is_unavailable():
    engine = FakeEngine()
    def fail(*args):
        raise RuntimeError("fixture-only read failure")
    engine.execute = fail
    unavailable(review(engine))


def test_existing_route_default_and_opt_in_are_separate(monkeypatch):
    from api.routers import intelligence_thesis as router
    engine = FakeEngine()
    sentinel = {"content": "original", "market_moves": {"unchanged": True}}
    monkeypatch.setattr(router, "get_db_engine", lambda: engine)
    monkeypatch.setattr(diary, "get_diary_entry", lambda *args: sentinel)
    original = asyncio.run(router.get_diary(date="2026-10-15", reference_overlay=False, reference_as_of=None, _token="fixture"))
    assert original is sentinel and not engine.calls
    def forbidden(*args):
        raise AssertionError("overlay must bypass original ensure_table path")
    monkeypatch.setattr(diary, "get_diary_entry", forbidden)
    result = asyncio.run(router.get_diary(date="2026-10-15", reference_overlay=True, reference_as_of=_AS_OF, _token="fixture"))
    assert result["reference"]["status"] == "available"


@pytest.mark.parametrize("day,cutoff", [(None, _AS_OF), ("2026-10-15", None), ("bad", _AS_OF),
                                         ("2026-10-15", "2026-10-17T04:00:00")])
def test_route_requires_explicit_date_and_aware_cutoff(monkeypatch, day, cutoff):
    from fastapi import HTTPException
    from api.routers import intelligence_thesis as router
    monkeypatch.setattr(router, "get_db_engine", lambda: FakeEngine())
    with pytest.raises(HTTPException) as exc:
        asyncio.run(router.get_diary(date=day, reference_overlay=True, reference_as_of=cutoff, _token="fixture"))
    assert exc.value.status_code == 400


def test_duplicate_correction_member_refuses():
    engine = FakeEngine()
    engine.rows.append(copy.deepcopy(next(r for r in engine.rows if r[0] == "corrections")))
    unavailable(review(engine))
