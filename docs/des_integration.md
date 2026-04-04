# Dallas Fed Energy Survey Integration

## Supported metrics

- `des_business_activity_index`
- `des_company_outlook_index`
- `des_outlook_uncertainty_index`
- `des_oil_production_index`
- `des_gas_production_index`
- `des_capex_index`
- `des_employment_index`
- `des_input_cost_index`
- `des_finding_development_costs_index`
- `des_lease_operating_expense_index`
- `des_prices_received_services_index`
- `des_equipment_utilization_index`
- `des_operating_margin_index`
- `des_wti_price_expectation_6m`
- `des_wti_price_expectation_1y`
- `des_wti_price_expectation_2y`
- `des_wti_price_expectation_5y`
- `des_hh_price_expectation_6m`
- `des_hh_price_expectation_1y`
- `des_hh_price_expectation_2y`
- `des_hh_price_expectation_5y`
- `des_breakeven_oil_us`
- `des_breakeven_gas_us`
- `des_breakeven_oil_permian`
- `des_breakeven_oil_eagle_ford`
- `des_special_questions_text`
- `des_comments_text`
- `des_report_summary_text`

## Ingestion workflow

1. `atlas/ingest/des_historical.py` crawls the Dallas Fed historical data page, downloads source files, and normalizes them into a tidy long-format table.
2. `atlas/ingest/des_reports.py` crawls the DES archive and parses quarterly report pages into structured records.
3. `atlas/tools/des_adapter.py` caches processed parquet outputs and exposes app-facing query helpers.
4. `scripts/des/sync_des.py` provides a deterministic sync entrypoint for historical datasets and report content.

## Cache layout

- Raw historical files: `data/raw/des/`
- Raw report HTML and parsed JSON: `data/raw/des/reports/`
- Processed historical table: `data/processed/des/des_historical.parquet`
- Processed report table: `data/processed/des/des_reports.parquet`

## Routing keys and synonyms

- `dallas fed energy survey`
- `des`
- `energy survey`
- `oil and gas survey`
- `business activity index`
- `company outlook`
- `uncertainty index`
- `oil production index`
- `gas production index`
- `capex index`
- `breakeven`
- `break-even`
- `price expectations`
- `henry hub expectations`
- `wti expectations`
- `survey comments`
- `special questions`

## Known limitations

- Dallas Fed historical workbooks vary by sheet layout, so the normalization layer relies on resilient header matching rather than a single hardcoded schema.
- The adapter currently supports deterministic single-metric retrieval and report-text retrieval; multi-source comparison charts are derived by helper functions rather than a dedicated executor intent.
- Basin-specific break-even metrics are normalized when the source workbook exposes them clearly in headers.

## Example queries

- `Show Dallas Fed gas production index since 2020`
- `What are Dallas Fed Henry Hub expectations?`
- `Compare WTI price expectations to current spot`
- `Summarize Dallas Fed special questions from the latest survey`
- `Show break-even oil price versus WTI`
