# Hospital Data Cleaner & Address Validator

## Overview
This tool cleans messy hospital datasets and validates addresses using Google Geocoding. It outputs a standardized dataset ready for human review and downstream matching.

## What it does (finalized rules)
- Hospital names: lowerCamelCase (machine-friendly IDs)
- Addresses: human-readable Title Case with expanded directions/suffixes (e.g., N. → North, St → Street)
- City: Title Case
- State: USPS 2-letter codes (maps full names like "Texas" → "TX"; invalid → blank). Flag State Valid (Y/N)
- ZIP: keep #####; if ZIP+4 present, format #####-####; invalid → blank. Flag ZIP Valid (Y/N)
- Phone/Fax: digits only, standardized to XXXXXXXXXX; invalid → blank
- Placeholders removed: unknown, address unknown, n/a, na, null, -, ., none, single-letter (including "1"), and X-only tokens like "xxxxx" (also when embedded like "xxxxx, 1")
- Deduplication: drop exact duplicates based on cleaned core fields
- Address validation: Google Geocoding with retry/backoff; stores Verified Address, Confidence, Lat/Lng

## Files
- `hospital_data_validator_final.py` – main processor
- `google_address_validator.py` – Google Geocoding wrapper with throttling and retries
- `Concierge Hospitals.xlsx` – source data
- Outputs:
  - `Hospital_Data_Validated.xlsx` – cleaned dataset
  - `Before_After_Comparison.xlsx` – side-by-side original vs cleaned
  - `Validation_Report.json` – validation stats and examples
  - `hospital_data_quality_report.xlsx` – summary metrics and sample rows

## Setup
1) Python 3.10+ recommended
2) Install deps
```bash
pip install -r requirements.txt
```
3) Google Geocoding API
- Geocoding API must be enabled and key must be valid. The validator defaults to a configured key and includes retry/backoff for stability.

## Usage
Run full processing (all active rows):
```bash
python3 hospital_data_validator_final.py
```
Optional rate control (default 50 req/s):
```bash
export GOOGLE_MAPS_RPS=50
python3 hospital_data_validator_final.py
```
Run a sample (e.g., 100 rows) for a quick test:
```bash
python3 -c "from hospital_data_validator_final import HospitalDataProcessor; p=HospitalDataProcessor(); p.process_file('Concierge Hospitals.xlsx', limit=100); p.save_results()"
```

## Output columns (Validated Data)
- Hospital Name, Address, Address2, City, State, ZIP, Phone, Fax
- State Valid (Y/N), ZIP Valid (Y/N)
- Validation Status, Verified Address, Address Confidence, Was Corrected, Latitude, Longitude

## Quality and consistency
- Formatting is consistent across all rows
- Placeholders removed or blanked
- State and ZIP flagged for quick filtering
- Duplicates removed after cleaning
- Target cleanliness ≥ 95%

## Troubleshooting
- If validation status shows REQUEST_DENIED: verify Geocoding API is enabled, key is valid, and restrictions allow server calls
- If you hit rate limits: lower `GOOGLE_MAPS_RPS` to 20

## License
Internal use.