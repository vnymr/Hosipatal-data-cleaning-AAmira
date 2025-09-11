# Hospital Data Cleaner & Address Validator

## Overview
This script cleans hospital data and validates addresses using Google Maps API to verify that addresses are real and correct.

## Features
✅ **Data Cleaning**
- Standardizes hospital names
- Expands address abbreviations (SW → Southwest, Ave → Avenue)
- Validates states and ZIP codes
- Formats phone numbers to 10 digits
- Removes invalid/placeholder data

✅ **Address Validation** 
- Verifies if addresses actually exist
- Corrects address errors automatically
- Provides confidence levels (HIGH/MEDIUM/LOW)
- Returns GPS coordinates for mapping
- Shows which addresses were corrected

## Files

### Input
- `Concierge Hospitals.xlsx` - Original hospital data

### Scripts
- `hospital_data_validator_final.py` - Main processing script

### Output Files
1. **`Hospital_Data_Validated.xlsx`** - Cleaned and validated data with:
   - Cleaned hospital names and addresses
   - Validation status for each address
   - Verified addresses from Google Maps
   - GPS coordinates
   - Confidence levels

2. **`Before_After_Comparison.xlsx`** - Side-by-side comparison showing:
   - Original vs cleaned data
   - Original vs verified addresses
   - What was corrected

3. **`Validation_Report.json`** - Statistics including:
   - Total addresses processed
   - Number verified, corrected, invalid
   - Example validations

## Setup

### 1. Install Required Libraries
```bash
pip install pandas openpyxl googlemaps requests
```

### 2. Enable Google Maps Geocoding API
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Enable "Geocoding API" for your project
3. API Key: `AIzaSyDKgAcoQOKHsbg6KEjRX8UVXUCe7BFaLAc`

## Usage

### Run Full Processing (All 1,436 hospitals)
```bash
python hospital_data_validator_final.py
```

### Test Mode (First 10 records only)
Edit the script and change line 579:
```python
processor.process_file(INPUT_FILE, limit=10)  # For testing
```

## Validation Status Meanings

- **✅ Verified - Exact Match**: Address exists exactly as provided (ROOFTOP level)
- **✅ Verified - Street Level**: Address exists on the street (RANGE_INTERPOLATED)
- **✅ Verified - Area Level**: Address area exists (GEOMETRIC_CENTER)
- **⚠️ Approximate Only**: Only city/area could be verified
- **❌ Not Found**: Address does not exist
- **🔧 Corrected**: Address had errors that were fixed

## Example Results

### Before/After Cleaning
```
Original: "3509 SW 34th Ave Circle, Ocala, FL"
Cleaned:  "3509 Southwest 34th Avenue Circle, Ocala, FL"
Verified: "3509 SW 34th Avenue Cir, Ocala, FL 34474, USA"
Status:   Verified - Exact Match
GPS:      29.1577, -82.1840
```

### Address Correction
```
Original: "1670 ST VINCENTS, Middleburg, FL" (missing "WAY")
Verified: "1670 St Vincents Way, Middleburg, FL 32068, USA"
Status:   Verified - Corrected
```

### Invalid Address
```
Original: "Unknown Address"
Status:   Not Found
```

## Notes

- First run may take 25-30 minutes for all 1,436 records
- Google Maps API allows 40,000 free requests/month
- Script automatically handles API errors
- All original data is preserved in comparison file

## Support

For issues with:
- **Google Maps API**: Check API is enabled in Google Cloud Console
- **Missing addresses**: These will be marked as "No Address"
- **API limits**: Process in batches using the `limit` parameter