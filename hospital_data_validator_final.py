#!/usr/bin/env python3
"""
Hospital Data Cleaner & Address Validator
==========================================
This script cleans hospital data and validates addresses using Google Maps API.

Features:
- Cleans and standardizes hospital names
- Validates and corrects addresses using Google Maps Geocoding API
- Provides before/after comparison
- Generates detailed validation reports
- Shows verified, corrected, and invalid addresses

Author: Hospital Data Processing System
Date: 2025
"""

import pandas as pd
import re
import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, Tuple, List, Optional
from google_address_validator import GoogleMapsValidator

# Configuration
INPUT_FILE = 'Concierge Hospitals.xlsx'
OUTPUT_FILE = 'Hospital_Data_Validated.xlsx'
COMPARISON_FILE = 'Before_After_Comparison.xlsx'
VALIDATION_REPORT = 'Validation_Report.json'

# Google Maps API Configuration via environment
# GOOGLE_MAPS_API_KEY should be set in the environment for cross-system portability
GOOGLE_MAPS_RPS = float(os.environ.get('GOOGLE_MAPS_RPS', '50'))

# Logging
INVALID_STATE_LOG = 'invalid_states.log'

# US States
VALID_US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'AS', 'GU', 'MP'
}

# Map full state names to USPS codes (basic normalization)
STATE_NAME_TO_CODE = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO',
    'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH',
    'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT',
    'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    'DISTRICT OF COLUMBIA': 'DC'
}


class DataCleaner:
    @staticmethod
    def is_placeholder(value: str) -> bool:
        """Return True if the value is a known placeholder or junk."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return True
        s = str(value).strip().strip('"\'')
        if s == '':
            return True
        lower = s.lower()
        placeholders = {
            'unknown', 'address unknown', 'n/a', 'na', 'null', '-', '.', 'none'
        }
        if lower in placeholders:
            return True
        # Single-character values or a lone '1' treated as placeholders
        if len(s) == 1:
            return True
        if s == '1':
            return True
        # Repeated X/x pattern (e.g., 'xxxxx', 'XXX') considered placeholder
        letters_only = re.sub(r'[^A-Za-z]', '', s)
        if len(letters_only) >= 3 and set(letters_only.upper()) == {'X'}:
            return True
        return False

    @staticmethod
    def _all_tokens_placeholder(text: str) -> bool:
        """Return True if all comma/space separated tokens are placeholders."""
        tokens = [t.strip() for t in re.split(r'[\s,]+', str(text)) if t and t.strip()]
        if not tokens:
            return True
        return all(DataCleaner.is_placeholder(t) for t in tokens)

    @staticmethod
    def _remove_placeholder_tokens(text: str) -> str:
        """Remove placeholder-like tokens (e.g., Xxxxxx, Unknown, 1) from address while preserving delimiters."""
        parts = re.split(r'(,|\s+)', str(text))  # keep delimiters
        kept = []
        for part in parts:
            if part is None or part == '':
                continue
            # Delimiters preserved
            if re.fullmatch(r'(,|\s+)', part):
                kept.append(part)
                continue
            token = part.strip()
            if DataCleaner.is_placeholder(token):
                continue
            # Repeated X/x letters length>=2 considered placeholder token
            letters_only = re.sub(r'[^A-Za-z]', '', token)
            if len(letters_only) >= 2 and set(letters_only.upper()) == {'X'}:
                continue
            kept.append(part)
        result = ''.join(kept)
        # Normalize spaces/commas
        result = re.sub(r'\s+,\s*', ', ', result)
        result = re.sub(r'\s+', ' ', result).strip(' ,')
        return result

    """Handles data cleaning and standardization"""
    
    @staticmethod
    def to_camel_case(text: str) -> str:
        """Convert a phrase to lowerCamelCase (alphanumeric only, preserves leading numbers)."""
        if not text or pd.isna(text):
            return ''
        # Normalize whitespace and strip
        normalized = re.sub(r'\s+', ' ', str(text)).strip()
        # Remove apostrophes and periods to normalize possessives and abbreviations
        normalized = re.sub(r"[\.'’]", '', normalized)
        # Split on any non-alphanumeric boundary
        tokens = re.split(r'[^A-Za-z0-9]+', normalized)
        tokens = [t for t in tokens if t]
        if not tokens:
            return ''
        # If the first token is numeric, keep as-is; otherwise lowercase it
        first = tokens[0]
        if first.isdigit():
            camel = first
        else:
            camel = first.lower()
        # Capitalize subsequent tokens
        for token in tokens[1:]:
            camel += token[:1].upper() + token[1:].lower()
        return camel

    @staticmethod
    def to_title_phrase(text: str) -> str:
        """Title-case a phrase with minor word exceptions."""
        if not text or pd.isna(text):
            return ''
        text = re.sub(r'\s+', ' ', str(text)).strip()
        minor = {'of', 'the', 'and', 'for', 'at', 'in', 'on', 'a', 'an'}
        words = text.split(' ')
        result = []
        for i, w in enumerate(words):
            lw = w.lower()
            if i > 0 and lw in minor:
                result.append(lw)
            else:
                result.append(lw[:1].upper() + lw[1:])
        return ' '.join(result)

    @staticmethod
    def standardize_street_and_directions(text: str) -> str:
        """Expand common street suffixes and directional abbreviations."""
        if not text or pd.isna(text):
            return ''
        s = str(text).strip()
        # Expand with word boundaries; avoid changing existing full words like "North Hill"
        replacements = {
            r'(?<!\w)NE\.?(?!\w)': 'Northeast',
            r'(?<!\w)NW\.?((?!\w))': 'Northwest',
            r'(?<!\w)SE\.?(?!\w)': 'Southeast',
            r'(?<!\w)SW\.?(?!\w)': 'Southwest',
            r'(?<!\w)N\.?(?!\w)': 'North',
            r'(?<!\w)S\.?((?!\w))': 'South',
            r'(?<!\w)E\.?(?!\w)': 'East',
            r'(?<!\w)W\.?((?!\w))': 'West',
            r'\bST\b': 'Street',
            r'\bAVE\b': 'Avenue',
            r'\bBLVD\b': 'Boulevard',
            r'\bDR\b': 'Drive',
            r'\bRD\b': 'Road',
            r'\bLN\b': 'Lane',
            r'\bCT\b': 'Court',
            r'\bPKWY\b': 'Parkway',
            r'\bHWY\b': 'Highway'
        }
        for pattern, repl in replacements.items():
            s = re.sub(pattern, repl, s, flags=re.IGNORECASE)
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    @staticmethod
    def clean_hospital_name(name: str) -> str:
        """Clean and standardize hospital name to camelCase."""
        if DataCleaner.is_placeholder(name):
            return ''
        return DataCleaner.to_camel_case(name)
    
    @staticmethod
    def clean_address(address: str) -> str:
        """Clean and standardize address to a human-readable form (not camelCase).
        Example: "123 N. Main St" -> "123 North Main Street".
        """
        if DataCleaner.is_placeholder(address):
            return ''
        
        raw = str(address).strip()
        # If composed entirely of placeholder tokens like 'xxxxx, 1', drop it
        if DataCleaner._all_tokens_placeholder(raw):
            return ''
        # Remove placeholder-like tokens embedded in the address
        raw = DataCleaner._remove_placeholder_tokens(raw)
        if not raw:
            return ''
        std = DataCleaner.standardize_street_and_directions(raw)
        return DataCleaner.to_title_phrase(std)
    
    @staticmethod
    def clean_phone(phone: str) -> str:
        """Clean and format phone number as 10 digits only (no punctuation)."""
        if DataCleaner.is_placeholder(phone):
            return ''
        
        # Extract digits only
        digits = re.sub(r'\D', '', str(phone))
        
        # Remove leading 1 for US numbers
        if len(digits) == 11 and digits[0] == '1':
            digits = digits[1:]
        
        # Check for valid 10-digit number
        if len(digits) == 10 and not all(d == digits[0] for d in digits):
            return digits
        
        return ''
    
    @staticmethod
    def validate_state(state: str) -> Tuple[str, bool]:
        """Validate state code"""
        if DataCleaner.is_placeholder(state):
            return '', False
        
        s = str(state).strip()
        code = s.upper()
        if code in VALID_US_STATES:
            return code, True
        # Try mapping full names
        full = re.sub(r'\s+', ' ', s).strip().upper()
        if full in STATE_NAME_TO_CODE:
            return STATE_NAME_TO_CODE[full], True
        # Common obvious typos: remove periods and extra spaces, try again
        full2 = re.sub(r'[\.]', '', full)
        if full2 in STATE_NAME_TO_CODE:
            return STATE_NAME_TO_CODE[full2], True
        return code, False
    
    @staticmethod
    def validate_zip(zip_code: str) -> Tuple[str, bool]:
        """Validate ZIP code"""
        if DataCleaner.is_placeholder(zip_code):
            return '', False
        
        digits = re.sub(r'\D', '', str(zip_code))
        if len(digits) >= 9:
            base = digits[:5]
            plus4 = digits[5:9]
            return f"{base}-{plus4}", True
        if len(digits) >= 5:
            return digits[:5], True
        return '', False


    # Removed internal validator in favor of standalone google_address_validator.GoogleMapsValidator


class HospitalDataProcessor:
    """Main processor for hospital data"""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.validator = GoogleMapsValidator(api_key=None, requests_per_second=GOOGLE_MAPS_RPS)
        self.original_df = None
        self.cleaned_df = None
        self.comparison_data = []
    
    def process_file(self, input_file: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Process hospital data file"""
        print(f"\n📂 Reading {input_file}...")
        df = pd.read_excel(input_file)
        
        # Filter active records
        active_df = df[df['Active'] == 1].copy()
        print(f"   Found {len(df)} total records, {len(active_df)} active")
        
        if limit:
            active_df = active_df.head(limit)
            print(f"   Processing first {limit} records (for testing)")
        
        self.original_df = active_df.copy()
        self.cleaned_df = active_df.copy()
        # Track original index for comparison after deduplication
        self.cleaned_df['Original_Index'] = self.cleaned_df.index
        
        # Process data
        self._clean_data()
        self._validate_addresses()
        self._deduplicate()
        self._create_comparison()
        
        return self.cleaned_df
    
    def _clean_data(self):
        """Clean all data fields"""
        print("\n🧹 Cleaning data...")
        
        for idx, row in self.cleaned_df.iterrows():
            # Clean hospital name
            self.cleaned_df.loc[idx, 'Hospital_Name_Clean'] = self.cleaner.clean_hospital_name(
                row.get('HospitalName', '')
            )
            
            # Clean address
            address_one_raw = row.get('AddressOne', '')
            address_two_raw = row.get('AddressTwo', row.get('Address2', ''))
            address_std = self.cleaner.standardize_street_and_directions(address_one_raw)
            self.cleaned_df.loc[idx, 'Address_Std'] = address_std
            self.cleaned_df.loc[idx, 'Address_Clean'] = self.cleaner.clean_address(address_one_raw)
            # Address 2
            self.cleaned_df.loc[idx, 'Address2_Clean'] = self.cleaner.to_camel_case(address_two_raw)
            
            # Clean city
            city_raw = str(row.get('City', '')).strip()
            city_std = '' if city_raw.lower() in ['unknown', 'n/a'] else self.cleaner.to_title_phrase(city_raw)
            self.cleaned_df.loc[idx, 'City_Std'] = city_std
            self.cleaned_df.loc[idx, 'City_Clean'] = '' if not city_std else self.cleaner.to_camel_case(city_std)
            
            # Validate state
            state, state_valid = self.cleaner.validate_state(row.get('State', ''))
            self.cleaned_df.loc[idx, 'State_Clean'] = state
            self.cleaned_df.loc[idx, 'State_Valid'] = 'Y' if state_valid else 'N'
            if state and not state_valid:
                try:
                    with open(INVALID_STATE_LOG, 'a') as logf:
                        hosp = str(row.get('HospitalName', ''))
                        logf.write(f"Invalid state '{state}' for hospital '{hosp}' (row {idx})\n")
                except Exception:
                    pass
            
            # Validate ZIP
            zip_code, zip_valid = self.cleaner.validate_zip(row.get('ZIPCode', ''))
            self.cleaned_df.loc[idx, 'ZIP_Clean'] = zip_code
            self.cleaned_df.loc[idx, 'ZIP_Valid'] = 'Y' if zip_valid else 'N'
            
            # Clean phone
            self.cleaned_df.loc[idx, 'Phone_Clean'] = self.cleaner.clean_phone(
                row.get('Phone', '')
            )
            
            # Clean fax
            self.cleaned_df.loc[idx, 'Fax_Clean'] = self.cleaner.clean_phone(
                row.get('Facimile', '')
            )
        
        print("   ✓ Data cleaning complete")

    def _deduplicate(self):
        """Drop exact duplicate rows based on cleaned core columns."""
        print("\n🧬 Deduplicating records...")
        core_cols = [
            'Hospital_Name_Clean', 'Address_Clean', 'Address2_Clean', 'City_Clean',
            'State_Clean', 'ZIP_Clean', 'Phone_Clean'
        ]
        before = len(self.cleaned_df)
        # Fill NaN with empty strings for dedup comparison consistency
        self.cleaned_df[core_cols] = self.cleaned_df[core_cols].fillna('')
        self.cleaned_df = self.cleaned_df.drop_duplicates(subset=core_cols, keep='first').reset_index(drop=True)
        after = len(self.cleaned_df)
        print(f"   ✓ Removed {before - after} duplicates; {after} records remain")
    
    def _validate_addresses(self):
        """Validate addresses using Google Maps"""
        print("\n📍 Validating addresses...")
        
        # Add validation columns
        self.cleaned_df['Validation_Status'] = ''
        self.cleaned_df['Verified_Address'] = ''
        self.cleaned_df['Address_Confidence'] = ''
        self.cleaned_df['Was_Corrected'] = ''
        self.cleaned_df['Latitude'] = ''
        self.cleaned_df['Longitude'] = ''
        
        total = len(self.cleaned_df)
        
        for idx, row in self.cleaned_df.iterrows():
            # Progress indicator
            if (idx + 1) % 10 == 0:
                print(f"   Processing {idx + 1}/{total}...")
            
            # Get address components
            # Use standardized, human-readable address/city for validation
            address = row.get('Address_Std', '') or row.get('Address_Clean', '')
            city = row.get('City_Std', '') or row.get('City_Clean', '')
            state = row.get('State_Clean', '')
            zip_code = row.get('ZIP_Clean', '')
            
            # Skip if no address
            if not address:
                self.cleaned_df.loc[idx, 'Validation_Status'] = 'No Address'
                continue
            
            # Validate with Google Maps
            result = self.validator.validate_address(address, city, state, zip_code)
            
            # Store results
            self.cleaned_df.loc[idx, 'Validation_Status'] = result['status']
            self.cleaned_df.loc[idx, 'Verified_Address'] = result['formatted']
            self.cleaned_df.loc[idx, 'Address_Confidence'] = result['confidence']
            self.cleaned_df.loc[idx, 'Was_Corrected'] = 'Yes' if result['was_corrected'] else 'No'
            
            if result['latitude']:
                self.cleaned_df.loc[idx, 'Latitude'] = str(result['latitude'])
                self.cleaned_df.loc[idx, 'Longitude'] = str(result['longitude'])
        
        stats = self.validator.get_statistics()
        print(f"\n   ✓ Validation complete:")
        print(f"     • Verified: {stats['verified']}")
        print(f"     • Corrected: {stats['corrected']}")
        print(f"     • Invalid: {stats['invalid']}")
        print(f"     • Errors: {stats['errors']}")
    
    def _create_comparison(self):
        """Create before/after comparison"""
        print("\n📊 Creating comparison data...")
        
        for idx in self.cleaned_df.index:
            clean = self.cleaned_df.loc[idx]
            idx_orig = clean.get('Original_Index', idx)
            # Fallback-safe access to original row
            orig = self.original_df.loc[idx_orig] if idx_orig in self.original_df.index else {}
            
            self.comparison_data.append({
                'Hospital_Original': orig.get('HospitalName', ''),
                'Hospital_Cleaned': clean.get('Hospital_Name_Clean', ''),
                'Address_Original': orig.get('AddressOne', ''),
                'Address_Cleaned': clean.get('Address_Clean', ''),
                'Address_Verified': clean.get('Verified_Address', ''),
                'Validation_Status': clean.get('Validation_Status', ''),
                'Confidence': clean.get('Address_Confidence', ''),
                'Was_Corrected': clean.get('Was_Corrected', ''),
                'City_Original': orig.get('City', ''),
                'City_Cleaned': clean.get('City_Clean', ''),
                'State_Original': orig.get('State', ''),
                'State_Cleaned': clean.get('State_Clean', ''),
                'ZIP_Original': orig.get('ZIPCode', ''),
                'ZIP_Cleaned': clean.get('ZIP_Clean', ''),
                'Phone_Original': orig.get('Phone', ''),
                'Phone_Cleaned': clean.get('Phone_Clean', ''),
                'Latitude': clean.get('Latitude', ''),
                'Longitude': clean.get('Longitude', '')
            })
        
        print("   ✓ Comparison data created")
    
    def save_results(self):
        """Save all results to files"""
        print("\n💾 Saving results...")
        
        # Save main output
        output_cols = [
            'ClinicKey', 'HospitalKey',
            'Hospital_Name_Clean', 'Address_Clean', 'Address2_Clean', 'City_Clean',
            'State_Clean', 'ZIP_Clean', 'Phone_Clean', 'Fax_Clean',
            'State_Valid', 'ZIP_Valid',
            'Validation_Status', 'Verified_Address', 'Address_Confidence',
            'Was_Corrected', 'Latitude', 'Longitude'
        ]
        
        output_df = self.cleaned_df[output_cols].copy()
        output_df.columns = output_df.columns.str.replace('_Clean', '').str.replace('_', ' ')
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            output_df.to_excel(writer, sheet_name='Validated Data', index=False)
        print(f"   ✓ Saved to {OUTPUT_FILE}")
        
        # Save comparison
        comparison_df = pd.DataFrame(self.comparison_data)
        with pd.ExcelWriter(COMPARISON_FILE, engine='openpyxl') as writer:
            comparison_df.to_excel(writer, sheet_name='Before After Comparison', index=False)
        print(f"   ✓ Saved to {COMPARISON_FILE}")
        
        # Save validation report
        stats = self.validator.get_statistics()
        report = {
            'timestamp': datetime.now().isoformat(),
            'statistics': {
                'total_processed': stats['total'],
                'verified': stats['verified'],
                'verified_percentage': round((stats['verified'] / stats['total'] * 100) if stats['total'] > 0 else 0, 2),
                'corrected': stats['corrected'],
                'invalid': stats['invalid'],
                'errors': stats['errors']
            },
            'examples': {
                'verified': [],
                'corrected': [],
                'invalid': []
            }
        }
        
        # Add examples
        for item in self.comparison_data[:50]:
            if item['Validation_Status'].startswith('Verified'):
                if len(report['examples']['verified']) < 3:
                    report['examples']['verified'].append({
                        'original': item['Address_Original'],
                        'verified': item['Address_Verified'],
                        'confidence': item['Confidence']
                    })
            elif item['Was_Corrected'] == 'Yes':
                if len(report['examples']['corrected']) < 3:
                    report['examples']['corrected'].append({
                        'original': item['Address_Original'],
                        'corrected': item['Address_Verified']
                    })
            elif item['Validation_Status'] == 'Not Found':
                if len(report['examples']['invalid']) < 3:
                    report['examples']['invalid'].append({
                        'address': item['Address_Original']
                    })
        
        with open(VALIDATION_REPORT, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"   ✓ Saved to {VALIDATION_REPORT}")


def main():
    """Main function"""
    print("="*80)
    print("HOSPITAL DATA CLEANER & ADDRESS VALIDATOR")
    print("="*80)
    
    # Check for input file
    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ Error: Input file '{INPUT_FILE}' not found!")
        return 1
    
    # Process data
    processor = HospitalDataProcessor()
    
    # Process ALL records
    processor.process_file(INPUT_FILE, limit=None)  # Process all 1,436 records
    
    # Save results
    processor.save_results()
    
    # Print summary
    print("\n" + "="*80)
    print("✅ PROCESSING COMPLETE")
    print("="*80)
    print(f"\nOutput files:")
    print(f"  • {OUTPUT_FILE} - Cleaned and validated data")
    print(f"  • {COMPARISON_FILE} - Before/after comparison")
    print(f"  • {VALIDATION_REPORT} - Validation statistics")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())