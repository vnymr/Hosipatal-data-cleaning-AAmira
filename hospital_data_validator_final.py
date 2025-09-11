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
import requests
from difflib import SequenceMatcher

# Try to import googlemaps
try:
    import googlemaps
    GOOGLEMAPS_AVAILABLE = True
except ImportError:
    GOOGLEMAPS_AVAILABLE = False

# Configuration
INPUT_FILE = 'Concierge Hospitals.xlsx'
OUTPUT_FILE = 'Hospital_Data_Validated.xlsx'
COMPARISON_FILE = 'Before_After_Comparison.xlsx'
VALIDATION_REPORT = 'Validation_Report.json'

# Google Maps API Configuration
GOOGLE_MAPS_API_KEY = 'AIzaSyDKgAcoQOKHsbg6KEjRX8UVXUCe7BFaLAc'
# Rate limiting
GOOGLE_MAPS_RATE_LIMIT = 0.02  # 50 requests per second
NOMINATIM_RATE_LIMIT = 1.1  # 1 request per second

# US States
VALID_US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'AS', 'GU', 'MP'
}


class DataCleaner:
    """Handles data cleaning and standardization"""
    
    @staticmethod
    def clean_hospital_name(name: str) -> str:
        """Clean and standardize hospital name"""
        if not name or pd.isna(name) or str(name).upper() == 'NULL':
            return ''
        
        # Remove extra spaces
        name = re.sub(r'\s+', ' ', str(name)).strip()
        
        # Title case with exceptions
        words = name.split()
        result = []
        for i, word in enumerate(words):
            if word.lower() in ['of', 'the', 'and', 'for', 'at'] and i > 0:
                result.append(word.lower())
            else:
                result.append(word[0].upper() + word[1:].lower() if len(word) > 1 else word.upper())
        
        return ' '.join(result)
    
    @staticmethod
    def clean_address(address: str) -> str:
        """Clean and standardize address"""
        if not address or pd.isna(address):
            return ''
        
        address = str(address).strip()
        
        # Check for invalid addresses
        if address.lower() in ['unknown', 'n/a', 'na', 'null']:
            return ''
        
        # Expand common abbreviations
        replacements = {
            r'\bST\b': 'Street',
            r'\bAVE\b': 'Avenue',
            r'\bBLVD\b': 'Boulevard',
            r'\bDR\b': 'Drive',
            r'\bRD\b': 'Road',
            r'\bLN\b': 'Lane',
            r'\bCT\b': 'Court',
            r'\bPKWY\b': 'Parkway',
            r'\bHWY\b': 'Highway',
            r'\bN\b': 'North',
            r'\bS\b': 'South',
            r'\bE\b': 'East',
            r'\bW\b': 'West',
            r'\bNE\b': 'Northeast',
            r'\bNW\b': 'Northwest',
            r'\bSE\b': 'Southeast',
            r'\bSW\b': 'Southwest'
        }
        
        for pattern, replacement in replacements.items():
            address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
        
        return re.sub(r'\s+', ' ', address).strip()
    
    @staticmethod
    def clean_phone(phone: str) -> str:
        """Clean and format phone number"""
        if not phone or pd.isna(phone) or str(phone).upper() == 'NULL':
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
        if not state or pd.isna(state):
            return '', False
        
        state = str(state).strip().upper()
        return state, state in VALID_US_STATES
    
    @staticmethod
    def validate_zip(zip_code: str) -> Tuple[str, bool]:
        """Validate ZIP code"""
        if not zip_code or pd.isna(zip_code):
            return '', False
        
        digits = re.sub(r'\D', '', str(zip_code))
        
        if len(digits) >= 5:
            return digits[:5], True
        return digits, False


class GoogleMapsValidator:
    """Handles Google Maps address validation"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.gmaps = None
        self.stats = {
            'total': 0,
            'verified': 0,
            'corrected': 0,
            'invalid': 0,
            'errors': 0
        }
        
        if GOOGLEMAPS_AVAILABLE:
            try:
                self.gmaps = googlemaps.Client(key=api_key)
                print("✓ Google Maps client initialized")
            except Exception as e:
                print(f"✗ Failed to initialize Google Maps: {e}")
    
    def validate_address(self, address: str, city: str, state: str, zip_code: str) -> Dict:
        """Validate address using Google Maps Places API (find_place + details)"""
        self.stats['total'] += 1
        
        if not self.gmaps:
            self.stats['errors'] += 1
            return self._error_result("Google Maps not available")
        
        # Build full address
        full_address = f"{address}, {city}, {state} {zip_code}, USA"
        
        try:
            # Use Places API to find a place by text query
            find_resp = self.gmaps.find_place(
                full_address,
                'textquery',
                fields=['place_id', 'formatted_address', 'geometry']
            )
            candidates = (find_resp or {}).get('candidates', [])
            if not candidates:
                self.stats['invalid'] += 1
                return self._invalid_result(full_address)
            
            candidate = candidates[0]
            place_id = candidate.get('place_id')
            details_result = {}
            if place_id:
                # Fetch detailed place info to get address components
                details_resp = self.gmaps.place(
                    place_id,
                    fields=['formatted_address', 'geometry', 'address_component']
                )
                details_result = (details_resp or {}).get('result', {})
            
            # Prefer details result if available, else fall back to candidate
            base_result = details_result if details_result else candidate
            validated = self._parse_result(base_result, full_address)
            
            # Update statistics
            if validated['is_valid']:
                self.stats['verified'] += 1
                if validated['was_corrected']:
                    self.stats['corrected'] += 1
            else:
                self.stats['invalid'] += 1
            
            # Rate limiting
            time.sleep(GOOGLE_MAPS_RATE_LIMIT)
            
            return validated
            
        except Exception as e:
            self.stats['errors'] += 1
            error_msg = str(e)
            
            # Check for common API errors
            if 'REQUEST_DENIED' in error_msg or 'PERMISSION_DENIED' in error_msg:
                print("\n⚠ Google Maps API Error: Places API not enabled or key restricted")
                print("  Please enable Places API and ensure key restrictions allow server-side calls.")
                self.gmaps = None  # Disable for rest of session
            
            return self._error_result(error_msg)
    
    def _parse_result(self, result: Dict, original: str) -> Dict:
        """Parse Google Maps result (compatible with Places Details or Geocoding)"""
        formatted = result.get('formatted_address', '')
        geometry = result.get('geometry', {})
        location = geometry.get('location', {'lat': None, 'lng': None})
        # Places Details typically does not include location_type; default to ROOFTOP
        location_type = geometry.get('location_type', 'ROOFTOP')
        components = result.get('address_components', [])
        
        # Parse components
        parsed = {
            'street_number': '',
            'street': '',
            'city': '',
            'state': '',
            'zip': ''
        }
        
        for comp in components:
            types = comp.get('types', [])
            if 'street_number' in types:
                parsed['street_number'] = comp['long_name']
            elif 'route' in types:
                parsed['street'] = comp['long_name']
            elif 'locality' in types:
                parsed['city'] = comp['long_name']
            elif 'administrative_area_level_1' in types:
                parsed['state'] = comp['short_name']
            elif 'postal_code' in types:
                parsed['zip'] = comp['long_name']
        
        # Determine validation quality
        if location_type == 'ROOFTOP':
            confidence = 'HIGH'
            status = 'Verified - Exact Match'
            is_valid = True
        elif location_type == 'RANGE_INTERPOLATED':
            confidence = 'HIGH'
            status = 'Verified - Street Level'
            is_valid = True
        elif location_type == 'GEOMETRIC_CENTER':
            confidence = 'MEDIUM'
            status = 'Verified - Area Level'
            is_valid = True
        else:
            confidence = 'LOW'
            status = 'Approximate Only'
            is_valid = False
        
        # Check if address was corrected
        orig_clean = re.sub(r'[^a-zA-Z0-9]', '', original.lower())
        formatted_clean = re.sub(r'[^a-zA-Z0-9]', '', formatted.lower())
        was_corrected = SequenceMatcher(None, orig_clean, formatted_clean).ratio() < 0.9
        
        return {
            'original': original,
            'formatted': formatted,
            'is_valid': is_valid,
            'status': status,
            'confidence': confidence,
            'was_corrected': was_corrected,
            'components': parsed,
            'latitude': location['lat'],
            'longitude': location['lng'],
            'location_type': location_type
        }
    
    def _invalid_result(self, address: str) -> Dict:
        """Create result for invalid address"""
        return {
            'original': address,
            'formatted': '',
            'is_valid': False,
            'status': 'Not Found',
            'confidence': 'NONE',
            'was_corrected': False,
            'components': {},
            'latitude': None,
            'longitude': None,
            'location_type': 'NOT_FOUND'
        }
    
    def _error_result(self, error: str) -> Dict:
        """Create result for error"""
        return {
            'original': '',
            'formatted': '',
            'is_valid': False,
            'status': f'Error: {error}',
            'confidence': 'ERROR',
            'was_corrected': False,
            'components': {},
            'latitude': None,
            'longitude': None,
            'location_type': 'ERROR'
        }
    
    def get_statistics(self) -> Dict:
        """Get validation statistics"""
        return self.stats.copy()


class HospitalDataProcessor:
    """Main processor for hospital data"""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.validator = GoogleMapsValidator(GOOGLE_MAPS_API_KEY)
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
        
        # Process data
        self._clean_data()
        self._validate_addresses()
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
            self.cleaned_df.loc[idx, 'Address_Clean'] = self.cleaner.clean_address(
                row.get('AddressOne', '')
            )
            
            # Clean city
            city = str(row.get('City', '')).strip()
            self.cleaned_df.loc[idx, 'City_Clean'] = city if city.lower() not in ['unknown', 'n/a'] else ''
            
            # Validate state
            state, state_valid = self.cleaner.validate_state(row.get('State', ''))
            self.cleaned_df.loc[idx, 'State_Clean'] = state
            self.cleaned_df.loc[idx, 'State_Valid'] = 'Y' if state_valid else 'N'
            
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
            address = row.get('Address_Clean', '')
            city = row.get('City_Clean', '')
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
            orig = self.original_df.loc[idx]
            clean = self.cleaned_df.loc[idx]
            
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
            'Hospital_Name_Clean', 'Address_Clean', 'City_Clean',
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
    
    # Check Google Maps availability
    if not GOOGLEMAPS_AVAILABLE:
        print("\n⚠ Warning: googlemaps library not installed")
        print("  Install with: pip install googlemaps")
        print("  Continuing without address validation...")
    
    # Process data
    processor = HospitalDataProcessor()
    
    # Process all records (set limit=10 for testing)
    processor.process_file(INPUT_FILE, limit=None)  # Set limit=10 for testing
    
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