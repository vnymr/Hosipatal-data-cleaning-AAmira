"""
Hospital Data Cleanup Script
Normalizes and standardizes hospital records according to specified criteria
"""

import pandas as pd
import re
import os
from datetime import datetime
import json
import requests
import time
from urllib.parse import quote

class HospitalDataCleaner:
    def __init__(self):
        # US State abbreviations for validation
        self.valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC', 'PR', 'VI', 'AS', 'GU', 'MP'  # Including territories
        }
        
        # Direction abbreviations mapping
        self.direction_mapping = {
            'N.': 'North', 'N': 'North',
            'S.': 'South', 'S': 'South',
            'E.': 'East', 'E': 'East',
            'W.': 'West', 'W': 'West',
            'NE.': 'Northeast', 'NE': 'Northeast',
            'NW.': 'Northwest', 'NW': 'Northwest',
            'SE.': 'Southeast', 'SE': 'Southeast',
            'SW.': 'Southwest', 'SW': 'Southwest'
        }
        
        # Street type abbreviations mapping
        self.street_type_mapping = {
            'St.': 'Street', 'St': 'Street',
            'Ave.': 'Avenue', 'Ave': 'Avenue',
            'Blvd.': 'Boulevard', 'Blvd': 'Boulevard',
            'Dr.': 'Drive', 'Dr': 'Drive',
            'Rd.': 'Road', 'Rd': 'Road',
            'Ln.': 'Lane', 'Ln': 'Lane',
            'Ct.': 'Court', 'Ct': 'Court',
            'Cir.': 'Circle', 'Cir': 'Circle',
            'Pl.': 'Place', 'Pl': 'Place',
            'Trl.': 'Trail', 'Trl': 'Trail',
            'Pkwy.': 'Parkway', 'Pkwy': 'Parkway',
            'Hwy.': 'Highway', 'Hwy': 'Highway',
            'Ter.': 'Terrace', 'Ter': 'Terrace',
            'Way': 'Way',
            'Sq.': 'Square', 'Sq': 'Square'
        }
        
        # Exception names that should not have directions expanded
        # These are proper names where direction words are part of the name
        self.direction_exceptions = [
            'North Hill Road',
            'North Shore Drive',
            'South Park Avenue',
            'East River Road',
            'West End Boulevard',
            'North Main Street',
            'South Main Street'
        ]
        
        self.cleanup_log = []
        self.validation_errors = []
    
    def to_camel_case(self, text):
        """Convert text to camelCase format"""
        if pd.isna(text) or text == 'NULL' or text == '':
            return ''
        
        text = str(text).strip()
        # Split by spaces and special characters
        words = re.split(r'[\s\-_]+', text)
        
        if not words:
            return ''
        
        # First word lowercase, rest capitalize first letter
        result = words[0].lower()
        for word in words[1:]:
            if word:
                result += word[0].upper() + word[1:].lower()
        
        return result
    
    def clean_null_values(self, value):
        """Clean NULL values and return empty string"""
        if pd.isna(value):
            return ''
        str_value = str(value).strip()
        if str_value.upper() == 'NULL':
            return ''
        return str(value)
    
    def trim_excess_spaces(self, text):
        """Remove excess spaces and trim"""
        if pd.isna(text) or text == '':
            return ''
        
        text = self.clean_null_values(text)
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def standardize_address(self, address):
        """Standardize address with direction and street type expansions"""
        if pd.isna(address) or address == '':
            return ''
        
        address = self.clean_null_values(address)
        address = self.trim_excess_spaces(address)
        
        # Check if this address is in exceptions list
        is_exception = any(exception in address for exception in self.direction_exceptions)
        
        if not is_exception:
            # Split address into tokens
            tokens = address.split()
            new_tokens = []
            
            for i, token in enumerate(tokens):
                # Check if token is a direction abbreviation
                if token in self.direction_mapping:
                    # Check context to ensure it's actually a direction
                    # If it's at the beginning or before a number, likely a direction
                    if i == 0 or (i > 0 and tokens[i-1].isdigit()):
                        new_tokens.append(self.direction_mapping[token])
                    else:
                        new_tokens.append(token)
                # Check if token is a street type abbreviation
                elif token in self.street_type_mapping:
                    # Usually street types are at the end or before unit numbers
                    if i == len(tokens) - 1 or (i < len(tokens) - 1 and 
                                               (tokens[i+1].startswith('#') or 
                                                tokens[i+1].lower() in ['suite', 'unit', 'apt'])):
                        new_tokens.append(self.street_type_mapping[token])
                    else:
                        new_tokens.append(token)
                else:
                    new_tokens.append(token)
            
            address = ' '.join(new_tokens)
        
        return address
    
    def clean_address_with_pound(self, address):
        """Remove pound signs from addresses"""
        if pd.isna(address) or address == '':
            return ''
        
        address = self.clean_null_values(address)
        # Replace # with space and clean up
        address = address.replace('#', ' ')
        address = self.trim_excess_spaces(address)
        return address
    
    def validate_state(self, state):
        """Validate state abbreviation and return uppercase"""
        if pd.isna(state) or state == '':
            return '', False, 'Empty state value'
        
        state = self.clean_null_values(state)
        if state == '':
            return '', False, 'Empty state value'
        state = state.strip().upper()
        
        if state in self.valid_states:
            return state, True, ''
        else:
            self.validation_errors.append(f"Invalid state abbreviation: {state}")
            return state, False, f'Invalid state: {state}'
    
    def standardize_zip(self, zip_code):
        """Standardize ZIP code to 5 digits"""
        if pd.isna(zip_code) or zip_code == '':
            return '', False, 'Empty ZIP code'
        
        original_zip = zip_code  # Keep original for error messages
        zip_code = self.clean_null_values(zip_code)
        if zip_code == '':
            return '', False, 'Empty ZIP code'
        zip_code = str(zip_code).strip()
        
        # Remove any non-digit characters
        zip_digits = re.sub(r'\D', '', zip_code)
        
        if len(zip_digits) == 0:
            return '', False, f'No digits in ZIP: {original_zip}'
        elif len(zip_digits) < 5:
            return zip_digits, False, f'ZIP too short: {zip_code}'
        elif len(zip_digits) >= 5:
            # Take first 5 digits (handles ZIP+4)
            return zip_digits[:5], True, ''
        
        return zip_code, False, f'Invalid ZIP format: {zip_code}'
    
    def format_phone(self, phone):
        """Return digits-only 10-digit phone (strip non-digits, drop leading 1)"""
        if pd.isna(phone) or phone == '':
            return '', False, 'Empty phone number'
        
        phone = self.clean_null_values(phone)
        # Extract digits only
        digits = re.sub(r'\D', '', str(phone))
        
        # Drop leading country code 1 for US numbers
        if len(digits) == 11 and digits[0] == '1':
            digits = digits[1:]
        
        if len(digits) == 10:
            return digits, True, ''
        elif len(digits) == 0:
            return '', False, 'No phone number'
        else:
            return digits, False, f'Invalid phone format: {phone} ({len(digits)} digits)'
    
    def expand_abbreviations(self, text):
        """Expand common medical and geographic abbreviations"""
        # Common medical abbreviations
        medical_abbrevs = {
            'mc': 'medical center',
            'med': 'medical',
            'reg': 'regional',
            'hosp': 'hospital',
            'hlth': 'health',
            'ctr': 'center',
            'mem': 'memorial',
            'comm': 'community',
            'gen': 'general',
            'univ': 'university',
            'rehab': 'rehabilitation',
            'psych': 'psychiatric',
            'behav': 'behavioral',
            'surg': 'surgical',
            'ortho': 'orthopedic',
            'cardio': 'cardiovascular',
            'peds': 'pediatric',
            'obgyn': 'obstetrics gynecology',
            'er': 'emergency room',
            'icu': 'intensive care unit'
        }
        
        # Geographic and direction abbreviations
        geo_abbrevs = {
            'n': 'north',
            's': 'south', 
            'e': 'east',
            'w': 'west',
            'ne': 'northeast',
            'nw': 'northwest',
            'se': 'southeast',
            'sw': 'southwest',
            'fl': 'florida',
            'tx': 'texas',
            'ca': 'california',
            'ny': 'new york',
            'pa': 'pennsylvania',
            'il': 'illinois',
            'oh': 'ohio',
            'ga': 'georgia',
            'nc': 'north carolina',
            'mi': 'michigan'
        }
        
        text_lower = text.lower()
        words = text_lower.split()
        expanded_words = []
        
        for word in words:
            # Remove punctuation for matching
            clean_word = word.strip('.,')
            
            # Check medical abbreviations first (usually longer)
            if clean_word in medical_abbrevs:
                expanded_words.append(medical_abbrevs[clean_word])
            # Then check geographic
            elif clean_word in geo_abbrevs:
                expanded_words.append(geo_abbrevs[clean_word])
            else:
                expanded_words.append(word)
        
        return ' '.join(expanded_words)
    
    def normalize_name_for_comparison(self, name):
        """Normalize hospital name for comparison by expanding abbreviations and cleaning"""
        # Expand abbreviations first
        name = self.expand_abbreviations(name)
        
        # Remove extra spaces and punctuation
        name = re.sub(r'[^\w\s]', ' ', name.lower())
        name = ' '.join(name.split())
        
        return name
    
    def calculate_name_similarity(self, name1, name2):
        """Calculate similarity score between two hospital names"""
        # Normalize both names
        norm1 = self.normalize_name_for_comparison(name1)
        norm2 = self.normalize_name_for_comparison(name2)
        
        # If one is a substring of the other (after normalization), they're likely the same
        if norm1 in norm2 or norm2 in norm1:
            return 1.0
        
        # Tokenize and compare
        tokens1 = set(norm1.split())
        tokens2 = set(norm2.split())
        
        # Calculate Jaccard similarity
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        if not union:
            return 0.0
            
        similarity = len(intersection) / len(union)
        
        # Boost score if key identifying words match
        key_words = ['ocala', 'orlando', 'park', 'memorial', 'regional', 'baptist', 
                    'methodist', 'catholic', 'jewish', 'presbyterian', 'lutheran',
                    'adventist', 'childrens', 'womens', 'veterans', 'university']
        
        for word in key_words:
            if word in norm1 and word in norm2:
                similarity = min(1.0, similarity + 0.2)
        
        return similarity
    
    def standardize_hospital_names(self, df):
        """Standardize hospital names by finding and using the most complete version"""
        print("Standardizing hospital names...")
        
        # Create a copy to work with
        df_copy = df.copy()
        
        # Group similar names together
        name_groups = {}
        processed_indices = set()
        
        # Sort by name length (longest first) to prefer more complete names
        sorted_data = df_copy.sort_values('HospitalName', key=lambda x: x.str.len(), ascending=False)
        
        for idx, row in sorted_data.iterrows():
            if idx in processed_indices:
                continue
                
            name = str(row.get('HospitalName', '')).strip()
            if not name or name == 'NULL' or name == '':
                continue
            
            # Start a new group with this name
            current_group = [idx]
            processed_indices.add(idx)
            
            # Find all similar names
            for idx2, row2 in sorted_data.iterrows():
                if idx2 in processed_indices:
                    continue
                    
                name2 = str(row2.get('HospitalName', '')).strip()
                if not name2 or name2 == 'NULL' or name2 == '':
                    continue
                
                # Calculate similarity
                similarity = self.calculate_name_similarity(name, name2)
                
                # If similarity is high enough (threshold of 0.6), group them together
                if similarity >= 0.6:
                    current_group.append(idx2)
                    processed_indices.add(idx2)
            
            # Use the longest name as the standard (first in sorted order)
            if current_group:
                name_groups[name] = current_group
        
        # Apply the standardized names
        for standard_name, indices in name_groups.items():
            for idx in indices:
                df_copy.loc[idx, 'HospitalName'] = standard_name
        
        print(f"Standardized {len(name_groups)} unique hospital names from {len(df_copy)} records")
        
        # Print some examples of what was standardized
        examples_shown = 0
        for standard_name, indices in name_groups.items():
            if len(indices) > 1 and examples_shown < 5:
                original_names = set()
                for idx in indices[:3]:  # Show first 3 variations
                    original = df.loc[idx, 'HospitalName']
                    if original != standard_name:
                        original_names.add(original)
                if original_names:
                    print(f"  Standardized: {', '.join(original_names)} → {standard_name}")
                    examples_shown += 1
        
        return df_copy
    
    def verify_address_with_nominatim(self, address, city, state, zip_code):
        """Verify address using Nominatim (OpenStreetMap) geocoding service"""
        try:
            # Construct full address for query
            full_address = f"{address}, {city}, {state} {zip_code}, USA"
            
            # Nominatim API endpoint (free, no key required)
            url = "https://nominatim.openstreetmap.org/search"
            
            # Parameters for the API call
            params = {
                'q': full_address,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1,
                'countrycodes': 'us'  # Limit to US addresses
            }
            
            # Headers (Nominatim requires user agent)
            headers = {
                'User-Agent': 'HospitalDataCleaner/1.0 (data standardization project)'
            }
            
            # Make the request
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            # Rate limiting - Nominatim requires max 1 request per second
            time.sleep(1.1)  # Sleep for 1.1 seconds to be safe
            
            if response.status_code == 200:
                data = response.json()
                
                if data and len(data) > 0:
                    result = data[0]
                    
                    # Extract verified address components
                    address_parts = result.get('address', {})
                    
                    # Build verified address
                    verified_parts = []
                    
                    # Get house number and street
                    house_number = address_parts.get('house_number', '')
                    road = address_parts.get('road', '')
                    if house_number and road:
                        verified_parts.append(f"{house_number} {road}")
                    elif road:
                        verified_parts.append(road)
                    
                    # Get city (try multiple fields)
                    verified_city = (address_parts.get('city') or 
                                   address_parts.get('town') or 
                                   address_parts.get('village') or 
                                   address_parts.get('hamlet', ''))
                    
                    # Get state and ZIP
                    verified_state = address_parts.get('state', '')
                    verified_zip = address_parts.get('postcode', '')
                    
                    # Build full verified address
                    if verified_parts:
                        verified_address = verified_parts[0]
                        if verified_city:
                            verified_address += f", {verified_city}"
                        if verified_state:
                            verified_address += f", {verified_state}"
                        if verified_zip:
                            verified_address += f" {verified_zip}"
                    else:
                        verified_address = result.get('display_name', '')
                    
                    # Get coordinates
                    lat = result.get('lat', '')
                    lon = result.get('lon', '')
                    
                    # Confidence score based on result type
                    place_rank = int(result.get('place_rank', 30))
                    if place_rank <= 20:  # Building level
                        confidence = 'High'
                    elif place_rank <= 25:  # Street level
                        confidence = 'Medium'
                    else:
                        confidence = 'Low'
                    
                    return {
                        'verified': True,
                        'verified_address': verified_address,
                        'latitude': lat,
                        'longitude': lon,
                        'confidence': confidence,
                        'original_query': full_address
                    }
                else:
                    return {
                        'verified': False,
                        'verified_address': '',
                        'latitude': '',
                        'longitude': '',
                        'confidence': 'Not Found',
                        'original_query': full_address
                    }
            else:
                return {
                    'verified': False,
                    'verified_address': '',
                    'latitude': '',
                    'longitude': '',
                    'confidence': 'API Error',
                    'original_query': full_address
                }
                
        except Exception as e:
            print(f"Error verifying address: {str(e)}")
            return {
                'verified': False,
                'verified_address': '',
                'latitude': '',
                'longitude': '',
                'confidence': 'Error',
                'original_query': f"{address}, {city}, {state} {zip_code}"
            }
    
    def clean_hospital_data(self, df, enable_geocoding=False):
        """Main cleaning function for hospital data
        
        Args:
            df: DataFrame with hospital data
            enable_geocoding: If True, verify addresses with Nominatim (slow - 1 sec per address)
        """
        print("Starting hospital data cleanup...")
        print(f"Processing {len(df)} records...")
        
        # First standardize hospital names to use the most complete version
        df = self.standardize_hospital_names(df)
        
        # Create new columns for cleaned data
        cleaned_df = df.copy()
        
        # Initialize validation columns
        cleaned_df['StateValid'] = ''
        cleaned_df['ZipValid'] = ''
        cleaned_df['PhoneValid'] = ''
        cleaned_df['FaxValid'] = ''
        cleaned_df['ValidationNotes'] = ''
        
        # Initialize geocoding columns if enabled
        if enable_geocoding:
            cleaned_df['VerifiedAddress'] = ''
            cleaned_df['AddressConfidence'] = ''
            cleaned_df['Latitude'] = ''
            cleaned_df['Longitude'] = ''
            print(f"Address verification enabled. This will take approximately {len(df) * 1.1 / 60:.1f} minutes...")
        
        # Process each row
        for idx, row in df.iterrows():
            notes = []
            
            # Clean Hospital Name (camelCase)
            original_name = row.get('HospitalName', '')
            cleaned_df.loc[idx, 'CleanedHospitalName'] = self.to_camel_case(original_name)
            if original_name != cleaned_df.loc[idx, 'CleanedHospitalName']:
                notes.append(f"Name changed from '{original_name}'")
            
            # Clean and standardize AddressOne
            original_addr1 = row.get('AddressOne', '')
            standardized_addr1 = self.standardize_address(original_addr1)
            cleaned_df.loc[idx, 'CleanedAddressOne'] = self.to_camel_case(standardized_addr1)
            if original_addr1 != cleaned_df.loc[idx, 'CleanedAddressOne']:
                notes.append(f"Address1 standardized")
            
            # Clean AddressTwo (remove # and apply camelCase)
            original_addr2 = row.get('AddressTwo', '')
            clean_addr2 = self.clean_address_with_pound(original_addr2)
            cleaned_df.loc[idx, 'CleanedAddressTwo'] = self.to_camel_case(clean_addr2)
            
            # Clean City (camelCase)
            original_city = row.get('City', '')
            cleaned_df.loc[idx, 'CleanedCity'] = self.to_camel_case(original_city)
            
            # Validate and uppercase State
            original_state = row.get('State', '')
            clean_state, state_valid, state_note = self.validate_state(original_state)
            cleaned_df.loc[idx, 'CleanedState'] = clean_state
            cleaned_df.loc[idx, 'StateValid'] = 'Y' if state_valid else 'N'
            if not state_valid:
                notes.append(state_note)
            
            # Standardize ZIP code
            original_zip = row.get('ZIPCode', '')
            clean_zip, zip_valid, zip_note = self.standardize_zip(original_zip)
            cleaned_df.loc[idx, 'CleanedZIP'] = clean_zip
            cleaned_df.loc[idx, 'ZipValid'] = 'Y' if zip_valid else 'N'
            if not zip_valid:
                notes.append(zip_note)
            
            # Format Phone
            original_phone = row.get('Phone', '')
            clean_phone, phone_valid, phone_note = self.format_phone(original_phone)
            cleaned_df.loc[idx, 'CleanedPhone'] = clean_phone
            cleaned_df.loc[idx, 'PhoneValid'] = 'Y' if phone_valid else 'N'
            if not phone_valid and phone_note:
                notes.append(phone_note)
            
            # Format Fax
            original_fax = row.get('Facimile', '')
            clean_fax, fax_valid, fax_note = self.format_phone(original_fax)
            cleaned_df.loc[idx, 'CleanedFacimile'] = clean_fax
            cleaned_df.loc[idx, 'FaxValid'] = 'Y' if fax_valid else 'N'
            if not fax_valid and fax_note and fax_note != 'Empty phone number':
                notes.append(f"Fax: {fax_note}")
            
            # Verify address with geocoding if enabled
            if enable_geocoding:
                # Use the standardized address (before camelCase conversion) for verification
                address_for_verification = standardized_addr1  # Use standardized but not camelCased
                city_for_verification = str(row.get('City', '')).strip()  # Use original city
                state_for_verification = clean_state  # Use the validated state
                zip_for_verification = clean_zip  # Use the cleaned ZIP
                
                # Only verify if we have minimum required fields
                if address_for_verification and city_for_verification:
                    verification_result = self.verify_address_with_nominatim(
                        address_for_verification,
                        city_for_verification,
                        state_for_verification,
                        zip_for_verification
                    )
                    
                    cleaned_df.loc[idx, 'VerifiedAddress'] = verification_result['verified_address']
                    cleaned_df.loc[idx, 'AddressConfidence'] = verification_result['confidence']
                    cleaned_df.loc[idx, 'Latitude'] = verification_result['latitude']
                    cleaned_df.loc[idx, 'Longitude'] = verification_result['longitude']
                    
                    if not verification_result['verified']:
                        notes.append(f"Address verification: {verification_result['confidence']}")
                    
                    # Show progress for geocoding
                    if (idx + 1) % 10 == 0:
                        print(f"Verified {idx + 1} addresses...")
                else:
                    cleaned_df.loc[idx, 'VerifiedAddress'] = ''
                    cleaned_df.loc[idx, 'AddressConfidence'] = 'Missing Address Data'
                    cleaned_df.loc[idx, 'Latitude'] = ''
                    cleaned_df.loc[idx, 'Longitude'] = ''
                    notes.append("Address verification: Insufficient data")
            
            # Combine validation notes
            cleaned_df.loc[idx, 'ValidationNotes'] = '; '.join(notes) if notes else 'All validations passed'
            
            # Log progress every 100 records
            if (idx + 1) % 100 == 0:
                print(f"Processed {idx + 1} records...")
        
        print(f"Cleanup complete! Processed {len(cleaned_df)} records.")
        return cleaned_df
    
    def generate_summary_report(self, cleaned_df):
        """Generate a summary report of the cleanup process"""
        report = {
            'total_records': int(len(cleaned_df)),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_summary': {
                'valid_states': int(cleaned_df['StateValid'].value_counts().get('Y', 0)),
                'invalid_states': int(cleaned_df['StateValid'].value_counts().get('N', 0)),
                'valid_zips': int(cleaned_df['ZipValid'].value_counts().get('Y', 0)),
                'invalid_zips': int(cleaned_df['ZipValid'].value_counts().get('N', 0)),
                'valid_phones': int(cleaned_df['PhoneValid'].value_counts().get('Y', 0)),
                'invalid_phones': int(cleaned_df['PhoneValid'].value_counts().get('N', 0)),
                'valid_fax': int(cleaned_df['FaxValid'].value_counts().get('Y', 0)),
                'invalid_fax': int(cleaned_df['FaxValid'].value_counts().get('N', 0))
            },
            'unique_validation_errors': list(set(self.validation_errors))[:20]  # First 20 unique errors
        }
        
        # Calculate percentages
        total = report['total_records']
        if total > 0:
            report['validation_percentages'] = {
                'state_validity': round(report['validation_summary']['valid_states'] / total * 100, 2),
                'zip_validity': round(report['validation_summary']['valid_zips'] / total * 100, 2),
                'phone_validity': round(report['validation_summary']['valid_phones'] / total * 100, 2),
                'fax_validity': round(report['validation_summary']['valid_fax'] / total * 100, 2)
            }
        
        return report


def main(enable_geocoding=False):
    """Main execution function
    
    Args:
        enable_geocoding: If True, verify addresses with Nominatim (adds ~1.1 sec per record)
    """
    cleaner = HospitalDataCleaner()
    
    # Input and output file paths
    input_file = 'Concierge Hospitals.xlsx'
    output_file = 'Cleaned_Hospital_Data.xlsx'
    report_file = 'Cleanup_Report.json'
    validation_log = 'Validation_Issues.csv'
    
    try:
        # Check if input file exists
        if not os.path.exists(input_file):
            print(f"Error: Input file '{input_file}' not found!")
            return
        
        # Load the data
        print(f"Loading data from {input_file}...")
        df = pd.read_excel(input_file)
        print(f"Loaded {len(df)} records with columns: {list(df.columns)}")
        
        # Filter for active records only
        print(f"Filtering for active records only...")
        active_df = df[df['Active'] == 1].copy()
        print(f"Found {len(active_df)} active records out of {len(df)} total records")
        
        # Perform cleanup on active records only
        cleaned_df = cleaner.clean_hospital_data(active_df, enable_geocoding=enable_geocoding)
        
        # Select only the cleaned columns for output
        output_columns = [
            'CleanedHospitalName',
            'CleanedAddressOne', 
            'CleanedAddressTwo',
            'CleanedCity',
            'CleanedState',
            'CleanedZIP',
            'CleanedPhone',
            'CleanedFacimile'
        ]
        
        # Add geocoding columns if enabled
        if enable_geocoding:
            output_columns.extend([
                'VerifiedAddress',
                'AddressConfidence',
                'Latitude',
                'Longitude'
            ])
        
        # Add validation columns
        output_columns.extend([
            'StateValid',
            'ZipValid',
            'PhoneValid',
            'FaxValid',
            'ValidationNotes'
        ])
        
        # Create output dataframe with only cleaned columns
        output_df = cleaned_df[output_columns].copy()
        
        # Rename columns to remove 'Cleaned' prefix for cleaner output
        output_df.columns = output_df.columns.str.replace('Cleaned', '')
        
        # Save cleaned data to Excel
        print(f"\nSaving cleaned data to {output_file}...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            output_df.to_excel(writer, sheet_name='Cleaned Data', index=False)
        
        # Generate and save summary report
        report = cleaner.generate_summary_report(cleaned_df)
        print(f"\nSaving summary report to {report_file}...")
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Save validation issues to CSV
        validation_df = cleaned_df[cleaned_df['ValidationNotes'] != 'All validations passed'][
            ['HospitalName', 'State', 'ZIPCode', 'Phone', 'Facimile', 'ValidationNotes']
        ]
        if not validation_df.empty:
            print(f"\nSaving validation issues to {validation_log}...")
            validation_df.to_csv(validation_log, index=False)
            print(f"Found {len(validation_df)} records with validation issues")
        
        # Print summary
        print("\n" + "="*60)
        print("CLEANUP SUMMARY")
        print("="*60)
        print(f"Total Records Processed: {report['total_records']}")
        print(f"Valid States: {report['validation_summary']['valid_states']} ({report['validation_percentages']['state_validity']}%)")
        print(f"Valid ZIP Codes: {report['validation_summary']['valid_zips']} ({report['validation_percentages']['zip_validity']}%)")
        print(f"Valid Phone Numbers: {report['validation_summary']['valid_phones']} ({report['validation_percentages']['phone_validity']}%)")
        print(f"Valid Fax Numbers: {report['validation_summary']['valid_fax']} ({report['validation_percentages']['fax_validity']}%)")
        print("="*60)
        
        print(f"\nOutput files created:")
        print(f"  - {output_file}: Cleaned hospital data")
        print(f"  - {report_file}: Detailed cleanup report")
        if not validation_df.empty:
            print(f"  - {validation_log}: Records with validation issues")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    enable_geocoding = False
    if len(sys.argv) > 1:
        if '--geocode' in sys.argv or '-g' in sys.argv:
            enable_geocoding = True
            print("Address geocoding verification enabled!")
            print("Note: This will take approximately 1.1 seconds per address due to API rate limits.")
            response = input("Continue? (y/n): ")
            if response.lower() != 'y':
                print("Exiting...")
                sys.exit(0)
    else:
        print("Running without address verification (faster).")
        print("To enable address verification, run with --geocode or -g flag")
        print("Example: python hospital_data_cleanup.py --geocode")
    
    main(enable_geocoding=enable_geocoding)