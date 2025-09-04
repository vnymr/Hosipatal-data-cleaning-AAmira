#!/usr/bin/env python3
"""
Hospital Data Standardization and Cleanup System
================================================
Purpose: Normalize and standardize hospital records for data quality improvement
Author: Data Standardization Team
Version: 2.0
Date: 2025

This module provides comprehensive data cleaning including:
- Hospital name standardization and deduplication
- Address normalization and geocoding verification
- Phone/fax formatting
- State and ZIP code validation
"""

import pandas as pd
import re
import os
import sys
import json
import time
import logging
from datetime import datetime
from typing import Dict, Tuple, List
import requests

# ================================
# CONFIGURATION CONSTANTS
# ================================

# File paths
INPUT_FILE = 'Concierge Hospitals.xlsx'
OUTPUT_FILE = 'Cleaned_Hospital_Data.xlsx'
OUTPUT_FILE_WITH_GEO = 'Cleaned_Hospital_Data_with_Geocoding.xlsx'
REPORT_FILE = 'Cleanup_Report.json'
VALIDATION_LOG = 'Validation_Issues.csv'

# API Configuration
NOMINATIM_API_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "HospitalDataCleaner/2.0 (data standardization project)"
API_RATE_LIMIT_SECONDS = 1.1  # Nominatim requires max 1 request per second

# Processing Configuration
RECORDS_PER_PROGRESS_UPDATE = 100
GEOCODING_PROGRESS_UPDATE = 10

# US State Abbreviations
VALID_US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'AS', 'GU', 'MP'  # Including territories
}

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hospital_cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AddressStandardizer:
    """Handles address standardization and validation"""
    
    def __init__(self):
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
            'Pkwy.': 'Parkway', 'Pkwy': 'Parkway',
            'Hwy.': 'Highway', 'Hwy': 'Highway', 'HWY': 'Highway',
            'Trl.': 'Trail', 'Trl': 'Trail',
            'Way.': 'Way', 'Wy': 'Way', 'Wy.': 'Way',
            'Sq.': 'Square', 'Sq': 'Square',
            'Ter.': 'Terrace', 'Ter': 'Terrace',
            'Pt.': 'Point', 'Pt': 'Point',
            'Plaza.': 'Plaza', 'Plz': 'Plaza', 'Plz.': 'Plaza',
            'Expy.': 'Expressway', 'Expy': 'Expressway',
            'Fwy.': 'Freeway', 'Fwy': 'Freeway',
            'Tpke.': 'Turnpike', 'Tpke': 'Turnpike'
        }
        
        self.direction_exceptions = [
            'North Hill Road', 'North Shore Drive', 'South Park Avenue',
            'East River Road', 'West End Boulevard'
        ]
    
    def standardize(self, address: str) -> str:
        """
        Standardize address with direction and street type expansions
        
        Args:
            address: Raw address string
            
        Returns:
            Standardized address string
        """
        if not address or pd.isna(address):
            return ''
        
        address = self._clean_null_values(address)
        address = self._trim_excess_spaces(address)
        
        # Check for exceptions
        if any(exception in address for exception in self.direction_exceptions):
            return address
        
        # Tokenize and process
        tokens = address.split()
        new_tokens = []
        
        for i, token in enumerate(tokens):
            if token in self.direction_mapping and self._is_direction_context(tokens, i):
                new_tokens.append(self.direction_mapping[token])
            elif token in self.street_type_mapping and self._is_street_type_context(tokens, i):
                new_tokens.append(self.street_type_mapping[token])
            else:
                new_tokens.append(token)
        
        return ' '.join(new_tokens)
    
    def _is_direction_context(self, tokens: List[str], index: int) -> bool:
        """Check if token is likely a direction abbreviation"""
        return index == 0 or (index > 0 and tokens[index-1].isdigit())
    
    def _is_street_type_context(self, tokens: List[str], index: int) -> bool:
        """Check if token is likely a street type abbreviation"""
        is_last = index == len(tokens) - 1
        is_before_unit = (index < len(tokens) - 1 and 
                         (tokens[index+1].startswith('#') or 
                          tokens[index+1].lower() in ['suite', 'unit', 'apt']))
        return is_last or is_before_unit
    
    @staticmethod
    def _clean_null_values(value: str) -> str:
        """Clean NULL values and return empty string"""
        if pd.isna(value):
            return ''
        str_value = str(value).strip()
        return '' if str_value.upper() == 'NULL' else str_value
    
    @staticmethod
    def _trim_excess_spaces(text: str) -> str:
        """Remove excess spaces and trim"""
        if not text:
            return ''
        return re.sub(r'\s+', ' ', text).strip()


class NameStandardizer:
    """Handles hospital name standardization and deduplication"""
    
    def __init__(self):
        self.medical_abbrevs = {
            'mc': 'medical center', 'med': 'medical',
            'reg': 'regional', 'hosp': 'hospital',
            'hlth': 'health', 'ctr': 'center',
            'mem': 'memorial', 'comm': 'community',
            'gen': 'general', 'univ': 'university'
        }
        
        self.state_abbrevs = {
            'fl': 'florida', 'tx': 'texas', 'ca': 'california',
            'ny': 'new york', 'pa': 'pennsylvania'
        }
    
    def standardize_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize hospital names by finding and using the most complete version
        
        Args:
            df: DataFrame with HospitalName column
            
        Returns:
            DataFrame with standardized hospital names
        """
        logger.info("Standardizing hospital names...")
        df_copy = df.copy()
        
        # Group similar names
        name_groups = self._group_similar_names(df_copy)
        
        # Apply standardized names with Title Case
        for standard_name, indices in name_groups.items():
            # Convert to Title Case for final output
            formatted_name = self._format_hospital_name(standard_name)
            for idx in indices:
                df_copy.loc[idx, 'HospitalName'] = formatted_name
        
        logger.info(f"Standardized {len(name_groups)} unique names from {len(df_copy)} records")
        self._log_examples(df, name_groups)
        
        return df_copy
    
    def _format_hospital_name(self, name: str) -> str:
        """Format hospital name to proper Title Case"""
        if not name or pd.isna(name) or str(name).upper() == 'NULL':
            return ''
        
        # Clean and trim excess spaces
        name = re.sub(r'\s+', ' ', str(name)).strip()
        
        # Convert to title case with special handling for certain words
        words = name.split()
        result = []
        
        for i, word in enumerate(words):
            if word:
                # Keep certain words lowercase unless at the start
                if word.lower() in ['of', 'the', 'and', 'for', 'at'] and i > 0:
                    result.append(word.lower())
                else:
                    # Capitalize first letter, keep rest as-is for abbreviations
                    result.append(word[0].upper() + word[1:] if len(word) > 1 else word.upper())
        
        return ' '.join(result)
    
    def _group_similar_names(self, df: pd.DataFrame) -> Dict[str, List[int]]:
        """Group similar hospital names together"""
        name_groups = {}
        processed_indices = set()
        
        # Sort by name length (prefer longer names)
        sorted_data = df.sort_values('HospitalName', 
                                    key=lambda x: x.str.len(), 
                                    ascending=False)
        
        for idx, row in sorted_data.iterrows():
            if idx in processed_indices:
                continue
            
            name = str(row.get('HospitalName', '')).strip()
            if not name or name == 'NULL':
                continue
            
            # Find all similar names
            current_group = [idx]
            processed_indices.add(idx)
            
            for idx2, row2 in sorted_data.iterrows():
                if idx2 in processed_indices:
                    continue
                
                name2 = str(row2.get('HospitalName', '')).strip()
                if not name2 or name2 == 'NULL':
                    continue
                
                if self._calculate_similarity(name, name2) >= 0.6:
                    current_group.append(idx2)
                    processed_indices.add(idx2)
            
            if current_group:
                name_groups[name] = current_group
        
        return name_groups
    
    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity score between two hospital names"""
        norm1 = self._normalize_for_comparison(name1)
        norm2 = self._normalize_for_comparison(name2)
        
        # Check substring relationship
        if norm1 in norm2 or norm2 in norm1:
            return 1.0
        
        # Calculate Jaccard similarity
        tokens1 = set(norm1.split())
        tokens2 = set(norm2.split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union) if union else 0.0
    
    def _normalize_for_comparison(self, name: str) -> str:
        """Normalize name for comparison"""
        name = name.lower()
        
        # Expand abbreviations
        for abbrev, full in {**self.medical_abbrevs, **self.state_abbrevs}.items():
            name = name.replace(abbrev, full)
        
        # Clean punctuation
        name = re.sub(r'[^\w\s]', ' ', name)
        return ' '.join(name.split())
    
    def _log_examples(self, original_df: pd.DataFrame, 
                     name_groups: Dict) -> None:
        """Log examples of standardization"""
        examples_shown = 0
        for standard_name, indices in name_groups.items():
            if len(indices) > 1 and examples_shown < 5:
                original_names = set()
                for idx in indices[:3]:
                    original = original_df.loc[idx, 'HospitalName']
                    if original != standard_name:
                        original_names.add(original)
                if original_names:
                    logger.info(f"  Standardized: {', '.join(original_names)} → {standard_name}")
                    examples_shown += 1


class DataValidator:
    """Handles validation of various data fields"""
    
    @staticmethod
    def validate_state(state: str) -> Tuple[str, bool, str]:
        """Validate and uppercase state abbreviation"""
        if not state or pd.isna(state):
            return '', False, 'Empty state value'
        
        state = str(state).strip().upper()
        
        if state in VALID_US_STATES:
            return state, True, ''
        else:
            return state, False, f'Invalid state: {state}'
    
    @staticmethod
    def validate_zip(zip_code: str) -> Tuple[str, bool, str]:
        """Standardize ZIP code to 5 digits"""
        if not zip_code or pd.isna(zip_code):
            return '', False, 'Empty ZIP code'
        
        zip_digits = re.sub(r'\D', '', str(zip_code))
        
        if len(zip_digits) == 0:
            return '', False, f'No digits in ZIP: {zip_code}'
        elif len(zip_digits) < 5:
            return zip_digits, False, f'ZIP too short: {zip_code}'
        else:
            return zip_digits[:5], True, ''
    
    @staticmethod
    def format_phone(phone: str) -> Tuple[str, bool, str]:
        """Format phone to (XXX) XXX-XXXX or empty for invalid/placeholder numbers"""
        if not phone or pd.isna(phone) or str(phone).upper() == 'NULL':
            return '', False, 'Empty phone number'
        
        phone_str = str(phone).strip()
        digits = re.sub(r'\D', '', phone_str)
        
        # Check for placeholder patterns
        if DataValidator._is_placeholder_phone(phone_str, digits):
            return '', False, 'Placeholder number'
        
        # Drop leading country code 1 for US numbers
        if len(digits) == 11 and digits[0] == '1':
            digits = digits[1:]
        
        if len(digits) == 10:
            # Format as (XXX) XXX-XXXX
            formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            return formatted, True, ''
        elif len(digits) == 0:
            return '', False, 'No digits in phone'
        else:
            return '', False, f'Invalid format ({len(digits)} digits)'
    
    @staticmethod
    def _is_placeholder_phone(phone_str: str, digits: str) -> bool:
        """Check if phone number is a placeholder"""
        # Check for common placeholder patterns
        if not digits:
            return False
            
        # Single digit repeated (e.g., 1111111111, 2222222222)
        if len(set(digits)) == 1:
            return True
        
        # Very short numbers (less than 5 digits)
        if len(digits) < 5:
            return True
        
        # Patterns like 0000000000, 1234567890
        placeholder_patterns = [
            '0000000000', '1111111111', '2222222222', '3333333333',
            '4444444444', '5555555555', '6666666666', '7777777777',
            '8888888888', '9999999999', '1234567890', '0123456789'
        ]
        
        if digits in placeholder_patterns:
            return True
        
        # Check for 'xxxxx' patterns in original string
        lower_str = phone_str.lower()
        if 'xxxx' in lower_str or 'placeholder' in lower_str:
            return True
        
        # Single digits like '0' or '1'
        if digits in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            return True
            
        return False


class GeocodingService:
    """Handles address verification using Nominatim API"""
    
    @staticmethod
    def verify_address(address: str, city: str, state: str, 
                      zip_code: str) -> Dict[str, any]:
        """
        Verify address using Nominatim geocoding service
        
        Args:
            address: Street address
            city: City name
            state: State abbreviation
            zip_code: ZIP code
            
        Returns:
            Dictionary with verification results
        """
        try:
            full_address = f"{address}, {city}, {state} {zip_code}, USA"
            
            params = {
                'q': full_address,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1,
                'countrycodes': 'us'
            }
            
            headers = {'User-Agent': NOMINATIM_USER_AGENT}
            
            response = requests.get(NOMINATIM_API_URL, 
                                  params=params, 
                                  headers=headers, 
                                  timeout=10)
            
            # Rate limiting
            time.sleep(API_RATE_LIMIT_SECONDS)
            
            if response.status_code == 200 and response.json():
                return GeocodingService._parse_response(response.json()[0], full_address)
            
            return GeocodingService._empty_result(full_address, 'Not Found')
            
        except Exception as e:
            logger.error(f"Geocoding error: {str(e)}")
            return GeocodingService._empty_result(
                f"{address}, {city}, {state} {zip_code}", 'Error'
            )
    
    @staticmethod
    def _parse_response(data: Dict, query: str) -> Dict[str, any]:
        """Parse Nominatim API response"""
        address_parts = data.get('address', {})
        
        # Build verified address
        house_number = address_parts.get('house_number', '')
        road = address_parts.get('road', '')
        verified_parts = []
        
        if house_number and road:
            verified_parts.append(f"{house_number} {road}")
        elif road:
            verified_parts.append(road)
        
        city = (address_parts.get('city') or 
                address_parts.get('town') or 
                address_parts.get('village', ''))
        state = address_parts.get('state', '')
        zip_code = address_parts.get('postcode', '')
        
        verified_address = ', '.join(filter(None, 
            [verified_parts[0] if verified_parts else '', 
             city, f"{state} {zip_code}".strip()]))
        
        # Determine confidence
        place_rank = int(data.get('place_rank', 30))
        confidence = 'High' if place_rank <= 20 else 'Medium' if place_rank <= 25 else 'Low'
        
        return {
            'verified': True,
            'verified_address': verified_address or data.get('display_name', ''),
            'latitude': data.get('lat', ''),
            'longitude': data.get('lon', ''),
            'confidence': confidence,
            'original_query': query
        }
    
    @staticmethod
    def _empty_result(query: str, status: str) -> Dict[str, any]:
        """Return empty result for failed verification"""
        return {
            'verified': False,
            'verified_address': '',
            'latitude': '',
            'longitude': '',
            'confidence': status,
            'original_query': query
        }


class HospitalDataCleaner:
    """Main class for hospital data cleanup operations"""
    
    def __init__(self):
        self.address_standardizer = AddressStandardizer()
        self.name_standardizer = NameStandardizer()
        self.validator = DataValidator()
        self.geocoding_service = GeocodingService()
        self.validation_errors = []
    
    def clean_data(self, df: pd.DataFrame, enable_geocoding: bool = False) -> pd.DataFrame:
        """
        Main cleaning function for hospital data
        
        Args:
            df: Input DataFrame with hospital data
            enable_geocoding: Enable address verification via geocoding
            
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Starting cleanup of {len(df)} records...")
        
        # Standardize hospital names first
        df = self.name_standardizer.standardize_names(df)
        
        # Create output DataFrame
        cleaned_df = df.copy()
        self._initialize_columns(cleaned_df, enable_geocoding)
        
        # Process each record
        for idx, row in df.iterrows():
            self._process_record(idx, row, cleaned_df, enable_geocoding)
            
            if (idx + 1) % RECORDS_PER_PROGRESS_UPDATE == 0:
                logger.info(f"Processed {idx + 1} records...")
        
        logger.info(f"Cleanup complete! Processed {len(cleaned_df)} records.")
        return cleaned_df
    
    def _initialize_columns(self, df: pd.DataFrame, enable_geocoding: bool) -> None:
        """Initialize validation and output columns"""
        df['StateValid'] = ''
        df['ZipValid'] = ''
        df['PhoneValid'] = ''
        df['FaxValid'] = ''
        df['ValidationNotes'] = ''
        
        if enable_geocoding:
            df['VerifiedAddress'] = ''
            df['AddressConfidence'] = ''
            df['Latitude'] = ''
            df['Longitude'] = ''
            estimated_time = len(df) * API_RATE_LIMIT_SECONDS / 60
            logger.info(f"Address verification enabled. Estimated time: {estimated_time:.1f} minutes")
    
    def _process_record(self, idx: int, row: pd.Series, 
                       df: pd.DataFrame, enable_geocoding: bool) -> None:
        """Process a single record"""
        notes = []
        
        # Preserve ClinicKey and HospitalKey
        df.loc[idx, 'ClinicKey'] = row.get('ClinicKey', '')
        df.loc[idx, 'HospitalKey'] = row.get('HospitalKey', '')
        
        # Clean hospital name (Title Case)
        df.loc[idx, 'CleanedHospitalName'] = self._to_title_case(row.get('HospitalName', ''))
        
        # Clean addresses (Title Case)
        addr1 = self.address_standardizer.standardize(row.get('AddressOne', ''))
        df.loc[idx, 'CleanedAddressOne'] = self._to_title_case(addr1)
        
        addr2 = self._clean_address_with_pound(row.get('AddressTwo', ''))
        df.loc[idx, 'CleanedAddressTwo'] = self._to_title_case(addr2) if addr2 else ''
        
        # Clean city (Title Case)
        df.loc[idx, 'CleanedCity'] = self._to_title_case(row.get('City', ''))
        
        # Validate state
        state, state_valid, state_note = self.validator.validate_state(row.get('State', ''))
        df.loc[idx, 'CleanedState'] = state
        df.loc[idx, 'StateValid'] = 'Y' if state_valid else 'N'
        if not state_valid:
            notes.append(state_note)
            self.validation_errors.append(f"Invalid state: {row.get('State', '')}")
        
        # Validate ZIP
        zip_code, zip_valid, zip_note = self.validator.validate_zip(row.get('ZIPCode', ''))
        df.loc[idx, 'CleanedZIP'] = zip_code
        df.loc[idx, 'ZipValid'] = 'Y' if zip_valid else 'N'
        if not zip_valid:
            notes.append(zip_note)
        
        # Format phone
        phone, phone_valid, phone_note = self.validator.format_phone(row.get('Phone', ''))
        df.loc[idx, 'CleanedPhone'] = phone
        df.loc[idx, 'PhoneValid'] = 'Y' if phone_valid else 'N'
        if not phone_valid and phone_note:
            notes.append(phone_note)
        
        # Format fax
        fax, fax_valid, fax_note = self.validator.format_phone(row.get('Facimile', ''))
        df.loc[idx, 'CleanedFacimile'] = fax
        df.loc[idx, 'FaxValid'] = 'Y' if fax_valid else 'N'
        if not fax_valid and fax_note and 'Empty' not in fax_note:
            notes.append(f"Fax: {fax_note}")
        
        # Geocode if enabled
        if enable_geocoding and addr1 and row.get('City'):
            self._geocode_address(idx, df, addr1, row.get('City', ''), 
                                 state, zip_code, notes)
        
        # Set validation notes
        df.loc[idx, 'ValidationNotes'] = '; '.join(notes) if notes else 'All validations passed'
    
    def _geocode_address(self, idx: int, df: pd.DataFrame, 
                        address: str, city: str, state: str, 
                        zip_code: str, notes: List[str]) -> None:
        """Geocode and verify address"""
        result = self.geocoding_service.verify_address(address, city, state, zip_code)
        
        df.loc[idx, 'VerifiedAddress'] = result['verified_address']
        df.loc[idx, 'AddressConfidence'] = result['confidence']
        df.loc[idx, 'Latitude'] = result['latitude']
        df.loc[idx, 'Longitude'] = result['longitude']
        
        if not result['verified']:
            notes.append(f"Address verification: {result['confidence']}")
        
        if (idx + 1) % GEOCODING_PROGRESS_UPDATE == 0:
            logger.info(f"Verified {idx + 1} addresses...")
    
    @staticmethod
    def _to_title_case(text: str) -> str:
        """Convert text to Title Case with proper spacing"""
        if not text or pd.isna(text) or str(text).upper() == 'NULL':
            return ''
        
        # Clean and trim excess spaces
        text = re.sub(r'\s+', ' ', str(text)).strip()
        
        # Convert to title case
        words = text.split()
        result = []
        
        for word in words:
            if word:
                # Keep certain words lowercase (articles, prepositions) unless at start
                if word.lower() in ['of', 'the', 'and', 'or', 'in', 'at', 'for'] and len(result) > 0:
                    result.append(word.lower())
                else:
                    # Capitalize first letter, lowercase the rest
                    result.append(word[0].upper() + word[1:].lower() if len(word) > 1 else word.upper())
        
        return ' '.join(result)
    
    @staticmethod
    def _clean_address_with_pound(address: str) -> str:
        """Clean address keeping # for suite/unit numbers"""
        if not address or pd.isna(address) or str(address).upper() == 'NULL':
            return ''
        # Preserve # for suite/unit numbers but clean excess spaces
        return re.sub(r'\s+', ' ', str(address)).strip()
    
    def generate_summary_report(self, df: pd.DataFrame) -> Dict:
        """Generate cleanup summary report"""
        report = {
            'total_records': int(len(df)),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_summary': {
                'valid_states': int(df['StateValid'].value_counts().get('Y', 0)),
                'invalid_states': int(df['StateValid'].value_counts().get('N', 0)),
                'valid_zips': int(df['ZipValid'].value_counts().get('Y', 0)),
                'invalid_zips': int(df['ZipValid'].value_counts().get('N', 0)),
                'valid_phones': int(df['PhoneValid'].value_counts().get('Y', 0)),
                'invalid_phones': int(df['PhoneValid'].value_counts().get('N', 0)),
                'valid_fax': int(df['FaxValid'].value_counts().get('Y', 0)),
                'invalid_fax': int(df['FaxValid'].value_counts().get('N', 0))
            },
            'unique_validation_errors': list(set(self.validation_errors))[:20]
        }
        
        # Calculate percentages
        if report['total_records'] > 0:
            report['validation_percentages'] = {
                field: round(report['validation_summary'][f'valid_{field}'] / 
                           report['total_records'] * 100, 2)
                for field in ['states', 'zips', 'phones', 'fax']
            }
        
        return report


def main(enable_geocoding: bool = False):
    """
    Main execution function
    
    Args:
        enable_geocoding: Enable address verification via Nominatim API
    """
    try:
        # Validate input file
        if not os.path.exists(INPUT_FILE):
            logger.error(f"Input file '{INPUT_FILE}' not found!")
            return 1
        
        # Load data
        logger.info(f"Loading data from {INPUT_FILE}...")
        df = pd.read_excel(INPUT_FILE)
        logger.info(f"Loaded {len(df)} records")
        
        # Filter active records
        active_df = df[df['Active'] == 1].copy()
        logger.info(f"Found {len(active_df)} active records out of {len(df)} total")
        
        # Clean data
        cleaner = HospitalDataCleaner()
        cleaned_df = cleaner.clean_data(active_df, enable_geocoding)
        
        # Prepare output columns
        output_columns = [
            'ClinicKey', 'HospitalKey',  # Preserve keys
            'CleanedHospitalName', 'CleanedAddressOne', 'CleanedAddressTwo',
            'CleanedCity', 'CleanedState', 'CleanedZIP',
            'CleanedPhone', 'CleanedFacimile'
        ]
        
        if enable_geocoding:
            output_columns.extend([
                'VerifiedAddress', 'AddressConfidence',
                'Latitude', 'Longitude'
            ])
        
        output_columns.extend([
            'StateValid', 'ZipValid', 'PhoneValid', 'FaxValid',
            'ValidationNotes'
        ])
        
        # Create output dataframe
        output_df = cleaned_df[output_columns].copy()
        output_df.columns = output_df.columns.str.replace('Cleaned', '')
        
        # Save results - use different filename if geocoding was enabled
        output_file = OUTPUT_FILE_WITH_GEO if enable_geocoding else OUTPUT_FILE
        logger.info(f"Saving cleaned data to {output_file}...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            output_df.to_excel(writer, sheet_name='Cleaned Data', index=False)
        
        # Generate report
        report = cleaner.generate_summary_report(cleaned_df)
        with open(REPORT_FILE, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Save validation issues
        validation_df = cleaned_df[cleaned_df['ValidationNotes'] != 'All validations passed'][
            ['CleanedHospitalName', 'CleanedState', 'CleanedZIP', 
             'CleanedPhone', 'CleanedFacimile', 'ValidationNotes']
        ]
        
        if not validation_df.empty:
            validation_df.to_csv(VALIDATION_LOG, index=False)
            logger.info(f"Found {len(validation_df)} records with validation issues")
        
        # Print summary
        print("\n" + "="*60)
        print("CLEANUP SUMMARY")
        print("="*60)
        print(f"Total Records: {report['total_records']}")
        print(f"Valid States: {report['validation_percentages']['states']}%")
        print(f"Valid ZIPs: {report['validation_percentages']['zips']}%")
        print(f"Valid Phones: {report['validation_percentages']['phones']}%")
        print(f"Output File: {output_file}")
        if enable_geocoding:
            print("Address verification completed with geocoding")
        print("="*60)
        
        logger.info("Cleanup process completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    # Parse command line arguments
    enable_geocoding = '--geocode' in sys.argv or '-g' in sys.argv
    
    if enable_geocoding:
        print("Address geocoding verification enabled!")
        print("Note: This will take approximately 1.1 seconds per address.")
        response = input("Continue? (y/n): ")
        if response.lower() != 'y':
            print("Exiting...")
            sys.exit(0)
    else:
        print("Running without address verification (faster).")
        print("To enable address verification, run with --geocode flag")
    
    sys.exit(main(enable_geocoding))