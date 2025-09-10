#!/usr/bin/env python3

import pandas as pd
import re
import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, Tuple, List
import requests

# File paths
INPUT_FILE = 'Concierge Hospitals.xlsx'
OUTPUT_FILE = 'Cleaned_Hospital_Data.xlsx'
OUTPUT_FILE_WITH_GEO = 'Cleaned_Hospital_Data_with_Geocoding.xlsx'

# API Configuration
NOMINATIM_API_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "HospitalDataCleaner/2.0"
API_RATE_LIMIT_SECONDS = 1.1

# US State Abbreviations
VALID_US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'AS', 'GU', 'MP'
}


class AddressStandardizer:
    def __init__(self):
        self._dir_expand = {
            'N': 'North', 'S': 'South', 'E': 'East', 'W': 'West',
            'NE': 'Northeast', 'NW': 'Northwest', 'SE': 'Southeast', 'SW': 'Southwest'
        }
        
        self._street_expand = {
            'ST': 'Street', 'AVE': 'Avenue', 'BLVD': 'Boulevard', 'DR': 'Drive',
            'RD': 'Road', 'LN': 'Lane', 'CT': 'Court', 'CIR': 'Circle',
            'PL': 'Place', 'PLZ': 'Plaza', 'TRL': 'Trail', 'TER': 'Terrace',
            'PKWY': 'Parkway', 'HWY': 'Highway', 'TPKE': 'Turnpike', 'WAY': 'Way',
            'SQ': 'Square', 'PT': 'Point', 'WY': 'Way'
        }
        
        self._unit_expand = {
            'STE': 'Suite', 'SUITE': 'Suite',
            'FL': 'Floor', 'FLOOR': 'Floor',
            'APT': 'Apartment', 'APARTMENT': 'Apartment',
            'BLDG': 'Building', 'BLD': 'Building', 'BUILDING': 'Building',
            'UNIT': 'Unit'
        }
    
    def standardize(self, address: str) -> str:
        if not address or pd.isna(address):
            return ''
        
        address = self._clean_null_values(address)
        address = self._trim_excess_spaces(address)
        
        tokens = address.split()
        new_tokens = []
        
        for i, token in enumerate(tokens):
            raw = token
            core = raw.strip(',')
            norm = core.rstrip('.').upper()
            
            if '#' in raw and norm == core.upper():
                new_tokens.append(raw)
                continue
            
            if norm in self._dir_expand:
                new_tokens.append(raw.replace(core, self._dir_expand[norm]))
                continue
            
            if norm in self._street_expand and self._is_street_type_context(tokens, i):
                new_tokens.append(raw.replace(core, self._street_expand[norm]))
                continue
            
            if norm in self._unit_expand:
                new_tokens.append(raw.replace(core, self._unit_expand[norm]))
                continue
            
            new_tokens.append(raw)
        
        return ' '.join(new_tokens)
    
    def _is_street_type_context(self, tokens: List[str], index: int) -> bool:
        is_last = index == len(tokens) - 1
        next_tok = tokens[index+1] if index < len(tokens) - 1 else ''
        next_core = next_tok.strip(',').rstrip('.')
        next_norm = next_core.upper()
        unit_like = {'#','STE','SUITE','FL','FLOOR','APT','APARTMENT','BLDG','BLD','BUILDING','UNIT'}
        is_before_unit = (next_tok.startswith('#') or next_norm in unit_like)
        dir_like = {'N','S','E','W','NE','NW','SE','SW'}
        is_before_dir = next_norm in dir_like
        is_lettered_follow = bool(re.match(r'^[A-Za-z]$|^\d+[A-Za-z]?$', next_core))
        return is_last or is_before_unit or is_before_dir or is_lettered_follow
    
    @staticmethod
    def _clean_null_values(value: str) -> str:
        if pd.isna(value):
            return ''
        str_value = str(value).strip()
        return '' if str_value.upper() == 'NULL' else str_value
    
    @staticmethod
    def _trim_excess_spaces(text: str) -> str:
        if not text:
            return ''
        return re.sub(r'\s+', ' ', text).strip()


class NameStandardizer:
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
        df_copy = df.copy()
        name_groups = self._group_similar_names(df_copy)
        
        for standard_name, indices in name_groups.items():
            formatted_name = self._format_hospital_name(standard_name)
            for idx in indices:
                df_copy.loc[idx, 'HospitalName'] = formatted_name
        
        return df_copy
    
    def _format_hospital_name(self, name: str) -> str:
        if not name or pd.isna(name) or str(name).upper() == 'NULL':
            return ''
        
        name = re.sub(r'\s+', ' ', str(name)).strip()
        words = name.split()
        result = []
        
        for i, word in enumerate(words):
            if word:
                if word.lower() in ['of', 'the', 'and', 'for', 'at'] and i > 0:
                    result.append(word.lower())
                else:
                    result.append(word[0].upper() + word[1:] if len(word) > 1 else word.upper())
        
        return ' '.join(result)
    
    def _group_similar_names(self, df: pd.DataFrame) -> Dict[str, List[int]]:
        name_groups = {}
        processed_indices = set()
        
        sorted_data = df.sort_values('HospitalName', 
                                    key=lambda x: x.str.len(), 
                                    ascending=False)
        
        for idx, row in sorted_data.iterrows():
            if idx in processed_indices:
                continue
            
            name = str(row.get('HospitalName', '')).strip()
            if not name or name == 'NULL':
                continue
            
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
        norm1 = self._normalize_for_comparison(name1)
        norm2 = self._normalize_for_comparison(name2)
        
        if norm1 in norm2 or norm2 in norm1:
            return 1.0
        
        tokens1 = set(norm1.split())
        tokens2 = set(norm2.split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union) if union else 0.0
    
    def _normalize_for_comparison(self, name: str) -> str:
        name = name.lower()
        
        for abbrev, full in {**self.medical_abbrevs, **self.state_abbrevs}.items():
            name = name.replace(abbrev, full)
        
        name = re.sub(r'[^\w\s]', ' ', name)
        return ' '.join(name.split())


class DataValidator:
    @staticmethod
    def validate_state(state: str) -> Tuple[str, bool, str]:
        if not state or pd.isna(state):
            return '', False, 'Empty state value'
        
        state = str(state).strip().upper()
        
        if state in VALID_US_STATES:
            return state, True, ''
        else:
            return state, False, f'Invalid state: {state}'
    
    @staticmethod
    def validate_zip(zip_code: str) -> Tuple[str, bool, str]:
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
        if not phone or pd.isna(phone) or str(phone).upper() == 'NULL':
            return '', False, 'Empty phone number'
        
        phone_str = str(phone).strip()
        digits = re.sub(r'\D', '', phone_str)
        
        if DataValidator._is_placeholder_phone(phone_str, digits):
            return '', False, 'Placeholder number'
        
        if len(digits) == 11 and digits[0] == '1':
            digits = digits[1:]
        
        if len(digits) == 10:
            return digits, True, ''
        elif len(digits) == 0:
            return '', False, 'No digits in phone'
        else:
            return '', False, f'Invalid format ({len(digits)} digits)'
    
    @staticmethod
    def _is_placeholder_phone(phone_str: str, digits: str) -> bool:
        if not digits:
            return False
            
        if len(set(digits)) == 1:
            return True
        
        if len(digits) < 5:
            return True
        
        placeholder_patterns = [
            '0000000000', '1111111111', '2222222222', '3333333333',
            '4444444444', '5555555555', '6666666666', '7777777777',
            '8888888888', '9999999999', '1234567890', '0123456789'
        ]
        
        if digits in placeholder_patterns:
            return True
        
        lower_str = phone_str.lower()
        if 'xxxx' in lower_str or 'placeholder' in lower_str:
            return True
        
        if digits in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            return True
            
        return False


class GeocodingService:
    @staticmethod
    def verify_address(address: str, city: str, state: str, 
                      zip_code: str) -> Dict[str, any]:
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
            
            time.sleep(API_RATE_LIMIT_SECONDS)
            
            if response.status_code == 200 and response.json():
                return GeocodingService._parse_response(response.json()[0], full_address)
            
            return GeocodingService._empty_result(full_address, 'Not Found')
            
        except Exception as e:
            return GeocodingService._empty_result(
                f"{address}, {city}, {state} {zip_code}", 'Error'
            )
    
    @staticmethod
    def _parse_response(data: Dict, query: str) -> Dict[str, any]:
        address_parts = data.get('address', {})
        
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
        return {
            'verified': False,
            'verified_address': '',
            'latitude': '',
            'longitude': '',
            'confidence': status,
            'original_query': query
        }


class HospitalDataCleaner:
    def __init__(self):
        self.address_standardizer = AddressStandardizer()
        self.name_standardizer = NameStandardizer()
        self.validator = DataValidator()
        self.geocoding_service = GeocodingService()
        self.general_abbrev = {
            'highway': 'hwy', 'freeway': 'fwy', 'road': 'rd', 'street': 'st',
            'avenue': 'ave', 'boulevard': 'blvd', 'drive': 'dr', 'lane': 'ln',
            'parkway': 'pkwy', 'suite': 'ste',
            'north': 'n', 'south': 's', 'east': 'e', 'west': 'w',
            'saint': 'st'
        }
    
    def clean_data(self, df: pd.DataFrame, enable_geocoding: bool = False) -> pd.DataFrame:
        df = self.name_standardizer.standardize_names(df)
        
        cleaned_df = df.copy()
        self._initialize_columns(cleaned_df, enable_geocoding)
        
        for idx, row in df.iterrows():
            self._process_record(idx, row, cleaned_df, enable_geocoding)
        
        cleaned_df = self._impute_missing_by_name(cleaned_df)
        
        return cleaned_df
    
    def _initialize_columns(self, df: pd.DataFrame, enable_geocoding: bool) -> None:
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
    
    def _process_record(self, idx: int, row: pd.Series, 
                       df: pd.DataFrame, enable_geocoding: bool) -> None:
        notes = []
        
        df.loc[idx, 'ClinicKey'] = row.get('ClinicKey', '')
        df.loc[idx, 'HospitalKey'] = row.get('HospitalKey', '')
        
        raw_name = row.get('HospitalName', '')
        name_abbr = self._abbreviate_text(raw_name)
        name_camel = self._to_camel_case(name_abbr)
        name_camel = re.sub(r'VA$', 'Va', name_camel)
        df.loc[idx, 'CleanedHospitalName'] = name_camel
        
        addr1 = self.address_standardizer.standardize(row.get('AddressOne', ''))
        df.loc[idx, 'CleanedAddressOne'] = self._to_address_case(addr1)
        
        addr2_raw = self._clean_address_with_pound(row.get('AddressTwo', ''))
        addr2_std = self.address_standardizer.standardize(addr2_raw) if addr2_raw else ''
        df.loc[idx, 'CleanedAddressTwo'] = self._to_address_case(addr2_std) if addr2_std else ''
        
        df.loc[idx, 'CleanedCity'] = self._to_title_case(row.get('City', ''))
        
        state, state_valid, state_note = self.validator.validate_state(row.get('State', ''))
        df.loc[idx, 'CleanedState'] = state
        df.loc[idx, 'StateValid'] = 'Y' if state_valid else 'N'
        if not state_valid:
            notes.append(state_note)
        
        zip_code, zip_valid, zip_note = self.validator.validate_zip(row.get('ZIPCode', ''))
        df.loc[idx, 'CleanedZIP'] = zip_code
        df.loc[idx, 'ZipValid'] = 'Y' if zip_valid else 'N'
        if not zip_valid:
            notes.append(zip_note)
        
        phone, phone_valid, phone_note = self.validator.format_phone(row.get('Phone', ''))
        df.loc[idx, 'CleanedPhone'] = phone
        df.loc[idx, 'PhoneValid'] = 'Y' if phone_valid else 'N'
        if not phone_valid and phone_note:
            notes.append(phone_note)
        
        fax, fax_valid, fax_note = self.validator.format_phone(row.get('Facimile', ''))
        df.loc[idx, 'CleanedFacimile'] = fax
        df.loc[idx, 'FaxValid'] = 'Y' if fax_valid else 'N'
        if not fax_valid and fax_note and 'Empty' not in fax_note:
            notes.append(f"Fax: {fax_note}")
        
        if enable_geocoding and addr1 and row.get('City'):
            self._geocode_address(idx, df, addr1, row.get('City', ''), 
                                 state, zip_code, notes)
        
        df.loc[idx, 'ValidationNotes'] = '; '.join(notes) if notes else 'All validations passed'
    
    @staticmethod
    def _is_blank(val) -> bool:
        return (val is None) or (pd.isna(val)) or (str(val).strip() == '')
    
    @staticmethod
    def _is_invalid_address(val: str) -> bool:
        if not val or pd.isna(val):
            return True
        s = str(val).strip()
        if len(s) < 5:
            return True
        if not re.search(r'[A-Za-z]', s):
            return True
        return False
    
    @staticmethod
    def _is_invalid_city(val: str) -> bool:
        if not val or pd.isna(val):
            return True
        s = str(val).strip()
        if len(s) < 2:
            return True
        if not re.search(r'[A-Za-z]', s):
            return True
        return False
    
    def _impute_missing_by_name(self, df: pd.DataFrame) -> pd.DataFrame:
        key = 'CleanedHospitalName'
        if key not in df.columns:
            return df
        
        fields = [
            ('CleanedAddressOne', None),
            ('CleanedAddressTwo', None),
            ('CleanedCity', None),
            ('CleanedState', 'StateValid'),
            ('CleanedZIP', 'ZipValid'),
            ('CleanedPhone', 'PhoneValid'),
            ('CleanedFacimile', 'FaxValid'),
        ]
        
        for name, group in df.groupby(key):
            for field, valid_flag in fields:
                if field not in df.columns:
                    continue
                series = group[field]
                
                if field == 'CleanedPhone':
                    candidates = group.loc[group['PhoneValid'] == 'Y', field]
                elif field == 'CleanedFacimile':
                    candidates = group.loc[group['FaxValid'] == 'Y', field]
                elif field == 'CleanedState':
                    candidates = group.loc[group['StateValid'] == 'Y', field]
                elif field == 'CleanedZIP':
                    candidates = group.loc[group['ZipValid'] == 'Y', field]
                elif field.startswith('CleanedAddress'):
                    candidates = group.loc[~group[field].apply(self._is_invalid_address), field]
                else:
                    candidates = group.loc[~group[field].apply(self._is_blank), field]
                
                values = [str(v) for v in candidates if not self._is_blank(v)]
                if not values:
                    continue
                
                from collections import Counter
                cnt = Counter(values)
                mode_val, mode_freq = cnt.most_common(1)[0]
                top_freqs = [c for v,c in cnt.items() if c == mode_freq]
                if len(top_freqs) > 1:
                    continue
                
                idxs = []
                if field == 'CleanedPhone':
                    idxs = group.index[(group['PhoneValid'] != 'Y') | df.loc[group.index, field].apply(self._is_blank)]
                elif field == 'CleanedFacimile':
                    idxs = group.index[(group['FaxValid'] != 'Y') | df.loc[group.index, field].apply(self._is_blank)]
                elif field == 'CleanedState':
                    idxs = group.index[(group['StateValid'] != 'Y') | df.loc[group.index, field].apply(self._is_blank)]
                elif field == 'CleanedZIP':
                    idxs = group.index[(group['ZipValid'] != 'Y') | df.loc[group.index, field].apply(self._is_blank)]
                elif field.startswith('CleanedAddress'):
                    idxs = group.index[df.loc[group.index, field].apply(self._is_invalid_address)]
                elif field == 'CleanedCity':
                    idxs = group.index[df.loc[group.index, field].apply(self._is_invalid_city)]
                else:
                    idxs = group.index[df.loc[group.index, field].apply(self._is_blank)]
                
                if len(idxs) == 0:
                    continue
                
                df.loc[idxs, field] = mode_val
                if valid_flag:
                    if valid_flag in df.columns:
                        df.loc[idxs, valid_flag] = 'Y'
                df.loc[idxs, 'ValidationNotes'] = df.loc[idxs, 'ValidationNotes'].apply(
                    lambda s: (s + '; Imputed ' + field.replace('Cleaned','') + ' by name') if s and s != 'All validations passed' else ('Imputed ' + field.replace('Cleaned','') + ' by name')
                )
        
        return df
    
    def _abbreviate_text(self, text: str) -> str:
        if not text or pd.isna(text) or str(text).upper() == 'NULL':
            return ''
        s = str(text)
        for full, abbr in self.general_abbrev.items():
            s = re.sub(rf"\b{re.escape(full)}\b", abbr, s, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', s).strip()
    
    @staticmethod
    def _to_camel_case(text: str) -> str:
        if not text:
            return ''
        s = str(text).strip()
        s = re.sub(r'[^A-Za-z0-9]+', ' ', s)
        s = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', s)
        tokens = [t for t in s.split() if t]
        if not tokens:
            return ''
        first = tokens[0].lower()
        rest = [t[:1].upper() + t[1:].lower() if len(t) > 1 else t.upper() for t in tokens[1:]]
        return ''.join([first] + rest)
    
    @staticmethod
    def _to_address_case(text: str) -> str:
        if not text:
            return ''
        words = re.sub(r'\s+', ' ', str(text)).strip().split(' ')
        out = []
        for w in words:
            core = w.strip(',')
            if re.fullmatch(r'[A-Za-z]', core):
                nw = w.replace(core, core.upper())
            elif re.fullmatch(r'\d+[A-Za-z]+', core):
                num = re.match(r'\d+', core).group(0)
                tail_raw = core[len(num):]
                if tail_raw.upper() in { 'ST', 'ND', 'RD', 'TH' }:
                    tail = tail_raw.lower()
                else:
                    tail = tail_raw.upper()
                nw = w.replace(core, num + tail)
            else:
                nw = w[:1].upper() + w[1:].lower() if len(w) > 1 else w.upper()
            out.append(nw)
        return ' '.join(out)
    
    def _geocode_address(self, idx: int, df: pd.DataFrame, 
                        address: str, city: str, state: str, 
                        zip_code: str, notes: List[str]) -> None:
        result = self.geocoding_service.verify_address(address, city, state, zip_code)
        
        df.loc[idx, 'VerifiedAddress'] = result['verified_address']
        df.loc[idx, 'AddressConfidence'] = result['confidence']
        df.loc[idx, 'Latitude'] = result['latitude']
        df.loc[idx, 'Longitude'] = result['longitude']
        
        if not result['verified']:
            notes.append(f"Address verification: {result['confidence']}")
    
    @staticmethod
    def _to_title_case(text: str) -> str:
        if not text or pd.isna(text) or str(text).upper() == 'NULL':
            return ''
        
        text = re.sub(r'\s+', ' ', str(text)).strip()
        words = text.split()
        result = []
        
        for word in words:
            if word:
                if word.lower() in ['of', 'the', 'and', 'or', 'in', 'at', 'for'] and len(result) > 0:
                    result.append(word.lower())
                else:
                    result.append(word[0].upper() + word[1:].lower() if len(word) > 1 else word.upper())
        
        return ' '.join(result)
    
    @staticmethod
    def _clean_address_with_pound(address: str) -> str:
        if not address or pd.isna(address) or str(address).upper() == 'NULL':
            return ''
        return re.sub(r'\s+', ' ', str(address)).strip()


def main(enable_geocoding: bool = False):
    if not os.path.exists(INPUT_FILE):
        print(f"Input file '{INPUT_FILE}' not found!")
        return 1
    
    df = pd.read_excel(INPUT_FILE)
    active_df = df[df['Active'] == 1].copy()
    
    print(f"Processing {len(active_df)} active records...")
    
    cleaner = HospitalDataCleaner()
    cleaned_df = cleaner.clean_data(active_df, enable_geocoding)
    
    output_columns = [
        'ClinicKey', 'HospitalKey',
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
    
    output_df = cleaned_df[output_columns].copy()
    output_df.columns = output_df.columns.str.replace('Cleaned', '')
    
    for col in ['Phone', 'Facimile', 'ZIP']:
        if col in output_df.columns:
            output_df[col] = output_df[col].apply(
                lambda x: (re.sub(r'\D', '', str(x)) if (pd.notna(x) and str(x).strip() != '') else '')
            )
    
    output_file = OUTPUT_FILE_WITH_GEO if enable_geocoding else OUTPUT_FILE
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        output_df.to_excel(writer, sheet_name='Cleaned Data', index=False)
    
    print(f"Output saved to {output_file}")
    return 0


if __name__ == "__main__":
    enable_geocoding = '--geocode' in sys.argv or '-g' in sys.argv
    
    if enable_geocoding:
        print("Geocoding enabled. This will take ~1.1 seconds per address.")
    
    sys.exit(main(enable_geocoding))