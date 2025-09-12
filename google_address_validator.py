#!/usr/bin/env python3
"""
Google Maps Address Validator (Standalone)
=========================================
Direct wrapper around Google Geocoding API with rate limiting and
consistent result formatting. No third-party googlemaps client required.

Environment variables:
- GOOGLE_MAPS_API_KEY: API key for Geocoding API (preferred)
If the environment variable is not set and no api_key is passed, a default
fallback key will be used.

Public class mirrors expected interface used by the main processor:
- GoogleMapsValidator(api_key: Optional[str] = None, requests_per_second: float = 50)
- validate_address(address, city, state, zip_code) -> Dict
- get_statistics() -> Dict
"""

import os
import time
import re
from typing import Dict, Optional
import requests
from difflib import SequenceMatcher


class GoogleMapsValidator:
    """Handles Google Maps Geocoding for address validation with rate limiting."""

    GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

    DEFAULT_GOOGLE_MAPS_API_KEY = "AIzaSyDYnvtK34251XIiG-13OOtHzZZVB8kV5wA"

    def __init__(self, api_key: Optional[str] = None, requests_per_second: float = 50.0, request_timeout_seconds: float = 10.0):
        # Always use the default project key as requested
        self.api_key = self.DEFAULT_GOOGLE_MAPS_API_KEY
        self.sleep_between_calls = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.timeout = request_timeout_seconds

        self.enabled = bool(self.api_key)
        self.stats = {
            'total': 0,
            'verified': 0,
            'corrected': 0,
            'invalid': 0,
            'errors': 0
        }

    def validate_address(self, address: str, city: str, state: str, zip_code: str) -> Dict:
        """Validate address using Google Geocoding API.

        Returns a dict with keys: original, formatted, is_valid, status, confidence,
        was_corrected, components, latitude, longitude, location_type
        """
        self.stats['total'] += 1

        full_address = f"{address}, {city}, {state} {zip_code}, USA".strip(', ')

        if not self.enabled:
            self.stats['invalid'] += 1
            return self._error_result("Google Maps API key missing. Set GOOGLE_MAPS_API_KEY.")

        # Retry with exponential backoff on transient errors and quota issues
        params = {
            'address': full_address,
            'key': self.api_key,
            'components': 'country:US'
        }
        backoff = self.sleep_between_calls if self.sleep_between_calls > 0 else 0.02
        max_backoff = 2.0
        attempts = 0
        max_attempts = 5
        last_error = None

        while attempts < max_attempts:
            attempts += 1
            try:
                response = requests.get(self.GEOCODE_URL, params=params, timeout=self.timeout)
                # base throttle
                time.sleep(self.sleep_between_calls)

                if response.status_code != 200:
                    last_error = f"HTTP {response.status_code}"
                    # transient? backoff and retry
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue

                data = response.json()
                status = data.get('status', 'UNKNOWN_ERROR')

                if status == 'OK' and data.get('results'):
                    result = data['results'][0]
                    parsed = self._parse_result(result, full_address)
                    if parsed['is_valid']:
                        self.stats['verified'] += 1
                        if parsed['was_corrected']:
                            self.stats['corrected'] += 1
                    else:
                        self.stats['invalid'] += 1
                    return parsed

                # Handle common API responses
                if status in ('OVER_DAILY_LIMIT', 'OVER_QUERY_LIMIT'):
                    last_error = status
                    # aggressive backoff but try a few times
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue
                if status in ('REQUEST_DENIED', 'INVALID_REQUEST'):
                    self.stats['errors'] += 1
                    return self._error_result(status)

                # ZERO_RESULTS or others treated as invalid
                self.stats['invalid'] += 1
                return self._invalid_result(full_address)

            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        # Exceeded retries
        self.stats['errors'] += 1
        return self._error_result(last_error or 'Unknown error')

    def _parse_result(self, result: Dict, original: str) -> Dict:
        formatted = result.get('formatted_address', '')
        geometry = result.get('geometry', {})
        location = geometry.get('location', {'lat': None, 'lng': None})
        location_type = geometry.get('location_type', 'APPROXIMATE')
        components = result.get('address_components', [])

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
                parsed['street_number'] = comp.get('long_name', '')
            elif 'route' in types:
                parsed['street'] = comp.get('long_name', '')
            elif 'locality' in types:
                parsed['city'] = comp.get('long_name', '')
            elif 'administrative_area_level_1' in types:
                parsed['state'] = comp.get('short_name', '')
            elif 'postal_code' in types:
                parsed['zip'] = comp.get('long_name', '')

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
            'latitude': location.get('lat'),
            'longitude': location.get('lng'),
            'location_type': location_type
        }

    def _invalid_result(self, address: str) -> Dict:
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
        return self.stats.copy()