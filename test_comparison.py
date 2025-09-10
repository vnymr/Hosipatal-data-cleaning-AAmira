#!/usr/bin/env python3
"""Test script to compare outputs from both cleanup scripts"""

import pandas as pd
import numpy as np

# Load both output files
df_original = pd.read_excel('Cleaned_Hospital_Data.xlsx')
df_minimal = pd.read_excel('Cleaned_Hospital_Data_minimal.xlsx')

print("=" * 60)
print("COMPARISON REPORT")
print("=" * 60)

# Check dimensions
print(f"\nDimensions:")
print(f"Original: {df_original.shape}")
print(f"Minimal:  {df_minimal.shape}")

# Check columns
print(f"\nColumns match: {list(df_original.columns) == list(df_minimal.columns)}")
if list(df_original.columns) != list(df_minimal.columns):
    print("Column differences:")
    print(f"  In original but not minimal: {set(df_original.columns) - set(df_minimal.columns)}")
    print(f"  In minimal but not original: {set(df_minimal.columns) - set(df_original.columns)}")

# Compare data values
print("\nData comparison:")
differences = []
for col in df_original.columns:
    if col in df_minimal.columns:
        # Handle NaN values properly
        orig_vals = df_original[col].fillna('__NULL__')
        min_vals = df_minimal[col].fillna('__NULL__')
        
        if not orig_vals.equals(min_vals):
            # Count differences
            diff_mask = orig_vals != min_vals
            num_diffs = diff_mask.sum()
            differences.append((col, num_diffs))
            
            if num_diffs <= 5:  # Show examples for small differences
                print(f"\n  {col}: {num_diffs} differences")
                diff_indices = df_original.index[diff_mask].tolist()[:5]
                for idx in diff_indices:
                    print(f"    Row {idx}: '{orig_vals.iloc[idx]}' vs '{min_vals.iloc[idx]}'")

if not differences:
    print("  ✓ All data values match perfectly!")
else:
    print(f"\n  Found differences in {len(differences)} columns:")
    for col, count in differences:
        print(f"    - {col}: {count} differences")

# Check specific transformations
print("\n" + "=" * 60)
print("SPOT CHECKS")
print("=" * 60)

# Check a few random rows
sample_indices = [0, 100, 500, 1000, 1400]
print("\nSample row checks:")
for idx in sample_indices:
    if idx < len(df_original):
        print(f"\nRow {idx}:")
        print(f"  Hospital: {df_original.loc[idx, 'HospitalName']}")
        print(f"  Address:  {df_original.loc[idx, 'AddressOne']}")
        print(f"  Phone:    {df_original.loc[idx, 'Phone']}")
        print(f"  ZIP:      {df_original.loc[idx, 'ZIP']}")
        
        # Check if minimal matches
        matches = all([
            df_original.loc[idx, col] == df_minimal.loc[idx, col] or 
            (pd.isna(df_original.loc[idx, col]) and pd.isna(df_minimal.loc[idx, col]))
            for col in ['HospitalName', 'AddressOne', 'Phone', 'ZIP']
        ])
        print(f"  Matches:  {'✓' if matches else '✗'}")

# Check phone number formatting
print("\nPhone number format check:")
phone_original = df_original['Phone'].dropna()
phone_minimal = df_minimal['Phone'].dropna()
print(f"  Original: {len(phone_original)} non-empty phones")
print(f"  Minimal:  {len(phone_minimal)} non-empty phones")

# Check all phones are digits-only
orig_digits_only = phone_original.apply(lambda x: str(x).isdigit() if x else True).all()
min_digits_only = phone_minimal.apply(lambda x: str(x).isdigit() if x else True).all()
print(f"  Original all digits: {orig_digits_only}")
print(f"  Minimal all digits:  {min_digits_only}")

# Check ZIP format
print("\nZIP code format check:")
zip_original = df_original['ZIP'].dropna()
zip_minimal = df_minimal['ZIP'].dropna()
print(f"  Original: {len(zip_original)} non-empty ZIPs")
print(f"  Minimal:  {len(zip_minimal)} non-empty ZIPs")

# Check all ZIPs are 5 digits
orig_5_digits = zip_original.apply(lambda x: len(str(x)) == 5 and str(x).isdigit()).all()
min_5_digits = zip_minimal.apply(lambda x: len(str(x)) == 5 and str(x).isdigit()).all()
print(f"  Original all 5-digit: {orig_5_digits}")
print(f"  Minimal all 5-digit:  {min_5_digits}")

# Validation flags
print("\nValidation flags comparison:")
for flag in ['StateValid', 'ZipValid', 'PhoneValid', 'FaxValid']:
    if flag in df_original.columns and flag in df_minimal.columns:
        orig_counts = df_original[flag].value_counts()
        min_counts = df_minimal[flag].value_counts()
        print(f"  {flag}:")
        print(f"    Original: Y={orig_counts.get('Y', 0)}, N={orig_counts.get('N', 0)}")
        print(f"    Minimal:  Y={min_counts.get('Y', 0)}, N={min_counts.get('N', 0)}")

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)