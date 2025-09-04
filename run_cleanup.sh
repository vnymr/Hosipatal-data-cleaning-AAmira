#!/bin/bash

# Hospital Data Cleanup Script Runner
# Makes it easy to run the cleanup with various options

echo "=========================================="
echo "Hospital Data Cleanup System v2.0"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install requirements if needed
echo "Checking dependencies..."
pip install -q -r requirements.txt 2>/dev/null || pip install -q pandas openpyxl requests

# Check if input file exists
if [ ! -f "Concierge Hospitals.xlsx" ]; then
    echo "ERROR: Input file 'Concierge Hospitals.xlsx' not found!"
    echo "Please place your Excel file in the current directory."
    exit 1
fi

# Ask user for mode
echo "Select running mode:"
echo "1) Fast mode (no geocoding) - ~10 seconds"
echo "2) Full mode with address verification - ~26 minutes"
echo "3) Test with 5 sample records"
echo ""
read -p "Enter choice (1-3): " choice

case $choice in
    1)
        echo ""
        echo "Running in fast mode..."
        python hospital_data_cleanup_v2.py
        ;;
    2)
        echo ""
        echo "Running with address verification..."
        echo "This will take approximately 26 minutes for 1400 records."
        read -p "Continue? (y/n): " confirm
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            python hospital_data_cleanup_v2.py --geocode
        else
            echo "Cancelled."
        fi
        ;;
    3)
        echo ""
        echo "Running test with sample data..."
        python test_with_geocoding.py
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Process complete!"
echo "Check the following output files:"
echo "- Cleaned_Hospital_Data.xlsx"
echo "- Cleanup_Report.json" 
echo "- Validation_Issues.csv"
echo "=========================================="