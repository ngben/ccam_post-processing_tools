#!/usr/bin/env python3

import os
import re
import shutil
import sys
import warnings
import xarray as xr
import argparse

# Suppress DeprecationWarnings from newer Xarray versions regarding use_cftime
warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- Environment & Configuration ---
sys.path.append("/g/data/xv83/users/bxn599/miniconda3/envs/axiom_20i_test/lib/python3.12/site-packages")
try:
    from axiom.config import load_config
    AXIOM_CONFIG = load_config('drs_20i')
except ImportError:
    print("⚠️  Warning: Could not load axiom.config. Falling back to default encoding.")
    AXIOM_CONFIG = None

# --- CONFIGURATION ---
parser = argparse.ArgumentParser(description="Concatenate NetCDF files.")
parser.add_argument("base_dir", help="Base directory of the model to process")
parser.add_argument("--run", action="store_true", help="Execute changes (default is Dry Run)")
args = parser.parse_args()

BASE_DIR = args.base_dir  # Now it takes the path from the command line
BACKUP_BASE_DIR = "/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year"
DRY_RUN = not args.run  # If --run is not provided, DRY_RUN is True
# ---------------------

def get_unique_years(dir_path):
    """Extracts a sorted list of unique years from 1-year daily files in the directory."""
    years = set()
    # Matches files like _YYYY0101-YYYY1231.nc
    pattern = re.compile(r'_(\d{4})0101-\1\d{4}\.nc')
    for filename in os.listdir(dir_path):
        match = pattern.search(filename)
        if match:
            years.add(int(match.group(1)))
    return sorted(list(years))

def concatenate_files(start_year, end_year, dir_path, backup_dir):
    source_files = []
    
    # Gather all 1-year files in the target range
    for year in range(start_year, end_year + 1):
        # We look for the specific 1-year daily format starting Jan 1st
        pattern = re.compile(rf'_({year}0101-{year}\d{{4}})\.nc$')
        for filename in os.listdir(dir_path):
            if pattern.search(filename):
                source_files.append(os.path.join(dir_path, filename))
                break # Move to next year once found

    # If there is only 1 (or 0) files, there is nothing to concatenate! Skip it.
    if len(source_files) <= 1:
        if DRY_RUN and len(source_files) == 1:
            print(f"   - Skipping: Only 1 file found ({os.path.basename(source_files[0])}). Nothing to concatenate.")
        return

    # Sort files chronologically
    source_files.sort()
    
    # Construct output filename
    first_filename = os.path.basename(source_files[0])
    last_filename = os.path.basename(source_files[-1])
    
    # Extract prefix (everything before the date range)
    prefix_match = re.match(r'^(.*?)_\d{8}-\d{8}\.nc$', first_filename)
    if not prefix_match:
        print(f"   - ERROR: Could not parse prefix from {first_filename}")
        return
        
    prefix = prefix_match.group(1)
    variable_id = prefix.split('_')[0]
    
    # Extract accurate start and end dates directly from the files
    first_date_str = re.search(r'_(\d{8})-\d{8}\.nc$', first_filename).group(1)
    last_date_str = re.search(r'_\d{8}-(\d{8})\.nc$', last_filename).group(1)
    
    output_filename = f"{prefix}_{first_date_str}-{last_date_str}.nc"
    output_filepath = os.path.join(dir_path, output_filename)
    temp_filepath = output_filepath + ".tmp"

    # Dynamically extract Model and Scenario from the BASE_DIR path
    from pathlib import Path
    p = Path(BASE_DIR)
    try:
        idx = p.parts.index('CSIRO')
        model_name = p.parts[idx+1]  # e.g., ACCESS-CM2
        scenario = p.parts[idx+2]    # e.g., historical
    except ValueError:
        model_name = "unknown_model"
        scenario = "unknown_scenario"

    # Determine backup path maintaining model/scenario directory structure
    rel_path = os.path.relpath(dir_path, BASE_DIR)
    target_backup_dir = os.path.join(backup_dir, model_name, scenario, rel_path)

    if DRY_RUN:
        print("-" * 48)
        print(f"DRY RUN: Would concatenate {len(source_files)} files.")
        print(f"         From: {os.path.basename(source_files[0])}")
        print(f"         To:   {os.path.basename(source_files[-1])}")
        print(f"         Out:  {output_filename}")
        print(f"         Backup to: {target_backup_dir}")
        return

    print("-" * 48)
    print(f"PROCESSING: Creating {output_filename}")

    try:
        # Load all datasets into memory lazily
        datasets = [xr.open_dataset(f, decode_cf=True) for f in source_files]
        
        # Capture original time encoding from the first file
        ds_ref = datasets[0]
        has_time = 'time' in ds_ref.coords
        input_time_units = ds_ref.time.encoding.get('units') if has_time else None
        input_time_calendar = ds_ref.time.encoding.get('calendar') if has_time else None

        # Concatenate
        print("   - Merging files with xarray...")
        ds_merged = xr.concat(
            datasets, 
            dim='time', 
            data_vars='minimal', 
            coords='minimal', 
            compat='override'
        )
        
        # Ensure sequential time
        ds_merged = ds_merged.sortby('time')

        # Retain original global attributes
        ds_merged.attrs = ds_ref.attrs.copy()

        # --- Apply Exact Output Encoding ---
        encoding = {}
        
        # Base Data Variable
        if AXIOM_CONFIG and 'variables' in AXIOM_CONFIG.encoding:
            encoding[variable_id] = AXIOM_CONFIG.encoding['variables'].copy()
            encoding[variable_id]['dtype'] = 'float32'
            if '_FillValue' in encoding[variable_id]:
                encoding[variable_id]['missing_value'] = encoding[variable_id]['_FillValue']
        else:
            encoding[variable_id] = {'dtype': 'float32', '_FillValue': 1e20, 'missing_value': 1e20}

        # Coordinates and Bounds
        for v in ['time', 'lat', 'lon', 'time_bnds', 'lat_bnds', 'lon_bnds']:
            if v in ds_merged.variables or v in ds_merged.coords:
                enc = AXIOM_CONFIG.encoding.get(v, {}).copy() if AXIOM_CONFIG else {}
                enc['dtype'] = 'float64'
                
                if 'time' in v:
                    if has_time:
                        if input_time_units: enc['units'] = input_time_units
                        if input_time_calendar: enc['calendar'] = input_time_calendar
                    else:
                        continue
                        
                encoding[v] = enc

        # --- Safe Write Operations ---
        print("   - Saving new concatenated NetCDF...")
        write_args = {
            "format": 'NETCDF4_CLASSIC',
            "encoding": encoding,
        }
        
        if has_time and 'time' in ds_merged.dims:
            write_args["unlimited_dims"] = ['time']

        ds_merged.to_netcdf(temp_filepath, **write_args)

        # Safely close all opened datasets to free memory/file handles
        for ds in datasets:
            ds.close()
        ds_merged.close()

        # Rename temp to final
        os.replace(temp_filepath, output_filepath)
        print("   - SUCCESS: File saved.")

        # Move source files to backup
        os.makedirs(target_backup_dir, exist_ok=True)
        for src_file in source_files:
            shutil.move(src_file, os.path.join(target_backup_dir, os.path.basename(src_file)))
        print(f"   - Moved {len(source_files)} source files to backup.")

    except Exception as e:
        print(f"   - ERROR: xarray merge failed during {start_year}-{end_year} concatenation.")
        print(f"            Details: {e}")
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        
        # Ensure files are closed even on failure
        if 'datasets' in locals():
            for ds in datasets:
                ds.close()


def main():
    if DRY_RUN:
        print("--- DRY RUN MODE ACTIVE: No files will be modified ---")
    else:
        print("--- EXECUTION MODE: Backups will be created and files modified ---")

    day_base_dir = os.path.join(BASE_DIR, "day")

    if not os.path.exists(day_base_dir):
        print(f"Directory not found: {day_base_dir}")
        sys.exit(1)

    # Walk through the base directory looking for variables/versions
    for dirpath, dirnames, filenames in os.walk(day_base_dir):
        # We only process directories that contain .nc files
        nc_files = [f for f in filenames if f.endswith('.nc')]
        if not nc_files:
            continue
            
        years = get_unique_years(dirpath)
        if not years:
            continue

        print(f"\nProcessing directory: {dirpath}")
        print(f"Frequency: day")
        print(f"Years found: {years}")

        span = 5
        processed_years = set()

        # Find starting years that end in '1' or '6' (e.g., 2001, 2006, 2011)
        start_years = [y for y in years if str(y).endswith('1') or str(y).endswith('6')]
        
        # If no years end in 1 or 6, start from the earliest year
        if not start_years:
            start_years = [years[0]]
            
        print(f"Start years: {start_years}")

        for start_year in start_years:
            end_year = start_year + span - 1
            if end_year > years[-1]:
                end_year = years[-1]

            # Verify we actually have years in this range before attempting
            years_in_range = [y for y in years if start_year <= y <= end_year]
            if not years_in_range:
                continue

            print(f"Concatenating files from {start_year} to {end_year}...")
            concatenate_files(start_year, end_year, dirpath, BACKUP_BASE_DIR)

            for y in years_in_range:
                processed_years.add(y)

        # Handle remaining years
        remaining_years = [y for y in years if y not in processed_years]
        
        if remaining_years:
            print(f"Remaining years: {remaining_years}")
            start_year = remaining_years[0]
            end_year = remaining_years[-1]
            
            print(f"Concatenating remaining files from {start_year} to {end_year}...")
            concatenate_files(start_year, end_year, dirpath, BACKUP_BASE_DIR)

    print("-" * 48)
    print("Done.")

if __name__ == "__main__":
    main()