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
    from axiom.drs.processing.ccam import has_height, has_height_attr
    AXIOM_CONFIG = load_config('drs_20i')
except ImportError:
    print("⚠️  Warning: Could not load axiom.config. Falling back to default encoding.")
    AXIOM_CONFIG = None

def process_files(root_dir, dry_run=True):
    if dry_run:
        print(f"--- DRY RUN MODE ACTIVE: No files will be modified in {root_dir} ---")
    else:
        print(f"--- EXECUTION MODE: Daily files will be modified in {root_dir} ---")

    # Regex to extract the date range for monthly files (e.g., 202701-202712)
    date_pattern = re.compile(r'([0-9]{4})[0-9]{2}-([0-9]{4})[0-9]{2}')

    # Walk through the directory tree
    for dirpath, _, filenames in os.walk(root_dir):
        # Filter for .nc files, excluding backups
        nc_files = [f for f in filenames if f.endswith('.nc') and not f.endswith('.bak')]

        # Create a 'processed' directory in the current path if not in dry run
        processed_dir = os.path.join(dirpath, "processed")

        for filename in nc_files:
            file_path = os.path.join(dirpath, filename)
            
            match = date_pattern.search(filename)
            if not match:
                continue

            start_year = int(match.group(1))
            end_year = int(match.group(2))

            # Identify Single Year Files (Source)
            if start_year == end_year:
                source_year = start_year
                source_file = file_path
                
                # Extract Prefix (removes the date and .nc to find the target multi-year file)
                prefix = filename[:match.start()]
                variable_id = filename.split('_')[0]

                target_file = None

                # Search for the multi-year target in the same directory
                for t_filename in nc_files:
                    if t_filename == filename:
                        continue
                    if not t_filename.startswith(prefix):
                        continue
                    
                    t_match = date_pattern.search(t_filename)
                    if not t_match:
                        continue
                        
                    t_start_year = int(t_match.group(1))
                    t_end_year = int(t_match.group(2))
                    
                    if t_start_year != t_end_year and t_start_year <= source_year <= t_end_year:
                        target_file = os.path.join(dirpath, t_filename)
                        break
                
                # If a target is found and the single-year source file is newer
                #if target_file and os.path.getmtime(source_file) > os.path.getmtime(target_file):
                # If a matching target file is found, force the update
                if target_file:
                    temp_file = target_file + ".tmp"
                    bak_file = target_file + ".bak"

                    if dry_run:
                        print("-" * 48)
                        print(f"DRY RUN (MONTHLY): Found update for {os.path.basename(target_file)}")
                        print(f"           Source: {os.path.basename(source_file)}")
                    else:
                        print("-" * 48)
                        print(f"PROCESSING MONTHLY: {os.path.basename(target_file)}")
                        
                        # Create Backup
                        if not os.path.exists(bak_file):
                            shutil.copy2(target_file, bak_file)
                        
                        try:
                            print(f"   - Replacing year {source_year} data using xarray...")
                            
                            # Open datasets
                            ds_target = xr.open_dataset(target_file, decode_cf=True)
                            ds_source = xr.open_dataset(source_file, decode_cf=True)
                            
                            # Capture original time encoding to prevent degradation
                            has_time = 'time' in ds_target.coords
                            input_time_units = ds_target.time.encoding.get('units') if has_time else None
                            input_time_calendar = ds_target.time.encoding.get('calendar') if has_time else None

                            # Step 1: Remove the old data for that specific year from the multi-year file
                            ds_target_filtered = ds_target.sel(time=ds_target['time'].dt.year != source_year)

                            # Step 2: Merge the filtered target with the new 1-year file
                            # Use minimal concatenation to protect 'crs' and other 0-D variables
                            ds_merged = xr.concat(
                                [ds_target_filtered, ds_source], 
                                dim='time', 
                                data_vars='minimal', 
                                coords='minimal', 
                                compat='override'
                            ).sortby('time')

                            # Explicitly retain original global attributes (concat can strip them)
                            ds_merged.attrs = ds_target.attrs.copy()

                            # remove height variable as a scalar coordinate
                            _has_height, hcoord = has_height(ds_merged, variable_id)
                            if _has_height:
                                ds_merged = ds_merged.reset_coords(hcoord, drop=False)
                                ds_merged[variable_id].attrs["coordinates"] = hcoord

                            # --- Encoding Logic ---
                            encoding = {}
                            if AXIOM_CONFIG and 'variables' in AXIOM_CONFIG.encoding:
                                encoding[variable_id] = AXIOM_CONFIG.encoding['variables'].copy()
                                encoding[variable_id]['dtype'] = 'float32'
                                if '_FillValue' in encoding[variable_id]:
                                    encoding[variable_id]['missing_value'] = encoding[variable_id]['_FillValue']
                            else:
                                encoding[variable_id] = {'dtype': 'float32', '_FillValue': 1e20, 'missing_value': 1e20}

                            # Update height scalar coordinate encoding
                            _has_height, hcoord = has_height_attr(ds_merged, variable_id)
                            if _has_height:
                                encoding[hcoord] = AXIOM_CONFIG.encoding[hcoord]

                            # Coordinates and Bounds (filtered for existence)
                            for v in ['time', 'lat', 'lon', 'time_bnds', 'lat_bnds', 'lon_bnds']:
                                if v in ds_merged.variables or v in ds_merged.coords:
                                    enc = AXIOM_CONFIG.encoding.get(v, {}).copy() if AXIOM_CONFIG else {}
                                    enc['dtype'] = 'float64'
                                    
                                    # Only apply time-specific encoding if time actually exists
                                    if 'time' in v and has_time:
                                        if input_time_units: enc['units'] = input_time_units
                                        if input_time_calendar: enc['calendar'] = input_time_calendar
                                    encoding[v] = enc
                                            
                            # Write temp file
                            write_args = {
                                "format": 'NETCDF4_CLASSIC',
                                "encoding": encoding,
                            }
                            
                            if has_time and 'time' in ds_merged.dims:
                                write_args["unlimited_dims"] = ['time']

                            # remove coordinates encoding if it is part of the variable
                            if 'coordinates' in ds_merged[variable_id].encoding:
                                del ds_merged[variable_id].encoding['coordinates']

                            ds_merged.to_netcdf(temp_file, **write_args)

                            # Safely close all handles before file operations
                            ds_target.close()
                            ds_source.close()
                            ds_target_filtered.close()
                            ds_merged.close()

                            # Overwrite target with the new updated file
                            os.replace(temp_file, target_file)
                            
                            # --- MOVE SOURCE FILE AFTER SUCCESS ---
                            if not os.path.exists(processed_dir):
                                os.makedirs(processed_dir)
                            
                            shutil.move(source_file, os.path.join(processed_dir, os.path.basename(source_file)))
                            
                            print(f"   - SUCCESS: Updated {os.path.basename(target_file)}")
                            print(f"   - MOVED: {os.path.basename(source_file)} to processed/")

                        except Exception as e:
                            print(f"   - ERROR: Failed for {os.path.basename(target_file)}: {e}")
                            if os.path.exists(temp_file):
                                os.remove(temp_file)

    print("-" * 48)
    print("Monthly Data Process Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concatenate and manage NetCDF files.")
    parser.add_argument("base_dir", help="Base directory of the model to process")
    parser.add_argument("--run", action="store_true", help="Execute the changes (default is Dry Run)")
    
    args = parser.parse_args()
    
    process_files(root_dir=args.base_dir, dry_run=not args.run)
