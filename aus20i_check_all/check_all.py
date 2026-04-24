import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
import argparse
import sys
import warnings
import os
import shutil

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

# --- Configuration Constants ---
EXPECTED_VAR_LISTS = {
    "1hr": [
        "clivi", "clt", "clwvi", "hfls", "hfss", "hurs", "huss", "mrfsos", "mrsos", 
        "pr", "prc", "prsn", "prw", "ps", "psl", "rlds", "rlus", "rlut", "rsds", 
        "rsdsdir", "rsdt", "rsus", "rsut", "sfcWind", "tas", "tauu", "tauv", "ts", "ua100m", "ua150m", 
        "ua50m", "uas", "va100m", "va150m", "va50m", "vas", "zmla"
    ],
    "6hr": [
        "CAPE", "CIN", "LI", "evspsbl", "hus1000", "hus200", "hus250", "hus300", 
        "hus400", "hus500", "hus600", "hus700", "hus850", "hus925", "mrfso", "mrro", 
        "mrros", "mrsfl", "mrso", "mrsol", "snc", "snd", "snm", "snw", "ta1000", "ta200", 
        "ta250", "ta300", "ta400", "ta500", "ta600", "ta700", "ta850", "ta925", 
        "tauu", "tauv", "tsl", "ua1000", "ua200", "ua250", "ua300", "ua400", 
        "ua500", "ua600", "ua700", "ua850", "ua925", "va1000", "va200", "va250", 
        "va300", "va400", "va500", "va600", "va700", "va850", "va925", "wa1000", 
        "wa200", "wa250", "wa300", "wa400", "wa500", "wa600", "wa700", "wa850", 
        "wa925", "zg1000", "zg200", "zg250", "zg300", "zg400", "zg500", "zg600", 
        "zg700", "zg850", "zg925"
    ],
    "day": [
        "CAPE", "CIN", "LI", "clivi", "clt", "clwvi", "evspsbl", "hfls", "hfss", 
        "hurs", "hus1000", "hus200", "hus250", "hus300", "hus400", "hus500", 
        "hus600", "hus700", "hus850", "hus925", "huss", "mrfso", "mrfsos", "mrro", 
        "mrros", "mrsfl", "mrso", "mrsol", "mrsos", "pr", "prc", "prhmax", "prsn", 
        "prw", "ps", "psl", "rlds", "rlus", "rlut", "rsds", "rsdsdir", "rsdt", 
        "rsus", "rsut", "sfcWind", "sfcWindmax", "siconca", "snc", "snd", "snm", 
        "snw", "sund", "ta1000", "ta200", "ta250", "ta300", "ta400", "ta500", 
        "ta600", "ta700", "ta850", "ta925", "tas", "tasmax", "tasmin", "tauu", 
        "tauv", "ts", "tsl", "ua1000", "ua100m", "ua150m", "ua200", "ua250", 
        "ua300", "ua400", "ua500", "ua50m", "ua600", "ua700", "ua850", "ua925", "uas", 
        "va1000", "va100m", "va150m", "va200", "va250", "va300", "va400", "va500", 
        "va50m", "va600", "va700", "va850", "va925", "vas", "wa1000", "wa200", 
        "wa250", "wa300", "wa400", "wa500", "wa600", "wa700", "wa850", "wa925", 
        "zg1000", "zg200", "zg250", "zg300", "zg400", "zg500", "zg600", "zg700", 
        "zg850", "zg925", "zmla"
    ],
    "mon": [
        "CAPE", "CIN", "LI", "clivi", "clt", "clwvi", "evspsbl", "hfls", "hfss", 
        "hurs", "hus1000", "hus200", "hus250", "hus300", "hus400", "hus500", 
        "hus600", "hus700", "hus850", "hus925", "huss", "mrfso", "mrfsos", "mrro", 
        "mrros", "mrsfl", "mrso", "mrsol", "mrsos", "pr", "prc", "prhmax", "prsn", 
        "prw", "ps", "psl", "rlds", "rlus", "rlut", "rsds", "rsdsdir", "rsdt", 
        "rsus", "rsut", "sfcWind", "sfcWindmax", "siconca", "snc", "snd", "snm", 
        "snw", "sund", "ta1000", "ta200", "ta250", "ta300", "ta400", "ta500", 
        "ta600", "ta700", "ta850", "ta925", "tas", "tasmax", "tasmin", "tauu", 
        "tauv", "ts", "tsl", "ua1000", "ua100m", "ua150m", "ua200", "ua250", 
        "ua300", "ua400", "ua500", "ua50m", "ua600", "ua700", "ua850", "ua925", "uas", 
        "va1000", "va100m", "va150m", "va200", "va250", "va300", "va400", "va500", 
        "va50m", "va600", "va700", "va850", "va925", "vas", "wa1000", "wa200", 
        "wa250", "wa300", "wa400", "wa500", "wa600", "wa700", "wa850", "wa925", 
        "zg1000", "zg200", "zg250", "zg300", "zg400", "zg500", "zg600", "zg700", 
        "zg850", "zg925", "zmla"
    ],
    "fx": ["orog", "sftlf"],
}

EXPECTED_FILES = {
    ("historical", "day"): 13, ("historical", "mon"): 7,
    ("ssp126", "day"): 18, ("ssp126", "mon"): 9,
    ("ssp370", "day"): 18, ("ssp370", "mon"): 9,
    ("evaluation", "day"): 9, ("evaluation", "mon"): 5,
    ("historical", "1hr"): 64, ("historical", "6hr"): 64,
    ("ssp126", "1hr"): 85, ("ssp126", "6hr"): 85,
    ("ssp370", "1hr"): 85, ("ssp370", "6hr"): 85,
    ("evaluation", "1hr"): 41, ("evaluation", "6hr"): 41,
    ("historical", "fx"): 1, ("ssp126", "fx"): 1,
    ("ssp370", "fx"): 1, ("evaluation", "fx"): 1,
}

EXPECTED_GLOBAL_ATTRIBUTES = {
    "Conventions": "CF-1.11",
    "source": "Conformal Cubic Atmospheric Model v2203",
    "institution": "Commonwealth Scientific and Industrial Research Organisation, Canberra, Australia",
    "activity_id": "DD",
    "contact": "ccam@csiro.au",
    "domain": "Australasia",
    "domain_id": "AUS-20i",
    "institution_id": "CSIRO",
    "license": "https://cordex.org/data-access/cordex-cmip6-data/cordex-cmip6-terms-of-use",
    "mip_era": "CMIP6",
    "grid": "Unrotated latitude/longitude grid interpolated from a variable resolution conformal cubic C384 grid with Schmidt=2.1",
    "product": "model-output",
    "project_id": "CORDEX-CMIP6",
    "source_id": "CCAM-v2203-SN",
    "source_type": "AOGCM",
    "version_realization": "v1-r1",
    "doi": "https://doi.org/10.25914/rd73-4m38",
}

EXPECTED_CALENDAR = {
    "ERA5": "proleptic_gregorian", "ACCESS-CM2": "proleptic_gregorian",
    "ACCESS-ESM1-5": "proleptic_gregorian", "CESM2": "noleap",
    "CMCC-ESM2": "noleap", "CNRM-ESM2-1": "proleptic_gregorian",
    "EC-Earth3": "noleap", "NorESM2-MM": "noleap",
}

EXPECTED_TIME_UNITS = "days since 1950-01-01"

EXPECTED_DRIVING_VARIANT_LABEL = {
    "ERA5": "r1i1p1f1", "ACCESS-CM2": "r4i1p1f1", "ACCESS-ESM1-5": "r6i1p1f1",
    "CESM2": "r11i1p1f1", "CMCC-ESM2": "r1i1p1f1", "CNRM-ESM2-1": "r1i1p1f2",
    "EC-Earth3": "r1i1p1f1", "NorESM2-MM": "r1i1p1f1",
}

EXPECTED_DRIVING_INSTITUTION_ID = {
    "ERA5": "ECMWF",  "ACCESS-CM2": "CSIRO-ARCCSS", "ACCESS-ESM1-5": "CSIRO",
    "CESM2": "NCAR", "CMCC-ESM2": "CMCC", "CNRM-ESM2-1": "CNRM-CERFACS",
    "EC-Earth3": "EC-Earth-Consortium", "NorESM2-MM": "NCC",
}

EXPECTED_DRIVING_EXPERIMENT = {
    "evaluation": "reanalysis simulation of the recent past",
    "historical": "all-forcing simulation of the recent past",
    "ssp126": "update of RCP2.6 based on SSP1",
    "ssp370": "gap-filling scenario reaching 7.0 based on SSP3",
    "ssp585": "update of RCP8.5 based on SSP5",
}

DATASET_TABLE = None

# --- Helper & Checking Functions ---
def load_dataset_table():
    global DATASET_TABLE
    if DATASET_TABLE is not None: return True
    url = "https://raw.githubusercontent.com/WCRP-CORDEX/data-request-table/main/cmor-table/datasets.csv"
    local_path = "/g/data/xv83/users/bxn599/aus20i_check_all/datasets.csv"
    try:
        DATASET_TABLE = pd.read_csv(url)
        return True
    except Exception:
        if os.path.exists(local_path):
            try:
                DATASET_TABLE = pd.read_csv(local_path)
                return True
            except Exception: pass
        return False

def get_driving_metadata_from_path(target_dir):
    path = Path(target_dir).resolve()
    parts = path.parts
    try:
        anchor_idx = parts.index("CSIRO")
        driving_source_id = parts[anchor_idx + 1]
        driving_experiment_id = parts[anchor_idx + 2]
        driving_variant_label = parts[anchor_idx + 3]
        return {
            "driving_source_id": driving_source_id,
            "driving_experiment_id": driving_experiment_id,
            "driving_variant_label": EXPECTED_DRIVING_VARIANT_LABEL.get(driving_source_id, driving_variant_label),
            "driving_institution_id": EXPECTED_DRIVING_INSTITUTION_ID.get(driving_source_id, "Unknown"),
            "driving_experiment": EXPECTED_DRIVING_EXPERIMENT.get(driving_experiment_id, driving_experiment_id)
        }
    except (ValueError, IndexError): return {}

def get_expected_steps(filename, freq):
    try:
        parts = filename.stem.split('_')
        start_str, end_str = parts[-1].split('-')
        def fix_date(s):
            if len(s) == 4: return f"{s}0101"
            if len(s) == 6: return f"{s}01"
            return s
        start_dt, end_dt = pd.to_datetime(fix_date(start_str)), pd.to_datetime(fix_date(end_str))
        if freq == 'mon': return (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month) + 1
        delta_seconds = (end_dt - start_dt).total_seconds()
        if freq == 'day': return int(delta_seconds / 86400) + 1
        elif freq == '6hr': return int(delta_seconds / 21600) + 1
        elif freq == '1hr': return int(delta_seconds / 3600) + 1
    except Exception: return None

def get_reference_calendar(files, driving_source_id=None):
    if driving_source_id in EXPECTED_CALENDAR: return EXPECTED_CALENDAR[driving_source_id]
    if not files: return None
    try:
        with xr.open_dataset(files[0], decode_times=False) as ds:
            if 'time' in ds.coords:
                return ds.time.attrs.get('calendar') or ds.time.encoding.get('calendar')
    except Exception: return None

def get_official_cell_method(variable_id, freq):
    if not load_dataset_table(): return None
    var_col = 'out_name' if 'out_name' in DATASET_TABLE.columns else 'variable_id'
    try:
        match = DATASET_TABLE[(DATASET_TABLE[var_col].str.lower() == variable_id.lower()) & (DATASET_TABLE['frequency'] == freq)]
        if not match.empty: return str(match.iloc[0]['cell_methods']).strip()
    except KeyError: return None

def check_file_calendar(ds, filepath, expected_calendar=None, freq=None):
    errors = []
    try:
        variable_id = filepath.name.split('_')[0]

        # 1. Try to get official method
        official_method = get_official_cell_method(variable_id, freq)

        # 2. Determine validation rule (Fallback Logic)
        if official_method:
            # Use the CSV standard if available
            should_be_instantaneous = "time: point" in official_method.lower()
            method_source = f"official CORDEX spec ('{official_method}')"
        else:
            # Fallback: Use the file's own metadata
            actual_cell_method = ds[variable_id].attrs.get('cell_methods', "")
            should_be_instantaneous = "time: point" in actual_cell_method.lower()
            method_source = f"file attribute cell_methods ('{actual_cell_method}')"
            # Optional: Keep the notice that it wasn't in the CSV
            # errors.append(f"Notice: Variable '{variable_id}' at frequency '{freq}' not found in CSV. Using file metadata.")

        if 'time' in ds.coords:
            actual_calendar = ds.time.encoding.get('calendar') or ds.time.attrs.get('calendar')
            actual_units = ds.time.encoding.get('units') or ds.time.attrs.get('units')

            if actual_units != EXPECTED_TIME_UNITS:
                errors.append(f"Time Unit Mismatch: Found '{actual_units}', Expected '{EXPECTED_TIME_UNITS}'")

            if actual_calendar is None:
                errors.append("Time calendar attribute missing.")
            elif expected_calendar and actual_calendar != expected_calendar:
                errors.append(f"Calendar Mismatch: {actual_calendar} != {expected_calendar}")

            if freq and freq != 'fx':
                expected_len = get_expected_steps(filepath, freq)
                actual_len = ds.sizes['time']
                if expected_len is not None and actual_len != expected_len:
                    errors.append(f"Time Step Mismatch: Found {actual_len}, Expected {expected_len} based on filename")

            has_time_bnds = "time_bnds" in ds.variables

            if should_be_instantaneous:
                if has_time_bnds:
                    errors.append(f"Instantaneous '{variable_id}' based on {method_source} should NOT have time_bnds")
            else:
                if not has_time_bnds:
                    errors.append(f"Non-instantaneous '{variable_id}' based on {method_source} is missing time_bnds")
                else:
                    # Midpoint validation for non-instantaneous data
                    t_vals = ds.time.values
                    bnd_vals = ds['time_bnds'].values
                    for i in range(len(t_vals)):
                        # Using np.isclose for floating point safety with time coordinates
                        expected_mid = (bnd_vals[i, 0] + bnd_vals[i, 1]) / 2
                        if not np.isclose(t_vals[i].astype(float), expected_mid.astype(float), atol=1e-5):
                            errors.append(f"Time index {i} midpoint error (not centered in time_bnds)")
                            break

    except Exception as e:
        return [f"CRITICAL ERROR (Time/Calendar check failed): {str(e)}"]

    return errors

def check_lat_lon_boundaries(ds):
    errors = []
    for coord_name in ['lat', 'lon']:
        bounds_name = f"{coord_name}_bnds"
        if bounds_name not in ds.variables:
            errors.append(f"Missing variable: {bounds_name}")
            continue
        coords, bounds = ds[coord_name].values, ds[bounds_name].values
        for i in range(len(coords)):
            expected_mid = (bounds[i, 0] + bounds[i, 1]) / 2
            if not np.isclose(coords[i], expected_mid, atol=1e-5):
                errors.append(f"{coord_name} index {i} is not the midpoint of its bounds. Coord: {coords[i]}, Expected: {expected_mid}")
                break 
    return errors

def check_global_attributes(ds, expected_dict, path_meta, freq):
    attr_errors = []
    for attr, expected_val in expected_dict.items():
        if attr not in ds.attrs: attr_errors.append(f"Missing Global Attribute: '{attr}'")
        elif str(ds.attrs[attr]) != str(expected_val):
            if attr == "frequency": continue 
            attr_errors.append(f"Mismatch '{attr}': Found '{ds.attrs[attr]}', Expected '{expected_val}'")
    for attr, expected_val in path_meta.items():
        actual_val = ds.attrs.get(attr)
        if not actual_val: attr_errors.append(f"Missing Path-Derived Attribute: '{attr}'")
        elif str(actual_val) != str(expected_val): attr_errors.append(f"Mismatch '{attr}': Found '{actual_val}', Expected '{expected_val}' (from path)")
    
    actual_freq = ds.attrs.get("frequency")
    if actual_freq != freq: attr_errors.append(f"Mismatch 'frequency': Found '{actual_freq}', Expected '{freq}'")
    if "tracking_id" not in ds.attrs: attr_errors.append("Missing 'tracking_id'")
    else:
        # Check if prefix and slash is present
        prefix = "hdl:21.14103/"
        if not str(ds.attrs["tracking_id"]).startswith(prefix):
            attr_errors.append(f"Invalid tracking_id: Missing prefix '{prefix}'")
    if "creation_date" not in ds.attrs: attr_errors.append("Missing 'creation_date'")

    return attr_errors

def check_variable_metadata(ds, variable_id, freq):
    errors = []
    if not load_dataset_table(): return ["CRITICAL: Could not load DATASET_TABLE to check metadata."]
    var_col = 'out_name' if 'out_name' in DATASET_TABLE.columns else 'variable_id'
    match = DATASET_TABLE[(DATASET_TABLE[var_col].str.lower() == variable_id.lower()) & (DATASET_TABLE['frequency'] == freq)]
    if match.empty: return [f"Notice: Variable '{variable_id}' at frequency '{freq}' not found in official CORDEX CSV."]
    
    row = match.iloc[0]
    def get_str(col_name): return str(row[col_name]).strip() if pd.notna(row.get(col_name)) else None
    
    exp_cm, exp_units = get_str('cell_methods'), get_str('units')
    exp_ln, exp_sn = get_str('long_name'), get_str('standard_name')

    if variable_id not in ds.variables: return [f"Error: Variable '{variable_id}' not found in file."]
    actual_attrs = ds[variable_id].attrs

    if exp_cm and str(actual_attrs.get('cell_methods', '')).strip().lower() != exp_cm.lower():
        errors.append(f"Cell Method Mismatch for {variable_id}: Found '{actual_attrs.get('cell_methods')}', Expected '{exp_cm}'")
    if exp_units and str(actual_attrs.get('units', '')).strip() != exp_units:
        errors.append(f"Units Mismatch for {variable_id}: Found '{actual_attrs.get('units')}', Expected '{exp_units}'")
    if exp_ln and str(actual_attrs.get('long_name', '')).strip().lower() != exp_ln.lower():
        errors.append(f"Long Name Mismatch for {variable_id}: Found '{actual_attrs.get('long_name')}', Expected '{exp_ln}'")
    if exp_sn and str(actual_attrs.get('standard_name', '')).strip() != exp_sn:
        errors.append(f"Standard Name Mismatch for {variable_id}: Found '{actual_attrs.get('standard_name')}', Expected '{exp_sn}'")
    return errors

def format_tracking_id(tracking_id):
    """
    Ensures the tracking_id has the required handle prefix.
    """
    if not tracking_id:
        return tracking_id
        
    prefix = "hdl:21.14103/"
    
    # Cast to string to handle potential object types from netCDF
    tid_str = str(tracking_id)
    
    # Return as-is if already correct
    if tid_str.startswith(prefix):
        return tid_str
        
    # prefix already has the slash, so just lstrip the ID and join them
    return f"{prefix}{tid_str.lstrip('/')}"

def apply_fixes(nc_path, variable_id, freq):
    """
    Opens the file with decode_times=True, applies missing attributes from CSV,
    generates time_bnds if required, and saves securely. Handles 'fx' (static) files.
    """
    print(f"      🔧 Applying fixes to {nc_path.name}...")
    nc_path = Path(nc_path)
    backup_path = nc_path.with_suffix(".nc.bak")

    if os.path.exists(backup_path):
        print(f"      ⚠️ Backup already exists, skipping to prevent overwrite.")
        return

    # Load dataset. decode_times=True is safe even if 'time' is missing.
    ds = xr.open_dataset(nc_path, decode_times=True)
    
    # remove height variable as a scalar coordinate
    _has_height, hcoord = has_height(ds, variable_id)
    if _has_height:
        ds = ds.reset_coords(hcoord, drop=False)
        ds[variable_id].attrs["coordinates"] = hcoord

    # Safely extract time metadata only if time exists
    has_time = 'time' in ds.coords
    input_time_units = ds.time.encoding.get('units') if has_time else None
    input_time_calendar = ds.time.encoding.get('calendar') if has_time else None

    current_tid = ds.attrs.get("tracking_id")
    if current_tid:
        new_tid = format_tracking_id(current_tid)
        if new_tid != current_tid:
            print(f"      🆔 Updating tracking_id prefix for {nc_path.name}")
            ds.attrs["tracking_id"] = new_tid

    if not load_dataset_table():
        ds.close()
        return

    var_col = 'out_name' if 'out_name' in DATASET_TABLE.columns else 'variable_id'
    match = DATASET_TABLE[(DATASET_TABLE[var_col].str.lower() == variable_id.lower()) & (DATASET_TABLE['frequency'] == freq)]
    
    used_fallback = False
    if match.empty and freq == "6hr":
        match = DATASET_TABLE[(DATASET_TABLE[var_col].str.lower() == variable_id.lower()) & (DATASET_TABLE['frequency'] == "1hr")]
        used_fallback = True

    # 1. Update Attributes
    if not match.empty and variable_id in ds.variables:
        row = match.iloc[0]
        attr_map = {'cell_methods': 'cell_methods', 'units': 'units', 'long_name': 'long_name', 'standard_name': 'standard_name'}
        for csv_col, attr_key in attr_map.items():
            if csv_col in row and pd.notna(row[csv_col]):
                new_val = str(row[csv_col]).strip()
                if used_fallback and attr_key == 'cell_methods':
                    new_val = new_val.replace("1hr", "6hr").replace("1-hour", "6-hour")
                ds[variable_id].attrs[attr_key] = new_val

    # 2. Add time_bnds if required (only for non-fx variables)
    if has_time:
        official_method = ds[variable_id].attrs.get('cell_methods', '')
        needs_bounds = any(x in official_method for x in ["time: mean", "time: maximum", "time: minimum"])
        
        if needs_bounds and 'time_bnds' not in ds.variables:
            print(f"      ⏱️ Generating missing time_bnds for {variable_id}")
            time_vals = ds.time.values
            if freq == '1hr': offset = pd.to_timedelta('30min')
            elif freq == '6hr': offset = pd.to_timedelta('3h')
            elif freq == 'day': offset = pd.to_timedelta('12h')
            else: offset = pd.to_timedelta('0min')
            
            if offset.total_seconds() > 0:
                bnds_data = np.zeros((len(time_vals), 2), dtype=time_vals.dtype)
                bnds_data[:, 0] = time_vals - offset
                bnds_data[:, 1] = time_vals + offset
                ds['time_bnds'] = xr.DataArray(bnds_data, dims=("time", "bnds"), attrs={})
                ds.time.attrs['bounds'] = 'time_bnds'

    # 3. Apply exact encoding
    encoding = {}
    
    # Base Data Variable
    if AXIOM_CONFIG and 'variables' in AXIOM_CONFIG.encoding:
        encoding[variable_id] = AXIOM_CONFIG.encoding['variables'].copy()
        encoding[variable_id]['dtype'] = 'float32'
        if '_FillValue' in encoding[variable_id]:
            encoding[variable_id]['missing_value'] = encoding[variable_id]['_FillValue']
    else:
        encoding[variable_id] = {'dtype': 'float32', '_FillValue': 1e20, 'missing_value': 1e20}

    # Update height scalar coordinate encoding
    _has_height, hcoord = has_height_attr(ds, variable_id)
    if _has_height:
        encoding[hcoord] = AXIOM_CONFIG.encoding[hcoord]

    # remove time chunks for fx
    if not has_time and 'chunks' in encoding[variable_id]:
        pass

    # Coordinates and Bounds (filtered for existence)
    for v in ['time', 'lat', 'lon', 'time_bnds', 'lat_bnds', 'lon_bnds']:
        if v in ds.variables or v in ds.coords:
            enc = AXIOM_CONFIG.encoding.get(v, {}).copy() if AXIOM_CONFIG else {}
            enc['dtype'] = 'float64'
            
            # Only apply time-specific encoding if time actually exists
            if 'time' in v:
                if has_time:
                    if input_time_units: enc['units'] = input_time_units
                    if input_time_calendar: enc['calendar'] = input_time_calendar
                else:
                    # Skip encoding this coordinate if it's missing (extra safety)
                    continue
                    
            encoding[v] = enc

    # 4. Safe Write Operations
    shutil.move(str(nc_path), str(backup_path))
    try:
        # Static files should not have unlimited_dims=['time']
        write_args = {
            "format": 'NETCDF4_CLASSIC',
            "encoding": encoding,
        }
        
        if has_time and 'time' in ds.dims:
            write_args["unlimited_dims"] = ['time']

        # remove coordinates encoding if it is part of the variable
        if 'coordinates' in ds_merged[variable_id].encoding:
            del ds_merged[variable_id].encoding['coordinates']
            
        ds.to_netcdf(str(nc_path), **write_args)
        
        backup_path.unlink() 
        print(f"      ✅ Successfully updated.")
    except Exception as e:
        print(f"      ❌ Error saving {nc_path.name}: {e}. Rolling back.")
        shutil.move(str(backup_path), str(nc_path))
    finally:
        ds.close()


# --- Main Flow ---
def main():
    parser = argparse.ArgumentParser(description="Strict CORDEX-CMIP6 Integrity Checker and Fixer")
    parser.add_argument("directory", help="Target directory (Root, Model, or Scenario level)")
    parser.add_argument("--fix", action="store_true", help="DANGER: Apply metadata fixes and generate time bounds, overwriting files.")
    parser.add_argument("--freq", choices=["1hr", "6hr", "day", "mon", "fx", "all"], default="all", help="Target specific frequency to process")
    args = parser.parse_args()
    
    target_path = Path(args.directory).resolve()
    if not target_path.exists():
        print(f"ERROR: Path does not exist: {target_path}")
        sys.exit(1)

    if args.fix:
        print("\n" + "!"*80)
        print("⚠️  WARNING: Running in FIX mode. Non-compliant files will be permanently modified.")
        print("!"*80 + "\n")
    else:
        print("\n🔍 Running in CHECK-ONLY mode. No files will be changed.\n")

    valid_scenarios = ["historical", "ssp126", "ssp370", "evaluation"]

    if target_path.name in valid_scenarios:
        model_name = target_path.parent.name
        tasks = [(model_name, target_path.name, target_path)]
    else:
        tasks = []
        for model_dir in [d for d in target_path.iterdir() if d.is_dir()]:
            for scen in valid_scenarios:
                scen_path = model_dir / scen
                if scen_path.exists():
                    tasks.append((model_dir.name, scen, scen_path))

    if not tasks:
        print(f"No valid CORDEX scenarios found in: {target_path}")
        return

    for model_name, scen, scen_path in tasks:
        print(f"\n>>> Checking Model: {model_name} | Scenario: {scen}")
        
        freqs_to_run = EXPECTED_VAR_LISTS.keys() if args.freq == "all" else [args.freq]

        for freq in freqs_to_run:
            expected_vars = EXPECTED_VAR_LISTS[freq]
            found_freqs = list(scen_path.rglob(freq))
            
            for f_path in found_freqs:
                current_vars = {v.name for v in f_path.iterdir() if v.is_dir()}
                target_vars = set(expected_vars)
                
                missing, extra = target_vars - current_vars, current_vars - target_vars
                if missing or extra:
                    print(f"    [VAR MISMATCH] {freq}")
                    if missing: print(f"      ❌ Missing: {sorted(list(missing))}")
                    if extra:   print(f"      ⚠️  Extra: {sorted(list(extra))}")

                for var_dir in [v for v in f_path.iterdir() if v.is_dir()]:
                    for ver_dir in [v for v in var_dir.iterdir() if v.is_dir()]:
                        nc_files = sorted(list(ver_dir.glob("*.nc")))
                        
                        target_count = EXPECTED_FILES.get((scen, freq), 0)
                        if len(nc_files) != target_count:
                            print(f"    [FILE COUNT] {freq}/{var_dir.name}: {len(nc_files)} (Exp: {target_count})")
                        
                        if nc_files:
                            path_metadata = get_driving_metadata_from_path(nc_files[0].parent)
                            source_id = path_metadata.get('driving_source_id')
                            ref_cal = get_reference_calendar(nc_files, driving_source_id=source_id)

                            for nc in nc_files:
                                variable_id = nc.name.split('_')[0]
                                path_metadata = get_driving_metadata_from_path(nc.parent)
                                issues = []

                                # 1. Fast Check Pass (Read-Only, Decode Times = False)
                                try:
                                    with xr.open_dataset(nc, decode_times=False) as ds:
                                        issues.extend(check_file_calendar(ds, nc, expected_calendar=ref_cal, freq=freq))
                                        issues.extend(check_global_attributes(ds, EXPECTED_GLOBAL_ATTRIBUTES, path_metadata, freq))
                                        issues.extend(check_lat_lon_boundaries(ds))
                                        issues.extend(check_variable_metadata(ds, variable_id, freq))
                                except Exception as e:
                                    issues.append(f"Attribute Read Error: {str(e)}")
                            
                                # 2. Report and Fix Pass
                                if issues:
                                    print(f"    [FAIL] {nc.name}:")
                                    for issue in issues:
                                        print(f"      - {issue}")
                                    
                                    # Trigger fixer if requested
                                    if args.fix:
                                        apply_fixes(nc, variable_id, freq)

    print(f"\n{'='*80}\nEXECUTION COMPLETE\n{'='*80}")

if __name__ == "__main__":
    main()
