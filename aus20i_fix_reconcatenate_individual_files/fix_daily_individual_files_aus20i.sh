#!/bin/bash

# --- CONFIGURATION ---
# Path to your 'day' root directory
ROOT_DIR="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/ssp370/r4i1p1f1/CCAM-v2203-SN/v1-r1/day"
DRY_RUN=false  # Set to 'false' to actually execute the changes
# ---------------------

# Load CDO if not already loaded
if ! command -v cdo &> /dev/null; then
    module load cdo 2>/dev/null
fi

if [ "$DRY_RUN" = true ]; then
    echo "--- DRY RUN MODE ACTIVE: No files will be modified ---"
else
    echo "--- EXECUTION MODE: Daily files will be modified ---"
fi

# Find all daily NetCDF files, excluding backups
find "$ROOT_DIR" -type f -name "*.nc" ! -name "*.bak" | while read -r FILE; do
    FILENAME=$(basename "$FILE")
    DIRNAME=$(dirname "$FILE")
    
    # Extract date range (e.g., 20270101-20271231)
    # Daily format is YYYYMMDD-YYYYMMDD
    RANGE=$(echo "$FILENAME" | grep -oE "[0-9]{8}-[0-9]{8}")
    [[ -z "$RANGE" ]] && continue

    START_DATE=${RANGE:0:8}
    END_DATE=${RANGE:9:8}
    
    START_YEAR=${START_DATE:0:4}
    END_YEAR=${END_DATE:0:4}

    # Identify Single Year Files (Source)
    if [ "$START_YEAR" -eq "$END_YEAR" ]; then
        SOURCE_FILE="$FILE"
        YEAR="$START_YEAR"
        
        # Extract Prefix (removes the date and .nc)
        # We look for '_day_' to ensure we split at the right place
        VAR_PREFIX=$(echo "$FILENAME" | sed -E 's/[0-9]{8}-[0-9]{8}\.nc//')
        
        TARGET_FILE=""

        # Search for the 5-year target in the same directory
        for f in "${DIRNAME}/${VAR_PREFIX}"*.nc; do
            [[ "$f" == *".bak"* ]] && continue
            [[ "$f" == "$SOURCE_FILE" ]] && continue
            
            T_RANGE=$(basename "$f" | grep -oE "[0-9]{8}-[0-9]{8}")
            [[ -z "$T_RANGE" ]] && continue
            
            T_START_YEAR=${T_RANGE:0:4}
            T_END_YEAR=${T_RANGE:9:4}
            
            # Match: Target is a 5-year range AND includes our source year
            if [ "$T_START_YEAR" -ne "$T_END_YEAR" ] && [ "$YEAR" -ge "$T_START_YEAR" ] && [ "$YEAR" -le "$T_END_YEAR" ]; then
                TARGET_FILE="$f"
                break 
            fi
        done

        # If a target is found and source is newer
        if [[ -n "$TARGET_FILE" ]]; then
            if [[ "$SOURCE_FILE" -nt "$TARGET_FILE" ]]; then
                
                TEMP_FILE="${TARGET_FILE}.tmp"
                HOLED_FILE="${TARGET_FILE}.holed"

                if [ "$DRY_RUN" = true ]; then
                    echo "------------------------------------------------"
                    echo "DRY RUN (DAILY): Found update for $(basename "$TARGET_FILE")"
                    echo "         Source: $(basename "$SOURCE_FILE")"
                else
                    echo "------------------------------------------------"
                    echo "PROCESSING DAILY: $(basename "$TARGET_FILE")"
                    
                    # Create Backup
                    cp "$TARGET_FILE" "${TARGET_FILE}.bak"
                    
                    # Step 1: Remove the old data for that specific year
                    echo "   - Removing year $YEAR from 5-year file..."
                    cdo -L -z zip delete,year="$YEAR" "$TARGET_FILE" "$HOLED_FILE"
                    
                    # Step 2: Merge the holed 5-year file with the new 1-year file
                    echo "   - Merging updated year $YEAR data..."
                    if cdo -L -z zip mergetime "$HOLED_FILE" "$SOURCE_FILE" "$TEMP_FILE"; then
                        mv "$TEMP_FILE" "$TARGET_FILE"
                        echo "   - SUCCESS: Updated daily file $(basename "$TARGET_FILE")"
                    else
                        echo "   - ERROR: CDO merge failed for $(basename "$TARGET_FILE")"
                        rm -f "$TEMP_FILE"
                    fi
                    
                    # Cleanup intermediate file
                    rm -f "$HOLED_FILE"
                fi
            fi
        fi
    fi
done

echo "------------------------------------------------"
echo "Daily Data Process Complete."
