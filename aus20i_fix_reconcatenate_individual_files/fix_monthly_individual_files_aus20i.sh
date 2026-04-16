#!/bin/bash

# --- CONFIGURATION ---
ROOT_DIR="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/ssp370/r4i1p1f1/CCAM-v2203-SN/v1-r1/mon"
DRY_RUN=false  # Set to 'false' to actually execute the changes
# ---------------------

# Load CDO if not already loaded (standard for Gadi)
if ! command -v cdo &> /dev/null; then
    module load cdo 2>/dev/null
fi

if [ "$DRY_RUN" = true ]; then
    echo "--- DRY RUN MODE ACTIVE: No files will be modified ---"
else
    echo "--- EXECUTION MODE: Backups will be created and files modified ---"
fi

# Find all NetCDF files, excluding backups
find "$ROOT_DIR" -type f -name "*.nc" ! -name "*.bak" | while read -r FILE; do
    FILENAME=$(basename "$FILE")
    DIRNAME=$(dirname "$FILE")
    
    # Extract date range (e.g., 202701-202712)
    RANGE=$(echo "$FILENAME" | grep -oE "[0-9]{6}-[0-9]{6}")
    [[ -z "$RANGE" ]] && continue

    START_YEAR=${RANGE:0:4}
    END_YEAR=${RANGE:7:4}

    # Identify Single Year Files (Source)
    if [ "$START_YEAR" -eq "$END_YEAR" ]; then
        SOURCE_FILE="$FILE"
        YEAR="$START_YEAR"
        
        # Extract Prefix (removes the date and .nc)
        VAR_PREFIX=$(echo "$FILENAME" | sed -E 's/[0-9]{6}-[0-9]{6}\.nc//')
        
        TARGET_FILE=""

        # Search for the 10-year target in the same directory
        for f in "${DIRNAME}/${VAR_PREFIX}"*.nc; do
            [[ "$f" == *".bak"* ]] && continue
            [[ "$f" == "$SOURCE_FILE" ]] && continue
            
            T_RANGE=$(basename "$f" | grep -oE "[0-9]{6}-[0-9]{6}")
            [[ -z "$T_RANGE" ]] && continue
            
            T_START=${T_RANGE:0:4}
            T_END=${T_RANGE:7:4}
            
            # Match: Target is a range AND includes the source year
            if [ "$T_START" -ne "$T_END" ] && [ "$YEAR" -ge "$T_START" ] && [ "$YEAR" -le "$T_END" ]; then
                TARGET_FILE="$f"
                break 
            fi
        done

        # If a target is found and the source is newer (or you can remove the -nt check)
        if [[ -n "$TARGET_FILE" ]]; then
            if [[ "$SOURCE_FILE" -nt "$TARGET_FILE" ]]; then
                
                TEMP_FILE="${TARGET_FILE}.tmp"
                HOLED_FILE="${TARGET_FILE}.holed"

                if [ "$DRY_RUN" = true ]; then
                    echo "------------------------------------------------"
                    echo "DRY RUN: Found update for $(basename "$TARGET_FILE")"
                    echo "         Source: $(basename "$SOURCE_FILE")"
                    echo "         Step 1: cdo delete,year=$YEAR $(basename "$TARGET_FILE") temp_holed.nc"
                    echo "         Step 2: cdo mergetime temp_holed.nc $(basename "$SOURCE_FILE") final.nc"
                else
                    echo "------------------------------------------------"
                    echo "PROCESSING: $(basename "$TARGET_FILE")"
                    
                    # Create Backup
                    cp "$TARGET_FILE" "${TARGET_FILE}.bak"
                    
                    # Step 1: Create the 'holed' file (remove the year being replaced)
                    echo "   - Removing old year $YEAR data..."
                    cdo -L -z zip delete,year="$YEAR" "$TARGET_FILE" "$HOLED_FILE"
                    
                    # Step 2: Merge with new source file
                    echo "   - Merging new year $YEAR data..."
                    if cdo -L -z zip mergetime "$HOLED_FILE" "$SOURCE_FILE" "$TEMP_FILE"; then
                        mv "$TEMP_FILE" "$TARGET_FILE"
                        echo "   - SUCCESS: Updated $(basename "$TARGET_FILE")"
                    else
                        echo "   - ERROR: CDO failed on merge step for $FILENAME"
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
echo "Done."
