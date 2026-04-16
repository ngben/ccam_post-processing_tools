#!/bin/bash

# Define an array of base directories
BASE_DIRS=(
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ERA5/evaluation"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/historical"
#    "/scratch/xv83/bxn599/test_aus20i_fix_script/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/historical"
    "/scratch/xv83/bxn599/axiom_20i_test_lookup/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/historical"
    "/scratch/xv83/bxn599/axiom_20i_test_lookup/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/ssp126"
    "/scratch/xv83/bxn599/axiom_20i_test_lookup/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/ssp370"
)

# Define the frequencies to process
FREQUENCIES=("1hr" "6hr" "day" "mon" "fx")
#FREQUENCIES=("fx" "mon" "day" "6hr")

for DIR in "${BASE_DIRS[@]}"; do
    # Extract model and scenario from the path
    model=$(basename "$(dirname "$DIR")")
    scenario=$(basename "$DIR")
    
    for FREQ in "${FREQUENCIES[@]}"; do
        # Append frequency to the job name so you can track them in qstat
        job_name="chk_${model}_${scenario}_${FREQ}"

        echo "Submitting job for $model/$scenario at frequency: $FREQ..."
        echo "Path: $DIR"

        # Pass both DIR and FREQ to the PBS script
        qsub -N "$job_name" \
             -v DIR="$DIR",FREQ="$FREQ" check_all.pbs
             
        echo "--------------------------------------"
    done
done
