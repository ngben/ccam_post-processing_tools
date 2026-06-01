#!/bin/bash

# Define an array of base directories
BASE_DIRS=(
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ERA5/evaluation"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/historical"
    "/scratch/e53/bxn599/axiom_20i_2026-05-25/CORDEX-CMIP6/DD/AUS-20i/CSIRO/EC-Earth3/historical"
#    "/scratch/e53/bxn599/axiom_20i_2026-05-25/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-ESM1-5/ssp126"
#    "/scratch/e53/bxn599/axiom_20i_2026-05-25/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-ESM1-5/ssp370"
#    "/scratch/xv83/bxn599/ERA5/evaluation"
)

# Define the frequencies to process
FREQUENCIES=("1hr" "6hr" "day" "mon" "fx")
#FREQUENCIES=("mon" "day")

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
