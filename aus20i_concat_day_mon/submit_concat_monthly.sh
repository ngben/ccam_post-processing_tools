#!/bin/bash

# Define an array of base directories
BASE_DIRS=(
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/historical/r4i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/ssp126/r4i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/ssp370/r4i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-ESM1-5/historical/r6i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-ESM1-5/ssp126/r6i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-ESM1-5/ssp370/r6i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CESM2/historical/r11i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CESM2/ssp126/r11i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CESM2/ssp370/r11i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CNRM-ESM2-1/historical/r1i1p1f2/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CNRM-ESM2-1/ssp126/r1i1p1f2/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CNRM-ESM2-1/ssp370/r1i1p1f2/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/EC-Earth3/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/EC-Earth3/ssp126/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/EC-Earth3/ssp370/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/ssp126/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/ssp370/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ERA5/evaluation/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/NorESM2-MM/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/NorESM2-MM/ssp126/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#    "/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/NorESM2-MM/ssp370/r1i1p1f1/CCAM-v2203-SN/v1-r1"
    "/scratch/xv83/bxn599/axiom_20i_test_lookup/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
)

for DIR in "${BASE_DIRS[@]}"; do
    # Extract model and scenario from the path
    model=$(echo "$DIR" | awk -F'CSIRO/' '{print $2}' | cut -d'/' -f1)
    scenario=$(echo "$DIR" | awk -F'CSIRO/' '{print $2}' | cut -d'/' -f2)
    
    job_name="cat_${model}_${scenario}"

    echo "Submitting job for $model/$scenario"
    echo "Path: $DIR"

    # Pass both DIR to the PBS script
    qsub -N "$job_name" \
        -v DIR="$DIR" submit_concat_monthly.pbs
             
    echo "--------------------------------------"
done
