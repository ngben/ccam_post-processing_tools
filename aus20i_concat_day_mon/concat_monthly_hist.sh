#!/bin/bash
#PBS -l walltime=12:00:00
#PBS -l ncpus=1
#PBS -l mem=48GB
#PBS -l wd
#PBS -m n
#PBS -P e53
#PBS -q normal
#PBS -l storage=gdata/xp65+gdata/hd50+gdata/ia39+gdata/tp28+gdata/dp9+gdata/xv83+gdata/hq89+scratch/e53+scratch/xv83+gdata/xv83

module use /g/data3/xp65/public/modules
module load conda/analysis3

# Function to get a sorted list of unique years in the files
get_unique_years() {
    local dir="$1"
    find "$dir" -name "*.nc" | grep -oP '_\d{6}-\d{6}\.nc' | grep -oP '\d{6}' | cut -c 1-4 | sort -u
}

# Function to concatenate files and move them to a backup directory if successful
concatenate_files() {
    local start_year="$1"
    local end_year="$2"
    local dir="$3"
    local backup_dir="$4"

    files_to_concat=()
    for ((year=start_year; year<=end_year; year++)); do
        for file in "$dir"/*"${year}01-${year}12".nc; do
            [ -e "$file" ] || continue
            files_to_concat+=("$file")
        done
    done

    if [ "${#files_to_concat[@]}" -gt 0 ]; then
        # Construct the output file name based on the first and last year
        first_year=$(basename "${files_to_concat[0]}" | grep -o '_[0-9]\{6\}-' | cut -c 2-5)
        last_year=$end_year

        basename_file=$(basename "${files_to_concat[0]}")

        # Extract the prefix before the date range
        prefix="${basename_file%_[0-9]*-[0-9]*.nc}"

        # Construct the output file name
        output_file="$dir/${prefix}_${first_year}01-${last_year}12.nc"

        echo "Output file: $output_file"

        # Concatenate the files with CDO
        cdo -O cat "${files_to_concat[@]}" "$output_file"
        if [ $? -eq 0 ]; then
            echo "Concatenation successful. Moving files to backup."

            # Move the input files to the backup directory while retaining directory structure
            for file in "${files_to_concat[@]}"; do
                relative_path=$(realpath --relative-to="$base_dir" "$dir")
                target_backup_dir="$backup_dir/$relative_path"

                mkdir -p "$target_backup_dir"
                mv "$file" "$target_backup_dir/"
                echo "Moved $file to $target_backup_dir"
            done
        else
            echo "Concatenation failed. Files not moved."
        fi
    fi
}

# Base directory for monthly files
### ACCESS-CM2
#base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-CM2/historical/r4i1p1f1/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/access-cm2_hist"
### ACCESS-ESM1-5
#base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ACCESS-ESM1-5/historical/r6i1p1f1/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/access-esm1-5_hist"
### CESM2
#base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CESM2/historical/r11i1p1f1/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/cesm2_hist"
### CMCC-ESM2
#base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CMCC-ESM2/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/cmcc-esm2_hist"
### CNRM-ESM2-1
#base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/CNRM-ESM2-1/historical/r1i1p1f2/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/cnrm-esm2-1_hist"
### EC-Earth3
#base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/EC-Earth3/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/access-esm1-5_hist"
### ERA5
base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/ERA5/evaluation/r1i1p1f1/CCAM-v2203-SN/v1-r1"
backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/era5"
### NorESM2-MM
#base_dir="/g/data/xv83/CORDEX-CMIP6/DD/AUS-20i/CSIRO/NorESM2-MM/historical/r1i1p1f1/CCAM-v2203-SN/v1-r1"
#backup_base_dir="/scratch/xv83/bxn599/backup_aus-20i_daily_monthly_separated_by_year/noresm2-mm_hist"

# Loop through directories for monthly data
for freq_dir in "$base_dir/mon"; do
    find "$freq_dir" -mindepth 2 -type d | while read -r subdir; do
        years=($(get_unique_years "$subdir"))

        echo "Processing directory: $subdir"
        echo "Frequency: mon"
        echo "Years found: ${years[@]}"

        span=10
        processed_years=() # Initialize or reset processed years
        
        # Start with years ending in '1'
        start_years=$(echo "${years[@]}" | tr ' ' '\n' | grep -E '1$')

        start_years=(${start_years[@]:-$(echo "${years[0]}")})
        echo "Start years: ${start_years[@]}"

        for start_year in "${start_years[@]}"; do
            end_year=$((start_year + span - 1))
            [ "$end_year" -gt "${years[-1]}" ] && end_year="${years[-1]}"

            backup_dir="${backup_base_dir}/mon"

            echo "Concatenating files from $start_year to $end_year in directory $subdir"
            concatenate_files "$start_year" "$end_year" "$subdir" "$backup_dir"

            # Mark these years as processed
            for ((year=start_year; year<=end_year; year++)); do
                processed_years+=("$year")
            done
        done

        # Handle remaining years outside the main start years
        remaining_years=()
        for year in "${years[@]}"; do
            if [[ ! " ${processed_years[@]} " =~ " $year " ]]; then
                remaining_years+=("$year")
            fi
        done
        echo "Remaining years: ${remaining_years[@]}"

        if [ ${#remaining_years[@]} -gt 0 ]; then
            # Concatenate all remaining years together
            start_year="${remaining_years[0]}"
            end_year="${remaining_years[-1]}"

            backup_dir="${backup_base_dir}/mon"

            echo "Concatenating remaining files from $start_year to $end_year in directory $subdir"
            concatenate_files "$start_year" "$end_year" "$subdir" "$backup_dir"
        fi
    done
done
