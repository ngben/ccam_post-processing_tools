#!/bin/bash

# List of variables to process
variables=("ps" "huss" "uas" "vas" "tas" "ta1000" "ta925" "ta850" "ta700" "ta600" "ta500" "ta400" "ta300" "ta250" "ta200" "ua1000" "ua925" "ua850" "ua700" "ua600" "ua500" "ua400" "ua300" "ua250" "ua200" "va1000" "va925" "va850" "va700" "va600" "va500" "va400" "va300" "va250" "va200" "zg1000" "zg925" "zg850" "zg700" "zg600" "zg500" "zg400" "zg300" "zg250" "zg200" "hus1000" "hus925" "hus850" "hus700" "hus600" "hus500" "hus400" "hus300" "hus250" "hus200") # Replace VAR1, VAR2, etc., with your variable names

# Base directory template
base_dir_template="/g/data/xv83/users/bxn599/ACS/axiom_20i/CORDEX/CMIP6/DD/AUS-20i/CSIRO/ERA5/evaluation/r1i1p1f1/CCAM-v2203-SN/v1-r1/day/{var}/v20240920"

# Backup directory template
backup_base_dir_template="/scratch/xv83/bxn599/backup_{var,,}"

# Function to get a sorted list of unique years in the files
get_unique_years() {
    local dir="$1"
    find "$dir" -name "*.nc" | grep -oP '_\d{8}-\d{8}\.nc' | grep -oP '\d{8}' | cut -c 1-4 | sort -u
}

# Function to concatenate files and move them to a backup directory if successful
concatenate_files() {
    local start_year="$1"
    local end_year="$2"
    local dir="$3"
    local backup_base_dir="$4"

    files_to_concat=()
    for ((year=start_year; year<=end_year; year++)); do
        for file in "$dir"/*"${year}0101-"*.nc; do
            [ -e "$file" ] || continue
            files_to_concat+=("$file")
        done
    done

    if [ "${#files_to_concat[@]}" -gt 0 ]; then
        first_year=$(basename "${files_to_concat[0]}" | grep -oP '_\d{8}' | cut -c 2-5)
        last_year=$end_year
        basename_file=$(basename "${files_to_concat[0]}")
        prefix="${basename_file%_[0-9]*-[0-9]*.nc}"
        output_file="$dir/${prefix}_${first_year}0101-${last_year}1231.nc"

        echo "Output file: $output_file"

        cdo -O cat "${files_to_concat[@]}" "$output_file"
        if [ $? -eq 0 ]; then
            echo "Concatenation successful. Moving files to backup."

            for file in "${files_to_concat[@]}"; do
                relative_path=$(realpath --relative-to="$base_dir" "$dir")
                target_backup_base_dir="$backup_base_dir/$relative_path"
                mkdir -p "$target_backup_base_dir"
                mv "$file" "$target_backup_base_dir/"
                echo "Moved $file to $target_backup_base_dir"
            done
        else
            echo "Concatenation failed. Files not moved."
        fi
    fi
}

# Main loop to process each variable
for var in "${variables[@]}"; do
    echo "Processing variable: $var"

    # Replace placeholders in directory paths
    base_dir="${base_dir_template/\{var\}/$var}"
    backup_base_dir="${backup_base_dir_template/\{var,,\}/${var,,}}"

    # Get the unique years for the specified directory
    years=($(get_unique_years "$base_dir"))
    echo "Years found for $var: ${years[@]}"

    span=5
    processed_years=()

    start_years=($(echo "${years[@]}" | tr ' ' '\n' | grep -E '1$|6$'))
    start_years=(${start_years[@]:-$(echo "${years[0]}")})
    echo "Start years for $var: ${start_years[@]}"

    for start_year in "${start_years[@]}"; do
        end_year=$((start_year + span - 1))
        [ "$end_year" -gt "${years[-1]}" ] && end_year="${years[-1]}"
        echo "Concatenating files for $var from $start_year to $end_year"
        concatenate_files "$start_year" "$end_year" "$base_dir" "$backup_base_dir"

        for ((year=start_year; year<=end_year; year++)); do
            processed_years+=("$year")
        done
    done

    remaining_years=()
    for year in "${years[@]}"; do
        if [[ ! " ${processed_years[@]} " =~ " $year " ]]; then
            remaining_years+=("$year")
        fi
    done
    echo "Remaining years for $var: ${remaining_years[@]}"

    if [ ${#remaining_years[@]} -gt 0 ]; then
        start_year="${remaining_years[0]}"
        end_year="${remaining_years[-1]}"
        echo "Concatenating remaining files for $var from $start_year to $end_year"
        concatenate_files "$start_year" "$end_year" "$base_dir" "$backup_base_dir"
    fi
done
