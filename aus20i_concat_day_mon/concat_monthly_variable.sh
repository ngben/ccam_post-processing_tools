#!/bin/bash

# Set the variable name
var="snw"

# Define the base and backup directories for the chosen variable
base_dir="/scratch/e53/bxn599/axiom_20i/CORDEX-CMIP6/DD/AUS-20i/CSIRO/EC-Earth3/ssp126/r1i1p1f1/CCAM-v2203-SN/v1-r1/mon/${var}/v20240920"
backup_base_dir="/scratch/xv83/bxn599/backup_${var,,}"

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
    local backup_base_dir="$4"

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

# Get the unique years for the specified directory
years=($(get_unique_years "$base_dir"))
echo "Years found: ${years[@]}"

span=10
processed_years=()

# Start with years ending in '1'
start_years=$(echo "${years[@]}" | tr ' ' '\n' | grep -E '1$')
start_years=(${start_years[@]:-$(echo "${years[0]}")})
echo "Start years: ${start_years[@]}"

for start_year in "${start_years[@]}"; do
    end_year=$((start_year + span - 1))
    [ "$end_year" -gt "${years[-1]}" ] && end_year="${years[-1]}"

    echo "Concatenating files from $start_year to $end_year in directory $base_dir"
    concatenate_files "$start_year" "$end_year" "$base_dir" "$backup_base_dir"

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

    echo "Concatenating remaining files from $start_year to $end_year in directory $base_dir"
    concatenate_files "$start_year" "$end_year" "$base_dir" "$backup_base_dir"
fi
