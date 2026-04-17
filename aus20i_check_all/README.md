Use this script to check metadata, calendar, bounds, file count

Use `--run` flag in the .pbs files to modify the data. Without the `--run` flag no data will be modified. 
`python check_all.py "$DIR" --freq "$FREQ" --fix`

After running this script, check whether time_bounds need to be updated