#!/usr/bin/bash

# Check for input parameters
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <s3_base_path> <table_name>"
  exit 1
fi

S3_BASE_PATH=$1
TABLE_NAME=$2
BASE_PATH="${S3_BASE_PATH}/${TABLE_NAME}"

# Temp file to store S3 listing
TEMP_FILE=$(mktemp)

# Function to calculate file stats
process_files() {
  local partition_name=$1
  local files=$2
  
  # Initialize variables
  local min_size=0
  local max_size=0
  local total_size=0
  local total_files=0
  local min_timestamp=""
  local max_timestamp=""
  
  # Process files
  while read -r file; do
    size=$(echo "$file" | awk '{print $1}')
    timestamp=$(echo "$file" | awk '{print $2}')
    
    # Initialize or update min/max/totals
    if [ "$total_files" -eq 0 ]; then
      min_size=$size
      max_size=$size
      min_timestamp=$timestamp
      max_timestamp=$timestamp
    else
      if [ "$size" -lt "$min_size" ]; then min_size=$size; fi
      if [ "$size" -gt "$max_size" ]; then max_size=$size; fi
      if [ "$timestamp" \< "$min_timestamp" ] || [ -z "$min_timestamp" ]; then min_timestamp=$timestamp; fi
      if [ "$timestamp" \> "$max_timestamp" ] || [ -z "$max_timestamp" ]; then max_timestamp=$timestamp; fi
    fi
    
    total_size=$((total_size + size))
    total_files=$((total_files + 1))
  done <<< "$files"
  
  # Calculate average size
  if [ "$total_files" -gt 0 ]; then
    avg_size=$((total_size / total_files))
  else
    avg_size=0
  fi
  
  # Output results
  echo "${partition_name},$min_size,$avg_size,$max_size,$min_timestamp,$max_timestamp,$total_files"
}

# Add headers to output
echo "Partition Name,Min File Size (bytes),Avg File Size (bytes),Max File Size (bytes),Min Timestamp,Max Timestamp,Total Number of Files"

# List all objects (non-hidden, excluding $folder$ suffix)
aws s3 ls "${BASE_PATH}/" --recursive | grep -Ev '/\.|/\$folder\$' > "$TEMP_FILE"

# Identify if the table is partitioned
partitions=$(awk -F"${TABLE_NAME}/" '{print $2}' "$TEMP_FILE" | awk -F'/' '{OFS="/"; $NF=""; print $0}' | sort -u)

if [ -z "$partitions" ]; then
  # Non-partitioned table
  files=$(awk '/\.parquet$/' "$TEMP_FILE" | awk '{print $3, $1}')
  if [ -z "$files" ]; then
    echo "No Parquet files found in the table $TABLE_NAME"
  else
    process_files "${TABLE_NAME}" "$files"
  fi
else
  # Partitioned table: Process each partition
  for partition in $partitions; do
    # Remove trailing slashes from partition names
    partition=$(echo "$partition" | sed 's/\/$//')
    
    # List parquet files in the partition
    files=$(aws s3 ls "${BASE_PATH}/${partition}/" --recursive | grep '.parquet' | awk '{print $3, $1}')
    
    if [ -z "$files" ]; then
      echo "No Parquet files in partition ${TABLE_NAME}/${partition}"
      continue
    fi
    
    process_files "${TABLE_NAME}/${partition}" "$files"
  done
fi

# Cleanup
rm -f "$TEMP_FILE"
