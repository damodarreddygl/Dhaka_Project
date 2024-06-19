#!/bin/bash

# Author: Damodar Reddy G.L
# Author's Designation: BI Developer
# Script Created Date: 2024-06-14
# Purpose: This script transfers data between databases and fills data gaps in the specified time intervals for the Fascia system.
# Testing Phase: Phase 1

# Log file path
LOG_FILE="/tmp/Db-To-Db-error_log.txt"

# Function to log messages with timestamps
log_message() {
    MESSAGE="$1"
    printf "[%s] - %s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "$MESSAGE" >> "$LOG_FILE"
}

# Function to execute SQL query and log errors if any
execute_query() {
    QUERY="$1"
    ERROR_MSG="$2"
    DATABASE="$3"

    # Log start time and comment
    log_message "Executing query: $ERROR_MSG"
    log_message "Query execution started."
    START_QUERY_TIME=$(date +"%Y-%m-%d %H:%M:%S")

    # Execute query and capture output
    # These options -s, -N, and -e are particularly useful when automating MySQL queries through scripts
    # -s (--silent) 
    # -N (--skip-column-names) 
    # -e (--execute) This option allows you to specify the query (or commands) to execute directly on the command line, immediately after `-e`
    # The -s -N options ensure that only the query results are displayed, without any headers or column names.

    QUERY_RESULT=$(mysql -u"$USER" -p"$PASSWORD" -h"$HOST" "$DATABASE" -s -N -e "$QUERY" 2>&1)
    if [ $? -ne 0 ]; then
        log_message "Error occurred: $ERROR_MSG"
        log_message "Error details: $QUERY_RESULT"
    else
        log_message "Success: $ERROR_MSG"
        if [[ "$ERROR_MSG" == *"Counting entries"* ]]; then
    # Format for count queries
            printf "+----------+---------------------------+---------------------+---------------------+\n" >> "$LOG_FILE"
            printf "| count(*) |     count(host_name)      |         min         |         max         |\n" >> "$LOG_FILE"
            printf "+----------+---------------------------+---------------------+---------------------+\n" >> "$LOG_FILE"
            printf "%s" "$QUERY_RESULT" | awk 'BEGIN {FS="\t"} {printf("| %-8s | %-25s | %-19s | %-19s |\n", $1, $2, $3, $4)}' >> "$LOG_FILE"
            printf "+----------+---------------------------+---------------------+---------------------+\n" >> "$LOG_FILE"
        else
    # Default format for other queries
            printf "Query Result:\n" >> "$LOG_FILE"
            printf "%s\n" "$QUERY_RESULT" >> "$LOG_FILE"
        fi
    fi

    # Log end time
    END_QUERY_TIME=$(date +"%Y-%m-%d %H:%M:%S")
    log_message "Query execution ended."
    log_message "Query execution time: Started at $START_QUERY_TIME, Ended at $END_QUERY_TIME."
}

# Function to calculate total execution time
calculate_execution_time() {
    START_SECONDS=$(date +%s -d "$START_TIME")
    END_SECONDS=$(date +%s -d "$END_TIME")
    EXECUTION_TIME=$((END_SECONDS - START_SECONDS))

    # Format seconds into HH:MM:SS
    formatted_time=$(date -u -d @"$EXECUTION_TIME" +'%H:%M:%S')
    printf "Total execution time: %s\n" "$formatted_time" >> "$LOG_FILE"
}

# Start time of script execution
START_TIME=$(date +"%Y-%m-%d %H:%M:%S")

# Add empty line before first log entry
echo "" >> "$LOG_FILE"

log_message "Script execution started."

# Check if author name is present in script header
AUTHOR_NAME="Damodar Reddy G.L"
AUTHOR_DESIGNATION="BI Developer"
SCRIPT_CREATED_DATE="2024-06-14"

AUTHOR_HEADER="# Author: Damodar Reddy G.L\n# Author Designation: BI Developer\n# Date Created: 2024-06-14"
if ! grep -qF "$AUTHOR_HEADER" "$0"; then
    log_message "Author information missing. Script execution stopped."
    exit 1
fi

# MySQL/MariaDB connection parameters
USER="root"
PASSWORD="USN7ETS0510SEC3030"
HOST="127.0.0.1"
SOURCE_DB="fascia3"
DESTINATION_DB="fascia"

# Query 1: Transferring data from Fascia3.hosts to Fascia.hosts using insert query
# This query transfers data from the source database (fascia3.hosts) to the destination database (fascia.hosts) within a specific time range.
# Note: This query should be executed in the source database.

INSERT_QUERY1="INSERT INTO $DESTINATION_DB.hosts (
    last_state_change, last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
)
SELECT
    last_state_change, last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
FROM $SOURCE_DB.hosts
WHERE last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'))
      AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))"

execute_query "$INSERT_QUERY1" "Transferring data from Fascia3.hosts to Fascia.hosts." "$SOURCE_DB"

# Query 1.2.1: Counting the number of entries transferred from Fascia3.hosts to Fascia.hosts

COUNT_QUERY="SELECT COUNT(*), COUNT(DISTINCT host_name), FROM_UNIXTIME(MIN(last_check)) AS min, FROM_UNIXTIME(MAX(last_check)) AS max FROM $DESTINATION_DB.hosts
                WHERE last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'))
                                        AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))"

execute_query "$COUNT_QUERY" "Counting entries from Fascia3.hosts to Fascia.hosts." "$DESTINATION_DB"

# Query 1.2.0: Additional count query from Fascia3.hosts for reference

COUNT_QUERY2="SELECT COUNT(*), COUNT(DISTINCT host_name), FROM_UNIXTIME(MIN(last_check)) AS min, FROM_UNIXTIME(MAX(last_check)) AS max FROM $SOURCE_DB.hosts
                WHERE last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'))
                                AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))"

execute_query "$COUNT_QUERY2" "Counting entries from Fascia3.hosts." "$SOURCE_DB"

# Query 2: Filling data gaps at time 00:00:00 using insert query on fascia.hosts
# This query fills data gaps at midnight (00:00:00) in the fascia.hosts table by inserting missing records.
# Note: This query should be executed in the destination database.

INSERT_QUERY2="INSERT INTO $DESTINATION_DB.hosts (
    last_state_change, last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
)
SELECT
    last_state_change,
    UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 00:00:00')) AS last_check,
    next_check, last_time_up, last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
FROM (
SELECT
    last_state_change, MIN(last_check) AS last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output, performance_data,
    scheduled_downtime_depth, current_state
FROM $DESTINATION_DB.hosts
WHERE
    last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'))
                AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))
GROUP BY
    host_name
HAVING
    TIME(FROM_UNIXTIME(last_check)) > '00:00:00'
) AS tmp;"

execute_query "$INSERT_QUERY2" "Filling data gaps at time 00:00:00 in fascia.hosts." "$DESTINATION_DB"

# Query 3: Filling data gaps at time 23:59:59 using insert query on fascia.hosts
# This query fills data gaps just before midnight (23:59:59) in the fascia.hosts table by inserting missing records.
# Note: This query should be executed in the destination database.

INSERT_QUERY3="INSERT INTO $DESTINATION_DB.hosts (
    last_state_change, last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
)
SELECT
    last_state_change,
    UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59')) AS last_check,
    next_check, last_time_up, last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
FROM (
SELECT
    last_state_change, MAX(last_check) AS last_check, next_check,
    last_time_up, last_time_down, last_update, host_name,
    plugin_output, performance_data, scheduled_downtime_depth, current_state
FROM $DESTINATION_DB.hosts
WHERE
    last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'))
                AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))
GROUP BY
    host_name
HAVING
    TIME(FROM_UNIXTIME(last_check)) < '23:59:59'
) AS tmp;"

execute_query "$INSERT_QUERY3" "Filling data gaps at time 23:59:59 in fascia.hosts." "$DESTINATION_DB"

# End time of script execution
END_TIME=$(date +"%Y-%m-%d %H:%M:%S")
log_message "Script execution ended."

# Calculate and log total execution time
calculate_execution_time

