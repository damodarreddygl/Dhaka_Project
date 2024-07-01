<?php
// Purpose: This script transfers data between databases and fills data gaps in the specified time intervals for the Fascia system.
// Testing Phase: Phase 1

// Log file path
$logFile = "/tmp/Db-To-Db-error_log1.txt";

// Function to log messages with timestamps
function log_message($message) {
    global $logFile;
    $timestamp = date("Y-m-d H:i:s");
    file_put_contents($logFile, "[$timestamp] - $message\n", FILE_APPEND);
}

// Function to execute SQL query and log errors if any
function execute_query($query, $errorMsg, $database, $conn) {
    log_message("Executing query: $errorMsg");
    log_message("Query execution started.");
    $startQueryTime = date("Y-m-d H:i:s");

    $result = $conn->query($query);
    if (!$result) {
        log_message("Error occurred: $errorMsg");
        log_message("Error details: " . $conn->error);
    } else {
        log_message("Success: $errorMsg");
        if (strpos($errorMsg, "Counting entries") !== false) {
            log_message("+----------+---------------------------+---------------------+---------------------+");
            log_message("| count(*) |     count(host_name)      |         min         |         max         |");
            log_message("+----------+---------------------------+---------------------+---------------------+");
            while ($row = $result->fetch_assoc()) {
                log_message(sprintf("| %-8s | %-25s | %-19s | %-19s |", $row["count(*)"], $row["count(DISTINCT host_name)"], $row["min"], $row["max"]));
            }
            log_message("+----------+---------------------------+---------------------+---------------------+");
        } else {
            log_message("Query Result:");
            while ($row = $result->fetch_assoc()) {
                log_message(print_r($row, true));
            }
        }
    }

    $endQueryTime = date("Y-m-d H:i:s");
    log_message("Query execution ended.");
    log_message("Query execution time: Started at $startQueryTime, Ended at $endQueryTime.");
}

// Function to calculate total execution time
function calculate_execution_time($startTime, $endTime) {
    $startSeconds = strtotime($startTime);
    $endSeconds = strtotime($endTime);
    $executionTime = $endSeconds - $startSeconds;

    $formattedTime = gmdate("H:i:s", $executionTime);
    log_message("Total execution time: $formattedTime");
}

// Start time of script execution
$startTime = date("Y-m-d H:i:s");

// Add empty line before first log entry
file_put_contents($logFile, "\n", FILE_APPEND);

log_message("Script execution started.");

// MySQL/MariaDB connection parameters
$user = "root";
$password = "USN7ETS0510SEC3030";
$host = "127.0.0.1";
$sourceDb = "fascia3";
$destinationDb = "fascia1";

// Create connection
$conn = new mysqli($host, $user, $password);

// Check connection
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}

// Query 1: Transferring data from Fascia3.hosts to Fascia.hosts using insert query
$insertQuery1 = "INSERT INTO $destinationDb.hosts (
    last_state_change, last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
)
SELECT
    last_state_change, last_check, next_check, last_time_up,
    last_time_down, last_update, host_name, plugin_output,
    performance_data, scheduled_downtime_depth, current_state
FROM $sourceDb.hosts
WHERE last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 12:00:01'))
      AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))";

execute_query($insertQuery1, "Transferring data from Fascia3.hosts to Fascia.hosts.", $sourceDb, $conn);

// Query 1.2.1: Counting the number of entries transferred from Fascia3.hosts to Fascia.hosts
$countQuery = "SELECT COUNT(*), COUNT(DISTINCT host_name), FROM_UNIXTIME(MIN(last_check)) AS min, FROM_UNIXTIME(MAX(last_check)) AS max FROM $destinationDb.hosts
                WHERE last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 12:00:01'))
                                        AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))";

execute_query($countQuery, "Counting entries from Fascia3.hosts to Fascia.hosts.", $destinationDb, $conn);

// Query 1.2.0: Additional count query from Fascia3.hosts for reference
$countQuery2 = "SELECT COUNT(*), COUNT(DISTINCT host_name), FROM_UNIXTIME(MIN(last_check)) AS min, FROM_UNIXTIME(MAX(last_check)) AS max FROM $sourceDb.hosts
                WHERE last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 12:00:01'))
                                AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))";

execute_query($countQuery2, "Counting entries from Fascia3.hosts.", $sourceDb, $conn);

// Query 2: Filling data gaps at time 23:59:59 using insert query on fascia.hosts
$insertQuery2 = "INSERT INTO $destinationDb.hosts (
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
FROM $destinationDb.hosts
WHERE
    last_check BETWEEN UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 12:00:00'))
                AND UNIX_TIMESTAMP(DATE_FORMAT(CURDATE() - INTERVAL 1 DAY, '%Y-%m-%d 23:59:59'))
GROUP BY
    host_name
HAVING
    TIME(FROM_UNIXTIME(last_check)) < '23:59:59'
) AS tmp;";

execute_query($insertQuery2, "Filling data gaps at time 23:59:59 in fascia.hosts.", $destinationDb, $conn);

// End time of script execution
$endTime = date("Y-m-d H:i:s");
log_message("Script execution ended.");

// Calculate and log total execution time
calculate_execution_time($startTime, $endTime);

$conn->close();
?>
