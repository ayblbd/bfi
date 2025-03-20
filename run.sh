#!/bin/bash

function log() {
    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    printf "%s - %s\n" "$timestamp" "$*" | tee -a "$LOG_FILE" >&2
}

function to_lower() {
    if [ -z "$1" ]; then
        log "No string provided to convert to lowercase."
        return 1
    fi

    local str="$1"
    tr '[:upper:]' '[:lower:]' <<<"$str"
}

# check_env checks the environment and changes directory based on the environment.
# It takes two arguments: the environment name and the home directory.
function check_env() {
    local environment="${1:-dev}"
    local home_directory=$2
    local environment_lowercase

    # Convert the environment name to lowercase.
    environment_lowercase=$(to_lower "$environment")

    # Check if the environment name contains "dev" or "prod".
    echo "$environment_lowercase"
}

# check_job checks the job name and returns the corresponding script name.
# It takes one argument: the job name.
function check_job() {
    local job_name="${1:-main}"
    local job_name_lowercase

    # Convert the job name to lowercase.
    job_name_lowercase=$(to_lower "$job_name")

    # Check the job name.
    case "$job_name_lowercase" in
    "main" | "bfi")
        log "INFO: Job set to 'bfi.py'"
        echo "$job_directory/bfi.py"
        ;;
    *)
        if [[ -f "$job_directory/$job_name_lowercase.py" ]]; then
            log "INFO: Job set to '$job_name_lowercase.py'"
            echo "$job_directory/$job_name_lowercase.py"
        else
            log "ERROR: Job '$job_name_lowercase.py' does not exist."
            return 1
        fi
        ;;
    esac
}

# check_date checks if the provided date matches the format 'YYYY-MM-DD'.
# It takes one argument: the date.
function check_date() {
    local partition_date="${1:-}"

    # Check if a date was provided.
    if [[ -n "$partition_date" ]]; then
        # Check if the date matches the format 'YYYY-MM-DD'.
        if date -d "$partition_date" >/dev/null 2>&1; then
            log "INFO: Date '$partition_date' is valid"
            echo "$partition_date"
        else
            log "ERROR: partition_date '$partition_date' does not match format 'YYYY-MM-DD'"
            return 1
        fi
    else
        log "INFO: No date provided, using default"
    fi
}

function check_history() {
    local is_history="${1:-}"
    # Check if the value is true or false.
    if [ "$is_history" = true ]; then
        echo true
        log "INFO: is_history value: true"
    else
        echo false
        log "INFO: is_history value: false"
    fi
   
}
# create_logs_directory creates a directory named 'logs' if it does not already exist.
function create_logs_directory() {
    log "INFO: Ensuring $logs_directory directory exists..."
    if ! mkdir -p "$logs_directory"; then
        log "ERROR: Failed to create $logs_directory directory"
        return 1
    else
        log "INFO: $logs_directory directory created or already exists"
    fi
}
# clean_old_logs lists and deletes files older than 7 days in the logs directory.
function clean_old_logs() {
    if [[ ! -d "$logs_directory" ]]; then
        log "ERROR: Directory '$logs_directory' does not exist" >&2
        return 1
    fi

    log "INFO: Listing files older than 7 days in $logs_directory directory:"
    find "$logs_directory" -type f -mtime +7 -ls

    log "INFO: Deleting files older than 7 days in $logs_directory directory."
    find "$logs_directory" -type f -mtime +7 -delete

    log "INFO: Old logs cleanup completed"
}

# log_file generates a log file name based on the provided job name and date.
# It takes two arguments: the job name and the date.
function log_file() {
    local job_name=$1
    local env=$2
    local log_file_name

    # Remove the .py extension from the job name.
    job_name=$(basename "$job_name" .py)

    # Check if the logs directory exists before trying to create a log file in it.
    if [[ ! -d "$logs_directory" ]]; then
        log "ERROR: Directory '$logs_directory' does not exist" >&2
        return 1
    fi

    # Generate the log file name.
    log_file_name="$logs_directory/${job_name}_${env}_$(date +%Y%m%d_%H%M%S).log"

    log "INFO: Log file '$log_file_name' will be created"
    echo "$log_file_name"
}

# tar_gz_src creates a tar.gz archive of all .py files in the src directory.
function tar_gz_src() {
    local src_directory="src"
    local archive_name="src.tar.gz"
    local tar_error

    # Check if the src directory exists before trying to find files in it.
    if [[ ! -d "$src_directory" ]]; then
        log "ERROR: Directory '$src_directory' does not exist" >&2
        return 2
    fi

    log "INFO: Found src directory '$src_directory'"

    # Check if there are any .py files in the src directory.
    if ! find "$src_directory" -name "*.py" -print -quit | grep -q .; then
        log "ERROR: No .py files found in directory '$src_directory'" >&2
        return 3
    fi

    log "INFO: Found .py files in src directory"

    # Create a tar.gz archive of all .py files in the src directory.
    tar_error=$(find "$src_directory" -name "*.py" | tar -czf "$archive_name" -T - 2>&1)
    if [[ $tar_error -ne 0 ]]; then
        log "ERROR: Failed to create archive '$archive_name': $tar_error" >&2
        return 4
    fi

    log "INFO: Successfully created archive '$archive_name'"
}

function check_status() {
    local status=$1
    local function_name=$2

    if [[ $status -ne 0 ]]; then
        log "ERROR: $function_name failed with status $status" >&2
        exit "$status"
    fi
}

run_spark_submit() {
    local job_name=$1
    local partition_date=$2
    local temp_log_file=$3
    local environment=$4
    local is_history=$5
    local log4j_setting="-Dlog4j.configuration=file:log4j.properties"

    (
        spark-submit \
            --master yarn \
            --deploy-mode client \
            --num-executors 4 \
            --driver-memory 16g \
            --executor-memory 16g \
            --conf "spark.driver.extraJavaOptions=${log4j_setting}" \
            --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
            --files log4j.properties,config.ini \
            --conf spark.yarn.dist.archives=src.tar.gz#src \
            --conf spark.yarn.appMasterEnv.PYTHONPATH=src \
            --conf spark.executorEnv.PYTHONPATH=src \
            "$job_name" "$environment" "$partition_date" "$is_history"
    ) 2>&1 | tee -a "$temp_log_file"

    local spark_exit_status=${PIPESTATUS[0]}
    return $spark_exit_status
}

cleanup() {
    if [ -n "$temp_log_file" ] && [ -f "$temp_log_file" ]; then
        if [ -z "$log_file" ]; then
            log_file=$(log_file "$job_name" "$environment")
        fi
        mv "$temp_log_file" "$log_file"
        unset temp_log_file
    fi
}

create_temp_log_file() {
    if ! mkdir -p "$logs_directory"; then
        echo "ERROR: Failed to create $logs_directory directory" >&2
        exit 1
    fi
    temp_log_file=$(mktemp "$logs_directory/${job_name}_${environment}_$(date +%Y%m%d_%H%M%S).XXXXXX")
    LOG_FILE="$temp_log_file"
}

cd_home_dir() {
    local environment="${1:-dev}"
    local home_directory=$2
    local environment_lowercase

    # Convert the environment name to lowercase.
    environment_lowercase=$(to_lower "$environment")

    # Check if the environment name contains "dev" or "prod".
    case "$environment_lowercase" in
    dev*)
        echo "INFO: Changed directory to '$PWD'" >&2
        echo "$PWD"
        ;;
    prd)
        # Check if the home directory exists before trying to cd into it.
        if [[ ! -d "$home_directory" ]]; then
            echo "ERROR: Directory '$home_directory' does not exist" >&2
            exit 2
        fi
        echo "INFO: Changed directory to '$home_directory'" >&2
        echo "$home_directory"
        ;;
    esac
}

function main() {
    job_name=$1
    environment=$2
    partition_date=$3
    is_history=$4

    home_dir=$(cd_home_dir "$environment" "/home/user/spark/bfi")
    cd "$home_dir"

    logs_directory="$home_dir/logs"
    job_directory="$home_dir/src/job"

    create_temp_log_file

    # Set up a trap to ensure logs are saved on exit
    trap cleanup EXIT

    create_logs_directory
    check_status $? "create_logs_directory"

    log "INFO: Starting main function"

    job_name=$(check_job "$job_name")
    check_status $? "check_job"

    environment=$(check_env "$environment" "$home_dir")
    check_status $? "check_env"

    partition_date=$(check_date "$partition_date")
    check_status $? "check_date"

    is_history=$(check_history "$is_history")
    check_status $? "check_history"

    tar_gz_src "$home_dir"
    check_status $? "tar_gz_src"

    log_file=$(log_file "$job_name" "$environment")
    check_status $? "log_file"

    clean_old_logs
    check_status $? "clean_old_logs"

    log "INFO: Starting spark-submit command"

    run_spark_submit "$job_name" "$partition_date" "$temp_log_file" "$environment" "$is_history"
    check_status $? "run_spark_submit"

    log "INFO: Finished main function"
}

main "$@"