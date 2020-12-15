#!/usr/bin/env bash

export PATH=".virtualenv/bin:$PATH"

function usage {
    echo "Script exporting PostgreSQL aggregated data to BigQuery Google Cloud storage for further processing"
    echo "Usage: $0 --min_date=<DATE> --max_date=<DATE> | $0 --date=<DATE>" >&2
    echo "Optional arguments:"
    echo "  --env=<FILE>, specifying .env file for sourcing" >&2
    echo "Date format is YYYY-MM-DD" >&2
}

function check_valid_date {
    if ! [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        false
    else
        true
    fi
}

function add_day {
    # MacOS platform has different implementation of date (according to BSD),
    # use gdate which is an alternative (from coreutils) compatible with Linux date command
    if [ "$(uname)" == "Darwin" ]; then
        day=$(gdate -I -d "$1 + 1 day")
    else
        day=$(date -I -d "$1 + 1 day")
    fi
    echo $day
}

# Load arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --min_date=*)
      min_date="${1#*=}"
      ;;
    --max_date=*)
      max_date="${1#*=}"
      ;;
    --date=*)
      date="${1#*=}"
      ;;
    --env=*)
      env="${1#*=}"
      ;;
    --)
      break
      ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done

# Required arguments check
if [ -z $date ] && ([ -z $min_date ] || [ -z $max_date ]) ; then
    usage
    exit 1
fi

# Arguments validation
if [ ! -z $date ]; then
    if ! check_valid_date $date; then
        echo "Date $date is in an invalid format (not YYYY-MM-DD)."
    fi

    di=$date
    end_on=$(add_day $date)
fi

if [ ! -z $min_date ]; then
    if ! check_valid_date $min_date; then
        echo "Date $min_date is in an invalid format (not YYYY-MM-DD)."
    fi

    if ! check_valid_date $max_date; then
        echo "Date $max_date is in an invalid format (not YYYY-MM-DD)."
    fi

    di=$min_date
    end_on=$(add_day $max_date)
fi

if [ -z $env ]; then
    env=".env"
fi

echo "Sourcing environment variables from $env"
export $(grep -v '^#' $env | xargs)


while [ "$di" != "$end_on" ]; do
    file_date=${di//-/}
    echo "Exporting CSV files for $di"

    # Connection settings are passed automatically from .env file variables
    psql -c "\copy (SELECT * FROM browsers WHERE date = '$di') TO './csv/browsers_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM browser_users WHERE date = '$di') TO './csv/browser_users_$file_date.csv' DELIMITER '|' csv HEADER"

    psql -c "\copy (SELECT * FROM aggregated_browser_days WHERE date = '$di') TO './csv/aggregated_browser_days_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM aggregated_browser_days_tags WHERE date = '$di') TO './csv/aggregated_browser_days_tags_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM aggregated_browser_days_categories WHERE date = '$di') TO './csv/aggregated_browser_days_categories_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM aggregated_browser_days_referer_mediums WHERE date = '$di') TO './csv/aggregated_browser_days_referer_mediums_$file_date.csv' DELIMITER '|' csv HEADER"

    psql -c "\copy (SELECT * FROM aggregated_user_days WHERE date = '$di') TO './csv/aggregated_user_days_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM aggregated_user_days_tags WHERE date = '$di') TO './csv/aggregated_user_days_tags_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM aggregated_user_days_categories WHERE date = '$di') TO './csv/aggregated_user_days_categories_$file_date.csv' DELIMITER '|' csv HEADER"
    psql -c "\copy (SELECT * FROM aggregated_user_days_referer_mediums WHERE date = '$di') TO './csv/aggregated_user_days_referer_mediums_$file_date.csv' DELIMITER '|' csv HEADER"

    psql -c "\copy (SELECT user_id, browser_id, time, type FROM events WHERE computed_for = '$di') TO './csv/events_$file_date.csv' DELIMITER '|' csv HEADER"

    echo "Running upload to BigQuery for $di"

    python upload.py ${file_date}

    # delete temporary csv files
    rm csv/*.csv

    di=$(add_day $di)
done