#!/usr/bin/env bash

function usage {
    echo "Aggregation script moving data from Elastic storage to PostgreSQL DB for Pythia processing"
    echo "Usage: $0 --min_date=<DATE> --max_date=<DATE> | $0 --date=<DATE>" >&2
    echo "Optional argument is --dir=<DIR>, specifying where to look for/save aggregated elastic CSV files" >&2
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
    --dir=*)
      dir="${1#*=}"
      ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done

if ! [ -x "$(command -v es2csv)" ]; then
    echo 'Error: es2csv is not installed.' >&2
    exit 1
fi

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

if [ ! -z $dir ]; then
    if [ ! -e "$dir" ]; then
        echo "Directory $dir does not exist"
        exit 4
    elif [ ! -d "$dir" ]; then
        echo "$dir is not a directory"
        exit 5
    fi
else
    dir=$(pwd)
fi


export $(grep -v '^#' .env | xargs)

echo "Searching for aggregated files in $dir"

files=("pageviews_time_spent" "pageviews" "commerce")

# For every date, aggregate CSV files into Postgre (optionally download CSV files from elastic)
while [ "$di" != "$end_on" ]; do
    file_date=${di//-/}

    for idx in "${files[@]}"; do
        echo "Processing ${idx}, date: ${di}"
        cur_dir="$dir/$idx"
        mkdir -p $cur_dir # create directory if does not exist
        cur_file_gz="${cur_dir}/${idx}_${file_date}.csv.gz"
        cur_file_csv="${cur_dir}/${idx}_${file_date}.csv"

        if [ ! -f $cur_file_gz ]; then
            # aggregate CSV file from elastic
            es2csv -u $ELASTIC_ADDR -i "${idx}" -q "time: [ ${di} TO ${di} ]" -o $cur_file_csv
            # pack file to .gz
            gzip -k -f $cur_file_csv
        else
            # unpack .csv.gz file
            gzip -k -d $cur_file_gz
        fi
    done

    # Run aggregation
    python utils/aggregate.py ${file_date} --dir=$dir
    python utils/conversion_events.py ${file_date} --dir=$dir

    # Delete csv files
    for idx in "${files[@]}"; do
        cur_dir="$dir/$idx"
        cur_file_csv="${cur_dir}/${idx}_${file_date}.csv"
        rm $cur_file_csv
    done

    di=$(add_day $di)
done