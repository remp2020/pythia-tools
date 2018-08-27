#!/usr/bin/env bash

function download_files_for_day {
    file_date=$1
    # sometimes download may fail, therefore try at least 10 times
    retries=0
    max_tries=10
    pageviews_file=pageviews_${file_date}.csv.gz
    while [[ (! -f $pageviews_file) && ("$retries" < "$max_tries") ]]
    do
      gdrive download query "name='${pageviews_file}'"
      retries=$((retries+1))
    done

    retries=0
    pageviews_timespent_file=pageviews_time_spent_${file_date}.csv.gz
    while [[ (! -f $pageviews_timespent_file) && ("$retries" < "$max_tries") ]]
    do
      gdrive download query "name='${pageviews_timespent_file}'"
      retries=$((retries+1))
    done

    retries=0
    commerce_file=commerce_${file_date}.csv.gz
    while [[ (! -f $commerce_file) && ("$retries" < "$max_tries") ]]
    do
      gdrive download query "name='${commerce_file}'"
      retries=$((retries+1))
    done

    gzip -d pageviews_${file_date}.csv.gz
    gzip -d pageviews_time_spent_${file_date}.csv.gz
    gzip -d commerce_${file_date}.csv.gz
}

function remove_files {
    file_date=$1
    rm pageviews_${file_date}.csv pageviews_time_spent_${file_date}.csv commerce_${file_date}.csv
}

function aggregate_day {
    file_date=$1
    download_from_gdrive=$2
    # remove dashes from date format
    file_date=${file_date//-/}

    echo $file_date

    if [ "$download_from_gdrive" = true ] ; then
        download_files_for_day $file_date
    fi

    pipenv run python aggregate.py ${file_date}
    pipenv run python conversion_events.py ${file_date}

    if [ "$download_from_gdrive" = true ] ; then
        remove_files $file_date
    fi
}

function usage {
    echo "Usage: $0 <specific_date> <-d>" >&2
    echo "Date format is YYYY-MM-DD" >&2
    echo "-d: download files from gdrive: " >&2
}

# check 1 or 2 parameters
if [ "$#" -ne 1 ] && [ "$#" -ne 2 ]; then
    usage
    exit 1
fi

#
if [ "$#" -eq 1 ]; then
    aggregate_day $1 false
else
    aggregate_day $1 true
fi