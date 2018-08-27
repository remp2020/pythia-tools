#!/usr/bin/env bash

function usage {
    echo "Aggregation script moving data from Elastic storage to PostgreSQL DB for Pythia processing"
    echo "Usage: $0 <DATE>" >&2
    echo "Date format is YYYY-MM-DD" >&2
}

if [ "$#" -ne 1 ]; then
    usage
    exit 1
fi

if ! [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Date $1 is in an invalid format (not YYYY-MM-DD)."
    exit 1
fi

if ! [ -x "$(command -v es2csv)" ]; then
    echo 'Error: es2csv is not installed.' >&2
    exit 1
fi

export $(grep -v '^#' .env | xargs)

range_start=$1
file_date=${range_start//-/}

files=("pageviews_time_spent" "pageviews" "commerce")
#for idx in "${files[@]}"; do
#    echo "Processing ${idx}, date: ${range_start}"
#    es2csv -u $ELASTIC_ADDR -i "${idx}" -q "time: [ ${range_start} TO ${range_start} ]" -o ${idx}_${file_date}.csv
#done

python utils/aggregate.py ${file_date}
python utils/conversion_events.py ${file_date}

for idx in "${files[@]}"; do
    rm ${idx}_${file_date}.csv
done