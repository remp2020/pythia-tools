#!/usr/bin/env bash

export PATH=".virtualenv/bin:$PATH"

function usage {
    echo "Aggregation script processing data from Elastic storage to PostgreSQL DB for Pythia processing"
    echo "Usage: $0 --min_date=<DATE> --max_date=<DATE> | $0 --date=<DATE>" >&2
    echo "Optional arguments:"
    echo "  --dir=<DIR>, specifying where to look for/save aggregated (.gz) elastic CSV files" >&2
    echo "  --tmp=<DIR>, specifying where to extract/process CSV files" >&2
    echo "  --env=<FILE>, specifying .env file for sourcing" >&2
    echo "  --onlyaggregate, specifying not to download data from Elastic if not present" >&2
    echo "  --dryrun, specifying to check/save the CSVs, but prevent execution of aggregation" >&2
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
    --tmp=*)
      tmp="${1#*=}"
      ;;
    --dryrun)
      dryrun=1
      ;;
    --onlyaggregate)
      onlyaggregate=1
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

if [ ! -z $tmp ]; then
    if [ ! -e "$tmp" ]; then
        echo "Directory $tmp does not exist"
        exit 4
    elif [ ! -d "$tmp" ]; then
        echo "$tmp is not a directory"
        exit 5
    fi
else
    tmp=$dir
fi

if [ -z $env ]; then
    env=".env"
fi


echo "Sourcing environment variables from $env"
export $(grep -v '^#' $env | xargs)

echo "Searching for aggregated files in $dir"

files=("pageviews_time_spent" "pageviews" "pageviews_progress" "commerce" "events")

# For every date, aggregate CSV files into Postgres (optionally download CSV files from elastic)
while [ "$di" != "$end_on" ]; do
    file_date=${di//-/}
    skip_date=0

    for idx in "${files[@]}"; do
        echo "Processing ${idx}, date: ${di}"
        cur_dir="$dir/$idx"
        cur_tmp_dir="$tmp/$idx"
        mkdir -p $cur_tmp_dir # create directory if does not exist
        cur_file_gz="${cur_dir}/${idx}_${file_date}.csv.gz"
        cur_file_csv="${cur_tmp_dir}/${idx}_${file_date}.csv"

        if [ ! -f $cur_file_gz ]; then

            if [ -n "$onlyaggregate" ]; then
                echo "File ${cur_file_gz} not found, --onlyaggregate mode is turned on, skipping the date"
                skip_date=1
                break
            fi

            echo "File ${cur_file_gz} not found, downloading from Elastic ($ELASTIC_ADDR): ${idx} [ ${di} TO ${di} ]"
            # aggregate CSV file from elastic
            if [ -z "$ELASTIC_AUTH" ]; then
                authstring=''
            else
                authstring="-a $ELASTIC_AUTH"
            fi

            indexname="${ELASTIC_PREFIX}${idx}"

            echo "es2csv -u ${ELASTIC_ADDR} ${authstring} -i ${indexname} -q \"time: [ ${di} TO ${di} ]\" -s 10000 -o ${cur_file_csv}"

            es2csv -u $ELASTIC_ADDR $authstring -i "${indexname}" -q "time: [ ${di} TO ${di} ]" -s 10000 -o $cur_file_csv
            # pack file to .gz if it was downloaded (at least one record was present for the day)
            if [ -f $cur_file_csv ]; then
                gzip -k -f -c $cur_file_csv > $cur_file_gz
            fi
        else
            # unpack .csv.gz file
            gzip -k -f -d -c $cur_file_gz > $cur_file_csv
        fi
    done

    if [ "$skip_date" -eq "1" ]; then
        di=$(add_day $di)
        continue
    fi

    # Run aggregation
    if [ -z $dryrun ]; then
        python utils/aggregate.py ${file_date} --dir=$tmp
        python utils/conversion_and_commerce_events.py ${file_date} --dir=$tmp
        python utils/subscriptions_churn_events.py ${file_date}
    fi

    # Delete csv files
    for idx in "${files[@]}"; do
        cur_dir="$tmp/$idx"
        cur_file_csv="${cur_dir}/${idx}_${file_date}.csv"
        if [ -f $cur_file_csv ]; then
        rm $cur_file_csv
        fi
    done

    di=$(add_day $di)
done