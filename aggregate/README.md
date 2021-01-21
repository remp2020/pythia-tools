# Data Aggregation for Pythia

Aggregation script exporting Elasticsearch data to gziped CSV files and aggregating them to [BigQuery database](https://cloud.google.com/bigquery) for further Pythia processing.

## Requirements

`python3` and `pip3` are required and also instance of [BigQuery](https://cloud.google.com/bigquery).

All other required Python packages are described in `requirements.txt` file and are handled by the following installation steps..

## Installation

```
python3 -m venv .virtualenv
source .virtualenv/bin/activate
pip3 install -r requirements.txt
```

Create `.env` file based on `.env.example` file and fill the configuration options.

## Usage

Run aggregation for particular `--date` (YYYY-MM-DD) and specify to which `--dir` you want to export data, for example:

```
./run.sh --date=2020-01-14 --dir=/data/local/archive
```

Following options are available:

* If you want to only export the data and not aggregate them, use `--dryrun` flag.
* If you only want to aggregate data to Postgres from existing `csv.gz` files and but not to export data from Elastic, use `--onlyaggregate` flag.
* If you want to use multiple `.env` files, you can specify it via `--env` option. If not provided, `.env` is used.
* To specify where to store compressed CSV files, use `--dir=<DIR>` option. If not provided, current directory is used.
* To specify where to save extracted CSV files (for processing and aggregation, will be deleted afterwards), use `--tmp=<DIR>` option. If not provided,  current directory is used.

```
./run.sh --date=2020-01-14 --dry-run --dir=/data/local/archive --env=.env.production
```
