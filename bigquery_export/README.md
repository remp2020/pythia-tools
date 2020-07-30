# Pythia BigQuery export

Script for uploading aggregated data (from [Aggregation of data](../cmd/aggregate) script) to [BigQuery database](https://cloud.google.com/bigquery). This is required step for further processing like building a churn prediction model.

## Requirements

`python3` and `pip3` are required and also instance of [BigQuery](https://cloud.google.com/bigquery).

All other required Python packages are described in `requirements.txt` file and are handled by the following installation steps. 

## Installation

```
python3 -m venv .virtualenv;
source .virtualenv/bin/activate
pip3 install -r requirements.txt
```

Create `.env` file based on `.env.example` file and fill the configuration options.

## Usage

Run export for particular date by specifying `--date` option (or date range using `--min_date` and `--max_date`). All options accepts `YYYY-MM-DD` date format.

```
./run.sh --date=2020-01-14 
```

Following options are available:

* If you want to specify which `.env` file to use, set it via `--env` option. If not provided, `.env` is used.