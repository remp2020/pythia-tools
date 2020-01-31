# Data Aggregation for Pythia

Aggregation script exporting Elastic data to gziped csv files and aggregating them to PostgreSQL DB for Pythia processing.

## Requirements

`python2` and `pip2` are required because of support for `es2csv` package used to export the data. 

All other required Python packages are described in `requirements.txt` file and are handled by the following installation steps.. 

## Installation

```
pip2 install --user virtualenv
python2 -m virtualenv .virtualenv
source .virtualenv/bin/activate
pip install -r requirements.txt
```

Create `.env` file based on `.env.example` file and fill the configuration options.

## Usage

Run aggregation for particular `--date` (YYYY-MM-DD) and specify to which `--dir` you want to export data, for example:

```
./run.sh --date=2020-01-14 --dir=/data/local/archive
```

Following options are available:

* If you want to only export the data and not aggregate them, use `--dryrun` flag.
* If you want to use multiple `.env` files, you can specify it via `--env` option. If not provided, `.env` is used.

```
./run.sh --date=2020-01-14 --dry-run --dir=/data/local/archive --env=.env.production
```