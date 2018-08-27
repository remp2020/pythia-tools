###Data aggregation for Pythia

####Installation
First install `pipenv` using `pip instal --user pipenv`
Next install all vendor requirements using `pipenv install`
Script requires connection to PostreSQL DB, credentials should be specified in `.env` file.

####Usage
Script works by downloading exported data from Google Drive (you will be asked to provide authentication token during first run), 
extracting, aggregating it and saving results to database.

Script can be run in two variants:
`./aggregate.sh <date>`
`./aggregate.sh <start_date> <end_date>`
Date format is YYYY-MM-DD.