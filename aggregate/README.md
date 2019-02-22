###Data Aggregation for Pythia

Aggregation script moving data from Elastic storage to PostgreSQL DB for Pythia processing.

####Requirements
Python 2 is only supported (because of es2csv). 
All required Python packages are described in `requirements.txt` file. 

####Installation
First install all requirements using pip:

```
pip install --user virtualenv
python2 -m virtualenv .virtualenv
source .virtualenv/bin/activate
pip install -r requirements.txt
```

Script requires connection variables for PostreSQL DB and address of Elastic storage, 
put these to `.env` file according to `.env.example` format.

####Usage
Run aggregation for particular day using:

`./run.sh <date>`

Date format is YYYY-MM-DD.