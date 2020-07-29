# Pythia tools

Pythia is a set of tools that work with REMP and CRM data and allows to:
  
- Predict probability of users conversion and expose this information by creating segments (of low/high conversion probability) for other parts of the system (Beam/Campaign/CRM).
- Predict probability of users churn and store this information in BigQuery database (which can be later used by other tools,  e.g. CRM). 
 
Pythia works with different data sources - all of these are mandatory at this point:

- Beam pageviews data (stored in Elastic via JavaScript snippet)
- CRM payments and subscriptions data (stored in MySQL of CRM)
- Conversion/churn model is computed on aggregated data exported to BigQuery (Google Cloud service)

The whole process is split to several steps:

- [Aggregation of data](cmd/aggregate)
- [Export of aggregated data to BigQuery](cmd/bigquery_export)
- [Conversion model training and prediction](cmd/conversion_prediction)
- [Churn model training and prediction](cmd/churn_prediction)
- [Expoting generated data as segments for other applications via API](cmd/pythia_segments)

For installation and running instructions, please see README files of linked services.

To run Pythia tools, you'll need `python2`, `python3`, `Postgresql 12` and `BigQuery`.