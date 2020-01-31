# Pythia tools

This is a set of tools to aggregate, predict and create segment of users based on their probability of converting. Pythia tools work with different data sources - all of these are mandatory at this point:

- Beam pageviews data (stored in Elastic via javascript snippet)
- CRM payments and subscriptions data (stored in MySQL of CRM)

The whole process is split to three steps:

- [Aggregation of data](cmd/aggregate)
- [Conversion model training and prediction](cmd/conversion_prediction)
- [Exposting generated data as segments for other applications via API](cmd/pythia_segments)

For installation and running instructions, please see README files of linked services.

To run Pythia tools, you'll need `python2`, `python3` and `Postgresql 12`.