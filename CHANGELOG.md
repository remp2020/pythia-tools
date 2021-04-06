# Changelog

## [Unreleased]

### [Aggregate]

- Aggregation script rewritten to use BigQuery cloud storage instead of PostgreSQL for data aggregation.

### [Conversion prediction]

- Major rewrite of the script to work with BiqQuery database.

### [Churn prediction]

- Initial version of churn prediction pipeline based on BigQuery.

## 1.0.1 - 2020-02-01

### [Aggregate]

- Updated dependencies in `requirements.txt`

### [Conversion prediction]

- Added batch processing for `predict` task.
- Moved date range checking condition to only affect `train` task.
- Updated and cleaned up dependencies in `requirements.txt`.
- Improve column alignment between model and prediction data.
- Changed default aggregation function to a single function to speed up training and prediction.

## 1.0.0 - 2020-01-31

First major release after beta 18 months ago. We'll keep the track of changes from now on here.

### [Conversion prediction]

- Added tons of testing features and improved model since last beta release.

---

[Aggregate]: https://github.com/remp2020/pythia-tools/tree/master/aggregate
[Conversion prediction]: https://github.com/remp2020/pythia-tools/tree/master/conversion_prediction
[Churn prediction]: https://github.com/remp2020/pythia-tools/tree/master/churn_prediction
[Pythia segments]: https://github.com/remp2020/pythia-tools/tree/master/pythia_segments
