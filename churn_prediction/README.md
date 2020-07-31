# Pythia Churn Prediction

Script generating churn prediction models based on data aggregated by `cmd/aggregate` and exported to BigQuery using `cmd/bigquery_export`, data stored in Beam's MySQL database and data provided by CRM MySQL database.

## Requirements

#### System prerequisities (Ubuntu)

```bash
apt update
apt install libmysqlclient-dev python3-dev libblas-dev liblapack-dev python3-venv

# numpy is causing some issues when building the package
ln -s /usr/include/locale.h /usr/include/xlocale.h

```

#### System prerequisities (Fedora)

```bash
dnf update
dnf install lapack-devel blas-devel

# numpy is causing some issues when building the package
ln -s /usr/include/locale.h /usr/include/xlocale.h
```

#### System prerequisities (Manjaro)

```bash
pacman -Syu
pacman -Sy gcc-fortran lapack blas

# numpy is causing some issues when building the package
ln -s /usr/include/locale.h /usr/include/xlocale.h
```

## Instalation

```bash
python3 -m venv .virtualenv
source .virtualenv/bin/activate
pip3 install -r requirements.txt
```

Create `.env` file based on `.env.example` file and fill the configuration options based on the example values in the `.env.example` file.

## Usage

Churn prediction works on train/predict flow. First, you need to *train* your models based on the collected data. Once the models are ready, you can *predict* the outcome of behavior of your users.

We recommend to train your models periodically, so they reflect *current* behavior of your users.

Following commands assume that you already run `cmd/aggregate` to aggregate raw pageviews data collected by Beam, and pushed aggregated data to BigQuery via `cmd/bigquery_uploader`. If you haven't please see their respective README files first.

### Preaggregate

Preaggregate transforms aggregated user data to the rolling user profiles to speed up model training and prediction. Even if you don't train your models daily, you should *preaggregate* the data as often as you *predict* the outcomes.

```bash
python run.py --action=preaggregate --min-date='2020-03-05' --max-date='2020-03-05'
```

The output of script should be similar to ours:

```
2020-07-26 14:32:28,387 [INFO] __main__ - CHURN PREDICTION
2020-07-26 14:32:30,561 [INFO] __main__ - Table rolling_daily_user_profile already exists
2020-07-26 14:32:30,561 [INFO] __main__ - Starting with preaggregation for date range 2020-03-05 - 2020-03-05
22504 out of 22504 rows loaded.:32:44,284 [INFO] pandas_gbq.gbq - 
1it [00:04,  4.08s/it]
2020-07-26 14:33:49,650 [INFO] __main__ - Date 2020-03-05 00:00:00 succesfully aggregated & uploaded to BQ
```

### Train

If you don't have the model files generated yet, let's *train* the models first. 

```bash
python run.py --min-date=$(date -d "30 days ago" --rfc-3339=date) --action 'train' 
```

The output of script should be similar to ours:

```
2020-07-03 14:59:03,102 [INFO] __main__ - CHURN PREDICTION
2020-07-03 14:59:04,909 [INFO] __main__ - Executing training pipeline
2020-07-03 14:59:04,910 [INFO] __main__ -   * Loading user profiles
2020-07-03 15:01:55,318 [INFO] __main__ -   * Processing user profiles
2020-07-03 15:03:29,790 [INFO] __main__ -   * Filtering user profiles
2020-07-03 15:03:29,858 [INFO] __main__ -   * Normalizing user profiles
2020-07-03 15:03:31,012 [INFO] __main__ -   * Feature normalization success
2020-07-03 15:03:31,012 [INFO] __main__ -   * Query finished, processing retrieved data
2020-07-03 15:03:31,012 [INFO] __main__ -   * Retrieved initial user profiles frame from DB
2020-07-03 15:03:31,991 [INFO] __main__ - Successfully added global context features from mysql
2020-07-03 15:03:36,589 [INFO] __main__ - Successfully added user payment history features from mysql
/home/rootpd/gospace/src/gitlab.com/remp/pythia/cmd/churn_prediction/.virtualenv/lib/python3.8/site-packages/pandas/core/frame.py:4252: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame

See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  return super().fillna(
2020-07-03 15:03:36,692 [INFO] __main__ -   * Initial data validation success
run.py:396: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame

See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  data[~(data.isin(self.category_list_dict[data.name]))] = 'Unknown'
2020-07-03 15:03:37,032 [INFO] __main__ -   * Dummy variables generation success
2020-07-03 15:03:37,380 [INFO] __main__ -   * Numeric variables handling success
2020-07-03 15:03:37,503 [INFO] __main__ -   * user_profiles artifact dropped
2020-07-03 15:03:37,606 [INFO] __main__ -   * Commencing model training
2020-07-03 15:04:26,009 [INFO] __main__ -   * Model training complete, generating outcome frame
2020-07-03 15:04:29,970 [INFO] __main__ -   * Outcome frame generated
2020-07-03 15:04:29,970 [INFO] __main__ - Training ready, dumping to file
2020-07-03 15:04:30,208 [INFO] __main__ - Saved to /home/rootpd/workspace/pythia/models/model_2020-02-03.pkl
2020-07-03 15:04:30,208 [INFO] __main__ -   * X_train artifact dropped
2020-07-03 15:04:30,208 [INFO] __main__ -   * Y_train artifact dropped
2020-07-03 15:04:30,209 [INFO] __main__ -   * X_test artifact dropped
2020-07-03 15:04:30,209 [INFO] __main__ -   * Y_test artifact dropped
{'precision': {'churn_train': 0.99258114374034, 'renewal_train': 0.9911504424778761, 'churn_test': 0.6848184818481848, 'renewal_test': 0.8695103255478402}, 'recall': {'churn_train': 0.9488770685579196, 'renewal_train': 0.9987628865979381, 'churn_test': 0.12541553339377456, 'renewal_test': 0.9901925545571245}, 'f1_score': {'churn_train': 0.97023719595105, 'renewal_train': 0.9949421037767336, 'churn_test': 0.21200510855683274, 'renewal_test': 0.9259357069118671}, 'suport': {'churn_train': 3384.0, 'renewal_train': 19400.0, 'churn_test': 3309.0, 'renewal_test': 19475.0}}
```

Output of the script will be `.pkl` file in the folder you specified in `.env` file with `PATH_TO_MODEL_FILES` variable. The file contains model data trained based on the behavior of your users. It will be used to predict the churn probability in the next step.

### Predict

With models trained, you can now run the prediction for users that are being close to the churn/renewal.

### Next steps

Resulting data is a churn/renewal prediction for selected users with probability of the predicted action. You can find raw output in the BigQuery's `churn_predictions_log` table.

Output of this prediction can be fetched back to the CRM by CRM's [REMP Pythia Module](https://github.com/remp2020/crm-remp-pythia-module). Predictions can be displayed in widgets across the system and also utilized in segments so you can target campaigns to selected users.