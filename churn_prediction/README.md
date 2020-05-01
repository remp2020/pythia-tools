# Pythia Conversion Prediction

Script generating conversion prediction models based on data aggregated by `cmd/aggregate`, data stored in Beam's MySQL databaze and data provided by CRM MySQL database.

Once the prediction models are generated, script is writing list of users to PostgreSQL database with the probability of their conversion. This list is a requirement for `cmd/pythia_segments` API which can be integrated into REMP Campaign and REMP Beam to target users.

## Requirements

#### System prerequisities (Ubuntu)

```bash
apt update
apt install libmysqlclient-dev python3-dev libblas-dev liblapack-dev

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

Create `.env` file based on `.env.example` file and fill the configuration options.

## Usage

If you don't have the model files generated yet, let's *train* the models first:

```bash
python run.py --min-date=$(date -d "yesterday" --rfc-3339=date) --action 'train' 
```