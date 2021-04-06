# Installation

This guide covers installation of Churn and Conversion prediction tools.
Aggregate tool has its own it's installation guide in `aggregate/README.md` file.

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

Please read the usage guides in the script folders:

- `conversion_prediction/README.md`
- `churn_prediction/README.md`

