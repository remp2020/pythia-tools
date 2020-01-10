# Conversion Prediction

## Preparation

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

## Running

```bash
python run.py --min-date=$(date -d "yesterday" --rfc-3339=date) --action 'train' 
```