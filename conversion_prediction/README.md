# Conversion Prediction

## Instalation

#### System prerequisities (Ubuntu)

```bash
apt update

apt install libmysqlclient-dev python3-dev libblas-dev liblapack-dev
# dnf install lapack-devel blas-devel

# numpy is causing some issues when building the package
ln -s /usr/include/locale.h /usr/include/xlocale.h

```

#### Instalation

```bash
python3 -m venv .virtualenv
source .virtualenv/bin/activate
pip3 install -r requirements.txt
```