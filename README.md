# Isogeo Scan FME utils

[![Build Status](https://travis-ci.org/isogeo/isogeo-scanfme-utils.svg?branch=master)](https://travis-ci.org/isogeo/isogeo-scanfme-utils) ![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)

A quicky class to perform some regular operations on Isogeo Scan FME database.

## Features

* get scanned datasets by an Isogeo workgroup
* get installed workers by an Isogeo Workgroup
* export reports in CSV

---

## Setup

### Prerequisites

* Python 3.6+
* Internet connection
* credentials to access Isogeo DB

### Environment

In a Powershell (or Bash) command prompt:

```powershell
py -3 -m venv venv
.\venv3\Scripts\activate  # or source venv/bin/source on Unix OS
pip install --upgrade -r .\requirements.txt
```

### Settings

Rename `settings_TPL.ini` into `settings.ini`:

```powershell
Rename-Item -Path ".\settings_TPL.ini" -NewName "settings.ini"
```

And complete the different parameters (database credentials...).

## Usage

```powershell
python .\scanfme_db_utils.py
```
