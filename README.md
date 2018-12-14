# Isogeo Scan FME utils

[![Build Status](https://travis-ci.org/isogeo/isogeo-scanfme-utils.svg?branch=master)](https://travis-ci.org/isogeo/isogeo-scanfme-utils) ![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)

A quicky class to perform some regular operations on Isogeo Scan FME database.

## Features

* make a report about the Isogeo Worker (Scan FME) installations
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
pipenv install
```

### Settings

Rename `settings_TPL.ini` into `settings.ini`:

```powershell
Rename-Item -Path ".\settings_TPL.ini" -NewName "settings.ini"
```

And complete the different parameters (database credentials...).

## Usage

### Generate a report on the whole database

Useful to get an overview on the installed versions.

```powershell
python .\cli_report_global.py
```

### Generate a report on a specific workgroup

Useful for support issues.

```powershell
python .\cli_report_workgroup.py --workgroup [workgroup UUID]
```
