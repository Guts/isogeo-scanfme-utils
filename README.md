# Isogeo Scan FME utils

[![Build Status](https://travis-ci.org/Guts/isogeo-scanfme-utils.svg?branch=master)](https://travis-ci.org/Guts/isogeo-scanfme-utils) ![Python 3.5](https://img.shields.io/badge/python-3.5-blue.svg) ![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)

A quicky class to perform some regular operations on Isogeo Scan FME database.

## Setup

### Prerequisites

* Python 3.5+
* Internet connection

### Environment

In a Powershell (or Bash) command prompt:

```powershell
py -3 -m venv venv
.\venv3\Scripts\activate  # or source venv/bin/source on Unix OS
pip install --upgrade -r .\requirements.txt
```

### Settings

Rename `settings_TPL.ini`into `settings.ini`:

```powershell
Rename-Item -Path ".\settings_TPL.ini" -NewName "settings.ini"
```

And complete the different parameters.

## Usage

TO DOC
