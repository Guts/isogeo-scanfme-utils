# -*- coding: UTF-8 -*-
#! python3

"""
    Command-line to manage datbase operations.

    Author: Isogeo
"""

# #############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import configparser
import logging
from pathlib import Path

# 3rd party library
import click

# modules
from reporting import *


# #############################################################################
# ########## Globals ###############
# ##################################

# required subfolders
dir_logs = Path("_logs/").mkdir(exist_ok=True)
dir_reports = Path("_reports/").mkdir(exist_ok=True)

# CSV settings (see: https://pymotw.com/3/csv/)
csv.register_dialect("pipe",
                     delimiter="|",
                     escapechar="\\",
                     skipinitialspace=1
                     )


# #############################################################################
# ####### Command-line ############
# #################################

@click.command()
@click.option("--settings", default="settings.ini",
              help="Settings file.")
@click.option("--platform", default="prod",
              help="Database platform to read. Available values: 'prod' | 'qa'.")
@click.option("--platform", default="prod",
              help="Database platform to read. Available values: 'prod' | 'qa'.")
def cli_scanfme_reporting(settings, platform):
    """Command-line checking settings and executing required operations.

    :param str settings: path to a settings file containing credentials to read database
    :param str platform: deployed database to read (production or quality assurance)
    """
    # check settings file
    settings_file = Path(settings)
    if not settings_file.exists():
        raise IOError("settings file doesn't exist: {}".format(settings))
    settings_file = Path(settings).resolve()
    logger.info("Settings file used: {}".format(settings))

    # check platform value
    if platform not in ["prod", "qa"]:
        raise ValueError("Platform option must be one of: prod | qa")

    # load settings
    config = configparser.ConfigParser()
    config.read(settings_file)
    access = {"username": config.get(platform, "username"),
              "password": config.get(platform, "password"),
              "server": config.get(platform, "server"),
              "port": config.get(platform, "port"),
              "db_name": config.get(platform, "db_name"),
              "replicaSet": config.get(platform, "replicaSet"),
              }
    logger.info("Settings loaded. Database: {}".format(access.get("db_name")))

    # Start
    app = IsogeoScanUtils(access=access,
                          def_wg=config.get(platform, "wg"),
                          platform=platform,
                          wk_v=config.get(platform, "srv_version"))
    cli = app.connect()
    print(cli)

    # # collections overview
    # print(app.colls_stats())  # per workgroup
    # print(app.colls_stats(0))  # whole DB

    # # datasets overview
    # print(app.ds_diagnosis())  # per workgroup
    # print(app.ds_diagnosis(0))  # whole DB

    # # requests overview
    # print(app.rq_diagnosis())  # per workgroup
    # print(app.rq_diagnosis(0))  # whole DB

    # # srv info
    # print(app.wk_diagnosis())  # per workgroup
    # wks = app.wk_diagnosis(0)  # whole DB
    # print(wks.get("srvs_outdated")[1].keys())

    # app.csv_report("test.csv")
    # app.csv_report("test.csv", wg=0)

    # app.workers_report("test.csv")


# #############################################################################
# ##### Stand alone program ########
# ##################################

if __name__ == '__main__':
    """Standalone execution."""
    pass
