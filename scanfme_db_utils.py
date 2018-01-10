# -*- coding: UTF-8 -*-
#! python3

"""
    Connect to isogeo Scan FME database to perform some tidious and
     repetitive tasks.
"""

# #############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import configparser
import logging
from logging.handlers import RotatingFileHandler
from os import path
import pprint
from urllib.parse import quote_plus

# 3rd party library
from gevent import monkey
monkey.patch_all()
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# ##############################################################################
# ########## Globals ###############
# ##################################

# settings
settings_file = r"settings.ini"
log_lvl = logging.DEBUG

# LOG FILE ##
logger = logging.getLogger("isogeo_scanfme_utils")
logging.captureWarnings(True)
logger.setLevel(log_lvl)
log_form = logging.Formatter("%(asctime)s || %(levelname)s "
                             "|| %(module)s || %(lineno)s || %(message)s")
logfile = RotatingFileHandler("LOG.log", "a", 5000000, 1)
logfile.setLevel(log_lvl)
logfile.setFormatter(log_form)
logger.addHandler(logfile)

# collections
d_colls = {'datasets': "",
           'entrypoints': "",
           'geodatabases': "",
           'procdatasets': "",
           'requests': "",
           'sessions': "",
           'subscriptions': "",
           }


# #############################################################################
# ########## Classes ###############
# ##################################

class IsogeoScanUtils(object):
    """Make easy to get some metrics about Scan FME usage."""

    def __init__(self, settings, def_wg : str=None, platform="qa", ):
        """
            Instanciate class, check parameters and add object attributes.

            :param str def_wg: default workgroup UUID to use
            :param str platform: cluster to use qa or prod
        """
        # check parameters
        if platform.lower() not in ("qa", "prod"):
            raise ValueError("Platform parameter must be 'qa' or 'prod'.")
        else:
            pass

        if def_wg and len(def_wg) != 32:
            raise TypeError("Invalid workgroup UUID.")
        else:
            pass

        # add attributes
        self.def_wg = def_wg
        self.platform = platform
        self.user = settings.get(platform, "username")
        self.pswd = settings.get(platform, "password")
        self.serv = settings.get(platform, "server")
        self.port = settings.get(platform, "port")
        self.db_name = settings.get(platform, "db_name")
        self.rep_set = settings.get(platform, "replicaSet")

    # -- CONNECTION -----------------------------------------------------------

    def uri(self) -> str:
        """Construct URI and returns it."""
        if self.platform == "qa":
            uri = "mongodb://{}:{}@{}:{}/{}"\
                  .format(self.user,
                          self.pswd,
                          self.serv,
                          self.port,
                          self.db_name)
            logger.debug("QA URI built: " + uri)
        elif self.platform == "prod":
            srv0, srv1 = self.serv.split("|")
            uri = "mongodb://{}:{}@{}:{},{}:{}/{}?replicaSet={}"\
                  .format(self.user,
                          self.pswd,
                          srv0,
                          self.port,
                          srv1,
                          self.port,
                          self.db_name,
                          self.rep_set)
            logger.debug("PROD URI built: " + uri)
        # method end
        return uri

    def connect(self) -> "pymongo client":
        """Check connection and returns client object."""
        self.client = MongoClient(self.uri())
        self.db = self.client.get_default_database()
        self.conn_state = self.check_connection()

        if self.conn_state:
            self.collections_init()
        else:
            logger.debug("Collections can't be filled because of "
                         "lost server connection.")

        return self.client

    def check_connection(self) -> bool:
        """A quick check of server state."""
        try:
            self.client.admin.command('ismaster')
            return 1
        except (ConnectionFailure, ServerSelectionTimeoutError):
            return 0

    def collections_init(self):
        """Fillfull collectrions dict with collections objects."""
        self.colls = {coll: self.db.get_collection(coll)
                      for coll in d_colls}
        pass

    # -- SEARCH -----------------------------------------------------------

    def ds_is_duplicated(self, ds_name: str) -> bool:
        """
            Says if a dataset is duplicated in the database.

            :param str ds_name: dataset name to look for
        """
        ct = self.colls.get("datasets")\
                       .find({"groupId": self.def_wg,
                              "featureType": ds_name
                              })\
                       .count()
        return ct

    def get_ds_workgroup(self, workgroup_id: str):
        """Lists datasets which have been scanned by a specific workgroup."""
        pass

    # -- METRICS -----------------------------------------------------------

    def stats(self, wg:bool=1) -> dict:
        """
            Perform basic calculation about database.

            :param bool wg: option to filter on the default workgroup

        """
        # , self.db.command("dbstats")
        if wg == 1:
            counter = {coll: self.db.get_collection(coll)
                                    .find({"groupId": self.def_wg})
                                    .count()
                       for coll in d_colls}
        elif wg == 0:
            counter = {coll: self.db.get_collection(coll)
                                    .count()
                       for coll in d_colls}
        else:
            raise ValueError("A boolean value is required.")

        # method end
        return counter

# ##############################################################################
# ##### Stand alone program ########
# ##################################

if __name__ == '__main__':
    """Standalone execution."""
    if not path.isfile(path.realpath(settings_file)):
        raise IOError("settings.ini file required")
    else:
        pass

    # load settings
    config = configparser.ConfigParser()
    config.read(settings_file)

    # Start
