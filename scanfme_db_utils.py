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
import csv
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
logfile = RotatingFileHandler("LOG_ScanFME_Utils.log", "a", 5000000, 1)
logfile.setLevel(log_lvl)
logfile.setFormatter(log_form)
logger.addHandler(logfile)

# collections
d_colls = {'datasets': "where metadata about scanned datasets are stored",
           'entrypoints': "list of clients entrypoints",
           'geodatabases': "flat databases",
           'procdatasets': "history of all datasets which have been scanned",
           'requests': "list of requests sent to the super worker",
           'sessions': "active sessions",
           'subscriptions': "Isogeo Worker clients registered",
           }


# #############################################################################
# ########## Classes ###############
# ##################################

class IsogeoScanUtils(object):
    """Make easy to get some metrics about Scan FME usage."""

    def __init__(self, access: dict, def_wg: str=None,
                 platform="qa", wk_v: str="2.1.0"):
        """
            Instanciate class, check parameters and add object attributes.

            :param dict access: keys required = username, password,
                                server, port, db_name, replicaSet
            :param str def_wg: default workgroup UUID to use
            :param str platform: cluster to use qa or prod
            :param str wk_v: service Isogeo worker reference version
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

        if not set(("username", "password", "server",
                    "port", "db_name", "replicaSet")) <= set(access):
            raise KeyError("Required keys are not present in access dict.")
        else:
            pass

        # add attributes
        self.def_wg = def_wg
        self.platform = platform
        self.user = access.get("username")
        self.pswd = access.get("password")
        self.serv = access.get("server")
        self.port = access.get("port")
        self.db_name = access.get("db_name")
        self.rep_set = access.get("replicaSet")
        self.wk_vers = wk_v

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
        counter = {coll: self.db
                             .get_collection(coll)
                             .find({"groupId": self.def_wg})
                   for coll in d_colls}
        return counter

    # -- METRICS -----------------------------------------------------------

    def colls_stats(self, wg: bool=1) -> dict:
        """
            Perform basic calculation about database.

            :param bool wg: option to filter on the default workgroup
        """
        if wg == 1:
            counter = {coll: self.colls.get(coll)
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

    def ds_diagnosis(self, wg: bool=1) -> dict:
        """
            Some diagnosis on datasets collection:
                - count of scanned datasets without isogeo_id matching.

            :param bool wg: option to filter on the default workgroup
        """
        datasets = self.colls.get("datasets")
        if wg == 1:
            ds_report = {"no_isogeo_id": datasets.find({"groupId": self.def_wg,
                                                        "isogeo_id": {"$exists": False}}).count(),
                         }
        elif wg == 0:
            ds_report = {"no_isogeo_id": datasets.find({"isogeo_id": {"$exists": False}}).count(),
                         }
        else:
            raise ValueError("A boolean value is required.")

        # method end
        return ds_report

    def rq_diagnosis(self, wg: bool=1):
        """
            Inform about requests.

            :param bool wg: filter on the default workgroup
        """
        rq = self.colls.get("requests")
        if wg == 1:
            # wg_srv = self.colls.get("subscriptions")\
            #                    .find({"groupId": self.def_wg})
            rq_report = {"rq_finish": rq.find({"groupId": self.def_wg,
                                               "state": "finished"}).count(),
                         "rq_finish_last": rq.find({"groupId": self.def_wg,
                                                    "state": "finished"}).limit(1)[0]
                                                                         .get("err"),
                         "rq_broken": rq.find({"groupId": self.def_wg,
                                               "state": "broken"}).count(),
                         "rq_broken_last": rq.find({"groupId": self.def_wg,
                                                    "state": "broken"}).limit(1)[0]
                                                                       .get("err"),
                         "rq_killed": rq.find({"groupId": self.def_wg,
                                               "state": "killed"}).count(),
                         "rq_killed_last": rq.find({"groupId": self.def_wg,
                                                    "state": "killed"}).limit(1)[0]
                                                                       .get("err"),
                         }
        elif wg == 0:
            rq_report = {"rq_finish": rq.find({"state": "finished"}).count(),
                         "rq_finish_last": rq.find({"state": "finished"}).limit(1)[0]
                                                                         .get("err"),
                         "rq_broken": rq.find({"state": "broken"}).count(),
                         "rq_broken_last": rq.find({"state": "broken"}).limit(1)[0]
                                                                       .get("err"),
                         "rq_killed": rq.find({"state": "killed"}).count(),
                         "rq_killed_last": rq.find({"state": "killed"}).limit(1)[0]
                                                                       .get("err"),
                         }
        else:
            raise ValueError("A boolean value is required.")

        # method end
        return rq_report

    def wk_diagnosis(self, wg: bool=1):
        """
            Inform about installed services in a workgroup.

            :param bool wg: filter on the default workgroup
        """
        wks = self.colls.get("subscriptions")
        if wg == 1:
            # wg_srv = self.colls.get("subscriptions")\
            #                    .find({"groupId": self.def_wg})
            wk_report = {"srvs_uptodate": wks.find({"groupId": self.def_wg,
                                                    "workers.version": self.wk_vers}),
                         "srvs_outdated": wks.find({"groupId": self.def_wg,
                                                    "workers.version": {"$ne": self.wk_vers}}),
                         }
        elif wg == 0:
            wk_report = {"srvs_uptodate": wks.find({"workers.version": self.wk_vers}),
                         "srvs_outdated": wks.find({"workers.version": {"$ne": self.wk_vers}}),
                         }
        else:
            raise ValueError("A boolean value is required.")

        # method end
        return wk_report

        """
            Inform about installed services in a workgroup.

            :param bool wg: option to filter on the default workgroup.
        """
        if wg == 1:
            wg_srv = self.colls.get("subscriptions")\
                               .find({"groupId": self.def_wg})
            return wg_srv
        elif wg == 0:
            subscriptions = self.colls.get("subscriptions")
            db_srvs = {"srvs_uptodate": subscriptions.find({"workers.version": "2.1.0"}),
                       "srvs_outdated": subscriptions.find({"workers.version": {"$ne": "2.1.0"}}),
                       }
            return db_srvs
        else:
            raise ValueError("A boolean value is required.")


# ##############################################################################
# ##### Stand alone program ########
# ##################################

if __name__ == '__main__':
    """Standalone execution."""
    if not path.isfile(path.realpath(settings_file)):
        raise IOError("settings.ini file required")
    else:
        pass
    # target
    platform = "prod"

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

    # Start
    app = IsogeoScanUtils(access=access,
                          def_wg=config.get(platform, "wg"),
                          platform=platform)
    cli = app.connect()
    # print(type(cli), type(app.db))
    # print(app.colls)
    # print(type(app.colls.get("subscriptions")))

    # # utils
    # print(app.ds_is_duplicated("tache_urbaine"))

    # # Stats
    # print(app.stats())  # per workgroup
    # print(app.stats(0))  # whole DB

    # srv info
    srv_info_wg = app.registered_services()
    print(# dir(srv_info_wg),
          # type(srv_info_wg[0]),
          srv_info_wg[0].keys(),
          srv_info_wg[0].get("__v"),
          srv_info_wg[0].get("workers")[0].get("version")
          )

    srv_info_db = app.registered_services(0)

    print("{} services are up to date"
          .format(srv_info_db.get("srvs_uptodate").count()))
    print("{} services are outdated"
          .format(srv_info_db.get("srvs_outdated").count()))
