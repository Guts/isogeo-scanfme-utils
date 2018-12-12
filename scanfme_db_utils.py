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
import csv
import logging
from logging.handlers import RotatingFileHandler
from os import path
import pprint
from urllib.parse import quote_plus

# 3rd party library
from gevent import monkey
monkey.patch_all()
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# ##############################################################################
# ########## Globals ###############
# ##################################

# settings
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
d_colls = {'datasets': "where metadata about scanned datasets are stored | ABBRV: DS",
           'entrypoints': "list of clients entrypoints | ABBRV: DS",
           'geodatabases': "flat databases | ABBRV: GD",
           'procdatasets': "history of all datasets which have been scanned | ABBRV: PD",
           'requests': "list of requests sent to the super worker | ABBRV: RQ",
           'sessions': "active sessions | ABBRV: SS",
           'subscriptions': "Isogeo Worker clients registered | ABBRV: SB",
           }

# CSV settings (see: https://pymotw.com/3/csv/)
csv.register_dialect("pipe",
                     delimiter="|",
                     escapechar="\\",
                     skipinitialspace=1
                     )


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
        rqs = self.colls.get("requests")
        if wg == 1:
            # finished requests
            rq_finish = rqs.find({"groupId": self.def_wg,
                                  "state": "finished"}).count()
            if rq_finish:
                rq_finish_last = rqs.find({"groupId": self.def_wg,
                                           "state": "finished"}).limit(1)[0]\
                                                                .get("_id"),
            else:
                rq_finish_last = None
                pass
            # broken requests
            rq_broken = rqs.find({"groupId": self.def_wg,
                                  "state": "broken"}).count()
            if rq_broken:
                rq_broken_last = rqs.find({"groupId": self.def_wg,
                                           "state": "broken"}).limit(1)[0]\
                                                              .get("err"),
            else:
                rq_broken_last = None
                pass
            # killed requests
            rq_killed = rqs.find({"groupId": self.def_wg,
                                  "state": "killed"}).count()
            if rq_killed:
                rq_killed_last = rqs.find({"groupId": self.def_wg,
                                           "state": "killed"}).limit(1)[0]\
                                                              .get("err"),
            else:
                rq_killed_last = None
                pass
            # storing
            rq_report = {"rq_finish": rq_finish,
                         "rq_finish_last": rq_finish_last,
                         "rq_broken": rq_broken,
                         "rq_broken_last": rq_broken_last,
                         "rq_killed": rq_killed,
                         "rq_killed_last": rq_killed_last,
                         }
        elif wg == 0:
            # finished requests
            rq_finish = rqs.find({"state": "finished"}).count()
            if rq_finish:
                rq_finish_last = rqs.find({"state": "finished"}).limit(1)[0]\
                                                                .get("_id"),
            else:
                rq_finish_last = None
                pass
            # broken requests
            rq_broken = rqs.find({"state": "broken"}).count()
            if rq_broken:
                rq_broken_last = rqs.find({"state": "broken"}).limit(1)[0]\
                                                              .get("err"),
            else:
                rq_broken_last = None
                pass
            # killed requests
            rq_killed = rqs.find({"state": "killed"}).count()
            if rq_killed:
                rq_killed_last = rqs.find({"state": "killed"}).limit(1)[0]\
                                                              .get("err"),
            else:
                rq_killed_last = None
                pass
            # storing
            rq_report = {"rq_finish": rq_finish,
                         "rq_finish_last": rq_finish_last,
                         "rq_broken": rq_broken,
                         "rq_broken_last": rq_broken_last,
                         "rq_killed": rq_killed,
                         "rq_killed_last": rq_killed_last,
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
            wk_report = {"srvs_uptodate": wks.find({"groupId": self.def_wg,
                                                    "workers.version": self.wk_vers}),
                         "srvs_outdated": wks.find({"groupId": self.def_wg,
                                                    "workers": {"$exists": 1},
                                                    "workers.version": {"$ne": self.wk_vers}}),
                         "srvs_no_created": wks.find({"groupId": self.def_wg,
                                                      "workers": {"$exists": 0}
                                                      }),
                         }
        elif wg == 0:
            wk_report = {"srvs_uptodate": wks.find({"workers.version": self.wk_vers}).sort("groupId", ASCENDING),
                         "srvs_outdated": wks.find({"workers": {"$exists": 1},
                                                    "workers.version": {"$ne": self.wk_vers}}).sort("groupId", ASCENDING),
                         "srvs_no_created": wks.find({"workers": {"$exists": 0}}).sort("groupId", ASCENDING),
                         "srvs_no_install": wks.find({"workers": {"$size": 0}}).sort("groupId", ASCENDING),
                         }
        else:
            raise ValueError("A boolean value is required.")

        # method end
        return wk_report

    # -- CSV REPORT ----------------------------------------------------------

    def csv_report(self, csv_name: str, wg: bool=1, folder: str="./reports"):
        """
            Inform about installed services in a workgroup.

            :param str csv_name: CSV filename (extension required)
            :param bool wg: filter on the default workgroup
            :param str foler: parent folder where to write the CSV file
        """
        if wg == 1:
            # retrieve data
            stats_colls = self.colls_stats()
            stats_ds = self.ds_diagnosis()
            stats_rq = self.rq_diagnosis()
            stats_wk = self.wk_diagnosis()
            # prepare csv output file
            csv_out = path.normpath(path.join(folder,
                                              "ScanFME_Report_{}_{}_{}"
                                              .format(self.platform.upper(),
                                                      self.def_wg,
                                                      csv_name))
                                    )
            with open(csv_out, 'w', newline='') as csvfile:
                fieldnames = ("wg_id", "wg_url", "wg_ds_count",
                              "wg_ep_count", "wg_wk_count", "wg_rq_count",
                              "wg_gd_count", "wg_pd_count")
                writer = csv.DictWriter(csvfile,
                                        dialect="pipe",
                                        fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow({"wg_id": self.def_wg,
                                 "wg_url": "https://daemons.isogeo.com/g/{}"
                                           .format(self.def_wg),
                                 "wg_ds_count": stats_colls.get("datasets"),
                                 "wg_wk_count": stats_colls.get("subscriptions"),
                                 "wg_rq_count": stats_colls.get("requests"),
                                 "wg_ep_count": stats_colls.get("entrypoints"),
                                 "wg_gd_count": stats_colls.get("geodatabases"),
                                 "wg_pd_count": stats_colls.get("procdatasets"),
                                 }
                                )
        elif wg == 0:
            # retrieve data
            stats_colls = self.colls_stats(0)
            stats_ds = self.ds_diagnosis(0)
            stats_rq = self.rq_diagnosis(0)
            stats_wk = self.wk_diagnosis(0)
            # prepare csv output file
            csv_out = path.normpath(path.join(folder, "ScanFME_Report_{}_DB_{}"
                                                      .format(self.platform,
                                                              csv_name))
                                    )
            with open(csv_out, 'w', newline='') as csvfile:
                fieldnames = ("wg_id", "wg_url", "wg_ds_count", "")
                writer = csv.DictWriter(csvfile,
                                        dialect="pipe",
                                        fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow({"wg_id": "hoho",
                                 "wg_url": "https://daemons.isogeo.com/g/{}"
                                           .format(self.def_wg),
                                 "wg_url": "hihi",
                                 "wg_ds_count": "héhé",
                                 }
                                )
        else:
            raise ValueError("A boolean value is required.")

        # end method
        return csvfile

    def workers_report(self, csv_name: str, folder: str="./reports"):
        """
            Inform about installed services.

            :param str csv_name: CSV filename (extension required)
            :param str foler: parent folder where to write the CSV file
        """
        # retrieve data
        wks = self.wk_diagnosis(0)
        # prepare csv output file
        csv_out = path.normpath(path.join(folder,
                                          "ScanFME_Report_Workers_{}_{}"
                                          .format(self.platform,
                                                  csv_name))
                                )
        with open(csv_out, 'w', newline='') as csvfile:
            fieldnames = ("wg_id", "wg_url", "wk_id", "wk_count", "wk_uptodate", "wk_name", "wk_version")
            writer = csv.DictWriter(csvfile,
                                    dialect="pipe",
                                    fieldnames=fieldnames)
            writer.writeheader()
            try:
                for wk in wks.get("srvs_uptodate"):
                    writer.writerow({"wg_id": wk.get("groupId"),
                                     "wg_url": "https://app.isogeo.com/groups/{}/admin/isogeo-worker"
                                               .format(wk.get("groupId")),
                                     "wk_id": wk.get("_id"),
                                     "wk_count": len(wk.get("workers")),
                                     "wk_uptodate": 1,
                                     "wk_name": wk.get("workers")[0].get("givenName"),
                                     "wk_version": wk.get("workers")[0].get("version"),
                                     }
                                    )
                for wk in wks.get("srvs_outdated"):
                    if len(wk.get("workers")) == 0:
                        wk["workers"] = [{"givenName": "",
                                          "version": ""}, ]
                    else:
                        pass
                    writer.writerow({"wg_id": wk.get("groupId"),
                                     "wg_url": "https://app.isogeo.com/groups/{}/admin/isogeo-worker"
                                               .format(wk.get("groupId")),
                                     "wk_id": wk.get("_id"),
                                     "wk_count": len(wk.get("workers", "")),
                                     "wk_uptodate": 0,
                                     "wk_name": wk.get("workers")[0].get("givenName"),
                                     "wk_version": wk.get("workers")[0].get("version"),
                                     }
                                    )
                for wk in wks.get("srvs_no_created"):
                    writer.writerow({"wg_id": wk.get("groupId"),
                                     "wg_url": "https://app.isogeo.com/groups/{}/admin/isogeo-worker"
                                               .format(wk.get("groupId")),
                                     "wk_id": wk.get("_id"),
                                     "wk_count": 0,
                                     "wk_uptodate": 0,
                                     }
                                    )
            except Exception as e:
                logger.error(e)
                logger.error("https://mlab.com/clusters/rs-ds053053/databases/scanfme-prod-cluster/collections/subscriptions?_id={}".format(wk.get("_id")), wk.get("workers"))

        # end method
        return csvfile


# ##############################################################################
# ##### Stand alone program ########
# ##################################

if __name__ == '__main__':
    """Standalone execution."""
    import configparser
    settings_file = r"settings.ini"
    # check ini file
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
                          platform=platform,
                          wk_v=config.get(platform, "srv_version"))
    cli = app.connect()

    # collections overview
    print(app.colls_stats())  # per workgroup
    print(app.colls_stats(0))  # whole DB

    # datasets overview
    print(app.ds_diagnosis())  # per workgroup
    print(app.ds_diagnosis(0))  # whole DB

    # requests overview
    print(app.rq_diagnosis())  # per workgroup
    print(app.rq_diagnosis(0))  # whole DB

    # srv info
    print(app.wk_diagnosis())  # per workgroup
    wks = app.wk_diagnosis(0)  # whole DB
    print(wks.get("srvs_outdated")[1].keys())

    app.csv_report("test.csv")
    app.csv_report("test.csv", wg=0)

    app.workers_report("test.csv")
