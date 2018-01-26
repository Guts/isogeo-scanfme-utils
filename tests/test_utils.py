# -*- coding: UTF-8 -*-
#! python3

# #############################################################################
# ########## Libraries #############
# ##################################

# Standard library
from os import environ
import unittest

# 3rd party
import pymongo

# package
from scanfme_db_utils import IsogeoScanUtils


# #############################################################################
# ######## Globals #################
# ##################################

access = {
          "username": environ.get("username"),
          "password": environ.get("password"),
          "server": environ.get("server"),
          "port": environ.get("port"),
          "db_name": environ.get("db_name"),
          "replicaSet": environ.get("replicaSet"),
          }

# #############################################################################
# ######## Classes #################
# ##################################


class DbStats(unittest.TestCase):
    """Test authentication process."""
    app = IsogeoScanUtils(access=access,
                          def_wg=environ.get("wg_test"),
                          platform="qa",
                          wk_v=environ.get("srv_version_ref"))
    cli = app.connect()

    # standard methods
    def setUp(self):
        """Executed before each test."""
        pass

    def tearDown(self):
        """Executed after each test."""
        pass

    # tests
    def test_attributes(self):
        """API secret must be 64 length."""
        self.assertTrue(hasattr(self.app, "def_wg"))
        self.assertTrue(hasattr(self.app, "platform"))
        self.assertTrue(hasattr(self.app, "user"))
        self.assertTrue(hasattr(self.app, "pswd"))
        self.assertTrue(hasattr(self.app, "serv"))
        self.assertTrue(hasattr(self.app, "port"))
        self.assertTrue(hasattr(self.app, "db_name"))
        self.assertTrue(hasattr(self.app, "rep_set"))
        self.assertTrue(hasattr(self.app, "wk_vers"))
        self.assertTrue(hasattr(self.app, "colls"))
        self.assertIsInstance(self.app.db, pymongo.database.Database)
        self.assertIsInstance(self.app.colls, pymongo.collection.Collection)


# #############################################################################
# ######## Standalone ##############
# ##################################

if __name__ == '__main__':
    unittest.main()
