# -*- coding: UTF-8 -*-
#! python3

# #############################################################################
# ########## Libraries #############
# ##################################

# Standard library
from os import environ
import unittest

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
                          platform="qa")
    cli = app.connect()

    # standard methods
    def setUp(self):
        """Executed before each test."""
        pass

    def tearDown(self):
        """Executed after each test."""
        pass

    # tests
    def test_stats(self):
        """API secret must be 64 length."""

        self.assertIsInstance(self.app.stats(), dict)


# #############################################################################
# ######## Standalone ##############
# ##################################

if __name__ == '__main__':
    unittest.main()
