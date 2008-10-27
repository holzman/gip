#!/usr/bin/env python

import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip_common import config
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler, GipValidate
import gip_testing

class TestGipInfo(unittest.TestCase):

    def make_site_tester(site):
        def test_site_entries(self):
            """
            Checks the %s information.
            """ % site
            # Switch commands over to the site ones:
            os.environ['GIP_TESTING'] = 'suffix=%s' % site
            path = os.path.expandvars("$GIP_LOCATION/bin/gip_info " \
                "--config=test_configs/%s.conf" % site)
            fd = os.popen(path)
            entries = read_ldap(fd)
            self.assertEquals(fd.close(), None)
            gv = GipValidate(entries)
            gv.run()
        return test_site_entries

    test_cigi_entries = make_site_tester("cigi")

def main():
    """
    The main entry point for when ce_print is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestGipInfo, stream, per_site=False)

if __name__ == '__main__':
    main()

