#!/usr/bin/env python

import unittest
import os
import sys
import tempfile
import urllib2
import re
import urlparse

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_bdii, prettyDN
from gip_common import config, addToPath, getLogger
from gip_testing import runTest, streamHandler

log = getLogger("GIP.Testing.DN")

class TestDnAds(unittest.TestCase):

    def __init__(self, site, cp):
        setattr(self, "testDnAds_%s" % site, self.testDnAds)
        unittest.TestCase.__init__(self, 'testDnAds_%s' % site)
        self.site = site
        self.cp = cp

    def testDnAds(self):
        """
        Test DN ads for the following:
            * o=grid appears once
            * mds-vo-name=local appears once
            * mds-vo-name=<site> appears once
            * they appear in the correct order
        """
        entries = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local," \
            "o=grid" % self.site)
        for entry in entries:
            dn = list(entry.dn)
            self.assertEquals(dn.pop(), "o=grid", msg="DN %s does not end with"\
                " o=grid" % prettyDN(entry.dn))
            self.assertEquals(dn.pop(), "mds-vo-name=local", msg="DN %s does " \
                "not end with mds-vo-name=local,o=grid" % prettyDN(entry.dn))
            self.assertEquals(dn.pop(), "mds-vo-name=%s" % self.site,
                msg="DN %s does not end with mds-vo-name=%s,mds-vo-name=local,"\
                "o=grid" % (prettyDN(entry.dn), self.site))
            for d in dn:
                self.failUnless(d.find("o=grid") < 0, msg="There is an extra " \
                    "o=grid entry in DN %s" % prettyDN(entry.dn))
                self.failUnless(d.startswith("mds-vo-name") == False,
                    "There is an extra mds-vo-name entry in DN %s" % \
                    prettyDN(entry.dn))

def main():
    """
    The main entry point for when dn_check is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestDnAds, stream)

if __name__ == '__main__':
    main()

