#!/usr/bin/env python

import unittest
import os
import sys
import tempfile
import urllib2
import re
import urlparse

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_bdii
from gip_common import config, addToPath, getLogger
from gip_testing import runBdiiTest

log = getLogger("GIP.Testing.SRM")

valid_versions = ['1.1.0', '2.2.0', '1.1', '2.2']
deprecated_versions = ['1.1.0', '2.2.0']
fk_re = re.compile("GlueSiteUniqueID=(.*)")

class TestSrmAds(unittest.TestCase):

    def __init__(self, site, cp):
        setattr(self, "testSrmAds_%s" % site, self.testSrmAds)
        unittest.TestCase.__init__(self, 'testSrmAds_%s' % site)
        self.site = site
        self.cp = cp
    
    def testSrmAds(self):
        """
        Test SRM ads for the following:
            * endpoint type is SRM
            * Version is 1.1 or 2.2 (1.1.0 or 2.2.0 generate warnings)
            * Site unique ID is not blank
            * Site unique ID is actual unique ID used for this site.
            * If dCache, make sure that the /srm/managervX string is correct.
        """
        entries = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local," \
            "o=grid" % self.site, query="(objectClass=GlueService)")
        for entry in entries:
            if entry.glue['ServiceType'].lower().find('srm') < 0:
                continue
            log.debug("Checking SRM entry:\n%s" % entry)
            self.assertEquals(entry.glue['ServiceType'], 'SRM',
                msg="ServiceType must be equal to 'SRM'")
            version = entry.glue['ServiceVersion']
            self.assertTrue(version in valid_versions,
                msg="ServiceVersion must be one of %s." % valid_versions)
            if version in deprecated_versions:
                log.warning("Version string %s is deprecated." % version)
            fk = entry.glue['ForeignKey']
            m = fk_re.match(fk)
            self.assertNotEquals(m, None, msg="Invalid GlueForeignKey.")
            site_unique_id = m.groups()[0]
            self.assertEquals(site_unique_id, self.getSiteUniqueID(),
                msg="Incorrect site unique ID for service.")
            path = self.getPath(entry.glue['ServiceEndpoint'])
            if path.startswith("/srm/managerv"):
                if version.startswith('2'):
                    self.assertEquals(path, "/srm/managerv2")
                elif version.startswith('1'):
                    self.assertEquals(path, '/srm/managerv1')

    def getSiteUniqueID(self):
        """
        Determine the unique ID for this site.
        """
        entries = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local," \
            "o=grid" % self.site, query="(objectClass=GlueSite)")
        self.assertEquals(len(entries), 1, msg="Multiple GlueSite entries for" \
            "site %s." % self.site)
        return entries[0].glue['SiteUniqueID']

    def getPath(self, surl):
        """
        Given a SRM SURL, determine the path of the SRM endpoint
        (i.e., for dCache, this is /srm/managervX)
        """
        surl = surl.replace("srm://", "https://").replace("httpg://",
            "https://")
        parts = urlparse.urlparse(surl)
        return parts.path

def main():
    """
    The main entry point for when srm_check is run in standalone mode.
    """
    cp = config()
    runBdiiTest(cp, TestSrmAds)

if __name__ == '__main__':
    main()
