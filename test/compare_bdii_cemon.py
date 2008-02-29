#!/usr/bin/env python

import unittest
import os
import sys
import urllib2
import cStringIO

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_ldap, read_bdii, getSiteList, compareDN
from gip_common import config

def prettyDN(dn_list):
    dn = ''
    for entry in dn_list:
        dn += entry + ','
    return dn[:-1]

def safe_site_name(site):
    return site.replace("-", "_")

class TestCompareData(unittest.TestCase):

    def __init__(self, site, cp):
        safe_site = safe_site_name(site)
        setattr(self, "testCompareData_%s" % safe_site, self.testCompareData)
        unittest.TestCase.__init__(self, 'testCompareData_%s' % safe_site)
        self.site = site
        self.cp = cp

    def testCompareData(self):
        """
        Compare the data served to CEMon versus that from BDII for %s.
        """ % self.site
        url = self.cp.get("test", "goc") % self.site
        #print url
        data = urllib2.urlopen(url).read()
        self.assertTrue(data.find("Error Message") < 0, \
            msg = "Site %s not serving with CEMon." % self.site)
        fp1 = cStringIO.StringIO(data)
        entries1 = read_ldap(fp1, multi=True)
        entries2 = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local," \
                        "o=grid" % self.site, multi=True)
        bad_entries = list(set(entries1).symmetric_difference(set(entries2)))
        filtered_entries = []
        for entry in bad_entries:
            if not entry.objectClass == ('GlueTop', ):
                filtered_entries.append(entry)
        bad_entries = filtered_entries
        msg = 'The following entries %i (out of %i) are inconsistent between ' \
              'BDII and CEMon:\n' % (len(bad_entries), len(entries1))
        dns = []
        for entry in bad_entries:
            dn = prettyDN(entry.dn) + ' (in CEMon %s; in BDII %s)' % \
                (entry in entries2, entry in entries1)
            dn += '\n%s' % entry
            dns.append(dn)
        dns.sort()
        for entry in dns:
            msg += entry + '\n'
        self.assertEquals(len(bad_entries), 0, msg=msg)

def generateTests(cls, args=[]):
    cp = config()
    sites = getSiteList(cp)
    tests = []
    for site in sites:
        if len(args) > 0 and site not in args:
            continue
        if site == 'local' or site == 'grid':
            continue
        case = TestCompareData(site, cp)
        tests.append(case)
    return unittest.TestSuite(tests)

if __name__ == '__main__':
    testSuite = generateTests(TestCompareData, sys.argv[1:])
    testRunner = unittest.TextTestRunner(verbosity=2)
    result = testRunner.run(testSuite)
    sys.exit(not result.wasSuccessful())

