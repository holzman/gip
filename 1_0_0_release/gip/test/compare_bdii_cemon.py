#!/usr/bin/env python

import unittest
import os
import sys
import urllib2
import cStringIO

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_ldap, read_bdii, getSiteList, compareDN, prettyDN
from gip_common import config
from gip_testing import runTest, streamHandler

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
        self.failUnless(data.find("Error Message") < 0, \
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

def main():
    """
    The main entry point for when compare_bdii_cemon is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestCompareData, stream)

if __name__ == '__main__':
    main()
