#!/usr/bin/env python

import os
import sys
import unittest

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_bdii
from gip_common import config, getLogger
from gip_testing import runTest, streamHandler

log = getLogger("GIP.Print.Site")

class TestPrintSite(unittest.TestCase):

    def __init__(self, site, cp):
        setattr(self, "testSiteAds_%s" % site, self.testSiteAds)
        unittest.TestCase.__init__(self, 'testSiteAds_%s' % site)
        self.site = site
        self.cp = cp

    def testSiteAds(self):
        """
        Print out the following information for each site:

          - CE names
          - Close SE
          - VOView for each CE
        """
        entries = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local," \
            "o=grid" % self.site)
        ce_entries = []
        cese_entries = []
        se_entries = []
        vo_entries = []
        ce_se = {}
        ce_vo = {}
        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                ce_entries.append(entry)
                ce_se[entry] = []
                ce_vo[entry] = []
            if 'GlueVOView' in entry.objectClass and 'GlueCETop' in \
                    entry.objectClass:
                vo_entries.append(entry)
            if 'GlueSE' in entry.objectClass:
                se_entries.append(entry)
            if 'GlueCESEBind' in entry.objectClass:
                cese_entries.append(entry)
        for entry in cese_entries:
            for entry2 in se_entries:
                if entry.glue['CESEBindSEUniqueID'] == \
                        entry2.glue['SEUniqueID']:
                    for entry3 in ce_entries:
                        if entry.glue['CESEBindCEUniqueID'] == \
                                entry3.glue['CEUniqueID']:
                            ce_se[entry3].append(entry2)
        for entry in vo_entries:
            for entry2 in ce_entries:
                desired_ck = 'GlueCEUniqueID=%s' % entry2.glue['CEUniqueID']
                if entry.glue['ChunkKey'] == desired_ck:
                    ce_vo[entry2].append(entry)
        print "\nSITE: %s\n" % self.site
        for entry in ce_entries:
            out = '\t* CE: %s' % entry.glue['CEUniqueID']
            if len(ce_se) > 0:
                for se in ce_se[entry]:
                    out += ', Close SE: %s' % se.glue['SEUniqueID']
            out += '\n'
            for vo in ce_vo[entry]:
                out += '\t\t- VO: %s\n' % vo.glue['VOViewLocalID']
            print out[:-1]

def main():
    """
    The main entry point for when site_print is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestPrintSite, stream)

if __name__ == '__main__':
    main()

