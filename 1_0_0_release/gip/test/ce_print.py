#!/usr/bin/env python

import os
import sys
import unittest

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_bdii
from gip_common import config, getLogger
from gip_testing import runTest, streamHandler

log = getLogger("GIP.Print.CE")

class TestPrintCe(unittest.TestCase):

    def __init__(self, site, cp):
        setattr(self, "testCeAds_%s" % site, self.testCeAds)
        unittest.TestCase.__init__(self, 'testCeAds_%s' % site)
        self.site = site
        self.cp = cp

    def testCeAds(self):
        """
        Print out the following information for each CE at a site:

          - LRMS and version
          - Free batch slots
          - Running jobs and waiting jobs
          - Total batch slots
          - Max wall clock time

        For each attached VO view, print:
          - VO
          - Running jobs
          - Waiting jobs
        """
        entries = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local," \
            "o=grid" % self.site)
        ce_entries = []
        vo_entries = []
        ce_vo = {}
        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                ce_entries.append(entry)
                ce_vo[entry] = []
            if 'GlueVOView' in entry.objectClass and 'GlueCETop' in \
                    entry.objectClass:
                vo_entries.append(entry)
        for entry in vo_entries:
            for entry2 in ce_entries:
                desired_ck = 'GlueCEUniqueID=%s' % entry2.glue['CEUniqueID']
                if entry.glue['ChunkKey'] == desired_ck:
                    ce_vo[entry2].append(entry)
        print "\nSITE: %s\n" % self.site
        for entry in ce_entries:
            out = '\t* CE: %s\n' % entry.glue['CEUniqueID']
            out += '\t\tLRMS type: %s, Version: %s\n' % \
                (entry.glue['CEInfoLRMSType'], entry.glue['CEInfoLRMSVersion'])
            out += '\t\tSlots used %s, Free %s\n' % \
                (entry.glue['CEStateFreeJobSlots'],
                 entry.glue['CEStateRunningJobs'])
            out += '\t\tTotal batch slots: %s\n' % \
                entry.glue['CEPolicyAssignedJobSlots']
            out += '\t\tMax wall time: %s\n' % \
                entry.glue['CEPolicyMaxWallClockTime']
            for vo in ce_vo[entry]:
                out += '\t\t- VO: %s\n' % vo.glue['VOViewLocalID']
                out += '\t\t\tRunning %s, Waiting %s\n' % \
                    (vo.glue['CEStateRunningJobs'],
                     vo.glue['CEStateWaitingJobs'])
            print out[:-1]

def main():
    """
    The main entry point for when ce_print is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestPrintCe, stream)

if __name__ == '__main__':
    main()

