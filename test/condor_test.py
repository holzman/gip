#!/usr/bin/env python

import os
import sys
import unittest
from gip_sets import Set

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from pbs_common import getVoQueues
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler
import gip_testing

class TestCondorProvider(unittest.TestCase):

    def test_condor_provider(self):
        """
        Checks to make sure that the Condor provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg_info_provider_" \
            "condor.py")
        fd = os.popen(path + " --config=test_configs/condor_test.conf")
        fd.read()
        self.assertEquals(fd.close(), None)

    def test_output_pf(self):
        """
        Test the sample output from prairiefire.unl.edu.  Should represent
        a "simple" Condor setup, no groups or priorities.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg_info_provider_" \
            "condor.py --config=test_configs/condor_test.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.assertEquals(fd.close(), None)
        has_ce = False
        for entry in entries:
            print entry
            if 'GlueCE' in entry.objectClass:
                has_ce = True
                self.assertEquals(entry.glue['CEStateTotalJobs'], '6')
                self.assertEquals(entry.glue['CEStateRunningJobs'], '4')
                self.assertEquals(entry.glue['CEStateFreeCPUs'], '77')
                self.assertEquals(entry.glue['CEPolicyAssignedJobSlots'], '81')
                self.assertEquals(entry.glue['CEUniqueID'], \
                    'prairiefire.unl.edu:2119/jobmanager-condor-default')
        self.assertEquals(has_ce, True)

    def test_collector_host(self):
        """
        Make sure that we can parse non-trivial COLLECTOR_HOST entries.
        """

def main():
    """
    The main entry point for when condor_test is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestCondorProvider, stream, per_site=False)

if __name__ == '__main__':
    main()

