#!/usr/bin/env python

import os
import sys
import socket
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_sets import Set
from gip_common import config, cp_get
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
        ce_name = socket.gethostname()
        for entry in entries:
            print entry
            if 'GlueCE' in entry.objectClass:
                has_ce = True
                self.assertEquals(entry.glue['CEStateTotalJobs'], '6')
                self.assertEquals(entry.glue['CEStateRunningJobs'], '4')
                self.assertEquals(entry.glue['CEStateFreeCPUs'], '77')
                self.assertEquals(entry.glue['CEPolicyAssignedJobSlots'], '81')
                self.assertEquals(entry.glue['CEUniqueID'], \
                    '%s:2119/jobmanager-condor-default' % ce_name)
        self.assertEquals(has_ce, True)

    def check_for_ce(self, cename, entries):
        """
        Make sure there is a CE with unique id cename in the entries list.

        Returns the entry corresponding to this CE.
        Raises an exception if no such CE exists.
        """
        for entry in entries:
            if 'GlueCE' not in entry.objectClass:
                continue
            if cename not in entry.glue['CEUniqueID']:
                continue
            return entry
        self.failIf(True, msg="Expected a CE named %s in the output." % cename)

    def test_groups_output(self):
        """
        Look at the group output from UCSD.
           - Check to make sure a GlueCE is defined for each group.
           - Check to make sure the black/white lists are obeyed.
           - Check to make sure that the FreeSlots are right for GlueCE.
               (FreeSlots should not be more than group quota).
           - Check to make sure that the FreeSlots are right for GlueVOView.
               (FreeSlots should not be more than group quota).
        """
        os.environ['GIP_TESTING'] = 'suffix=ucsd'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg_info_provider_" \
            "condor.py --config=test_configs/ucsd_condor.conf")
        fd = os.popen(path)
        entries = read_ldap(fd, multi=True)
        tmpl = "osg-gw-2.t2.ucsd.edu:2119/jobmanager-condor-%s"
        # This CE should *not* be present -> verifies blacklists work.
        try:
            entry = self.check_for_ce(tmpl % "group_cdf", entries)
            did_fail = False
        except:
            did_fail = True
        self.failUnless(did_fail, msg="CE %s is present, but shouldn't be" % \
            (tmpl % "group_cdf"))

        # Check lcgadmin group.  Verifies whitelist is being used.
        entry = self.check_for_ce(tmpl % "group_lcgadmin", entries)
        self.failUnless('VO:atlas' in entry.glue['CEAccessControlBaseRule'],
            msg="ATLAS is not allowed in group_lcgadmin")
        self.failUnless('VO:cms' in entry.glue['CEAccessControlBaseRule'],
            msg="CMS is not allowed in group_lcgadmin")

        # Check cmsprod group.  Verifies that the mapper is being used.
        entry = self.check_for_ce(tmpl % "group_cmsprod", entries)
        self.failUnless('VO:cms' in entry.glue['CEAccessControlBaseRule'],
            msg="CMS is not allowed in group_cmsprod")
        
        # Check ligo group.  Verifies that whitelist overrides the mapper.
        entry = self.check_for_ce(tmpl % "group_ligo", entries)
        self.failUnless('VO:fmri' in entry.glue['CEAccessControlBaseRule'],
            msg="FMRI is not allowed in group_cmsprod")
        self.failIf('VO:ligo' in entry.glue['CEAccessControlBaseRule'],
            msg="LIGO is allowed in group_cmsprod")

    def test_collector_host(self):
        """
        Make sure that we can parse non-trivial COLLECTOR_HOST entries.
        """

def main():
    """
    The main entry point for when condor_test is run in standalone mode.
    """
    os.environ['GIP_TESTING'] = '1'
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestCondorProvider, stream, per_site=False)

if __name__ == '__main__':
    main()

