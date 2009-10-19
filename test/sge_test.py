#!/usr/bin/env python

import os
import sys
import unittest

os.environ['GIP_TESTING'] = '1'
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_sets import Set
from gip_common import config, cp_get
from sge_common import getVoQueues
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler
import gip_testing

class TestSGEDynamic(unittest.TestCase):

    def test_dynamic_provider(self):
        """
        Checks to make sure that the SGE dynamic provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/pf-sge.conf")

        fd = os.popen(path)
        fd.read()
        self.assertEquals(fd.close(), None)

    def test_contact_string(self):
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/pf-sge.conf")

        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)

        has_ce = False        
        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                contact_string = entry.glue['CEInfoContactString']
                self.failIf(contact_string == "", "Contact string is missing")
                self.failIf(contact_string.endswith("jobmanager-sge"), \
                    "Contact string must include the queue.")
                has_ce = True
        self.failUnless(has_ce, msg="No GLUE CE object emitted.")

def main():
    """
    The main entry point for when sge_test is run in standalone mode.
    """
    cp = config("test_configs/pf-sge.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSGEDynamic, stream, per_site=False)

if __name__ == '__main__':
    main()
