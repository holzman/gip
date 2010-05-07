#!/usr/bin/python

import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler
from gip_sets import Set

class TestHTPC(unittest.TestCase):

    def check_htpc_enabled(self, ce):
        """
        Check that the HTPC capability is set
        """
        found_htpc = False
        for entry in ce.glue.get("CECapability", []):
            if entry == "htpc":
                found_htpc=True
                break
        self.failUnless(found_htpc, msg="CECapability for HTPC missing.")

    def check_htpc_rsl(self, ce, rsl):
        """
        Check that the HTPC RSL is correct
        """
        idx = 0
        for entry in ce.nonglue.get("HTPCrsl", []):
            idx += 1
            self.failUnless(entry == rsl, msg="Incorrect RSL; got %s, " \
                "expecting %s." % (entry, rsl))
        self.failUnless(idx == 1, msg="Got %i HTPCrsl entries; expected 1." % \
            idx)

    def check_htpc_acbrs(self, ce, vos):
        """
        Check the HTPC ACBRs are set correctly
        """
        vos = Set(vos)
        self.failUnless(vos, msg="Empty input VOs (bad test case)")
        myvos = Set()
        for entry in ce.nonglue.get("HTPCAccessControlBaseRule"):
             myvos.add(entry)
        result = myvos.difference(vos)
        self.failIf(result, msg="Extra VOs detected: %s" % ", ".join(result))
        result = vos.difference(myvos)
        self.failIf(result, msg="Missing VO entries: %s" % ", ".join(result))

    def test_ucsd(self):
        """
        Verify the UCSD test case is correct.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py")
        fd = os.popen(path + " --config=test_configs/ucsd_condor_htpc.conf")
        entries = read_ldap(fd, multi=True)
        has_ce = False
        vos = ["VO:hcc", "VOMS:/glow/Role=HTPC"]
        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                has_ce = True
                self.check_htpc_enabled(entry)
                self.check_htpc_rsl(entry, "(foo=bar)")
                self.check_htpc_acbrs(entry, vos)
        self.failUnless(has_ce, msg="No CEs detected in UCSD output.")

    def test_ucsd2(self):
        """
        Verify the UCSD test case is correct.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py")
        fd = os.popen(path + " --config=test_configs/ucsd_condor_htpc2.conf")
        entries = read_ldap(fd, multi=True)
        has_ce = False
        vos = ["VO:hcc", "VOMS:/glow/Role=HTPC"]
        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                has_ce = True
                self.check_htpc_enabled(entry)
                self.check_htpc_rsl(entry, "(foo=bar)")
                self.check_htpc_acbrs(entry, vos)
        self.failUnless(has_ce, msg="No CEs detected in UCSD output.")

def main():
    """
    The main entry point for when test_htpc is run in standalone mode.
    """
    os.environ['GIP_TESTING'] = '1'
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestHTPC, stream, per_site=False)

if __name__ == '__main__':
    main()

