#!/usr/bin/env python

import os
import sys
import cStringIO
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_testing import runTest, streamHandler
from gip_common import config
from gip_ldap import read_ldap, compareDN, prettyDN 

class TestGipLdap(unittest.TestCase):
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        self.originalStanza1 = """dn: Mds-Vo-name=resource,o=grid
objectClass: GlueTop
objectClass: Mds
"""
        self.originalStanza2 = """dn: Mds-Vo-name=resource2,o=grid
objectClass: GlueTop
objectClass: Mds
"""
        self.originalStanza3 = """dn: Mds-Vo-name=resource,o=grid2
objectClass: GlueTop
objectClass: Mds
"""
        self.alterStanza1 = """dn: GlueSiteUniqueID=USCMS-FNAL-WC1,mds-vo-name=local,o=grid
GlueSiteSecurityContact: mailto: computer-security@fnal.gov
"""
        self.alterStanza2 = """dn: GlueSiteUniqueID=USCMS-FNAL-XEN,mds-vo-name=local,o=grid
GlueSiteSecurityContact: mailto: computer-security@fnal.gov
"""

    def run_compareDN(self, string1, string2, should_match = False):
        orig_entries = []
        fp = cStringIO.StringIO(string1)
        orig_entries += read_ldap(fp, multi=True)
        
        other_entries = []
        fp = cStringIO.StringIO(string2)
        other_entries += read_ldap(fp, multi=True)
        
        result = compareDN(orig_entries[0], other_entries[0])
        if should_match:
            self.failIf(not result, msg="compareDN returned False when it should have "\
                    "returned True.\n\nDN1: %s \nDN2: %s" % \
                    (prettyDN(orig_entries[0].dn),prettyDN(other_entries[0].dn)))
        else:
            self.failIf(result, msg="compareDN returned True when it should have "\
                    "returned False.\n\nDN1: %s \nDN2: %s" % \
                    (prettyDN(orig_entries[0].dn),prettyDN(other_entries[0].dn)))
            
        return result

    def test_compare_DN(self):
        # test 2 dns both starting with mds-vo-name and are identical
        self.run_compareDN(self.originalStanza1, self.originalStanza1, should_match = True)
        # test 2 dns both starting with mds-vo-name but mds-vo-name is different
        self.run_compareDN(self.originalStanza1, self.originalStanza2)
        # test 2 dns both starting with mds-vo-name but o is different
        self.run_compareDN(self.originalStanza1, self.originalStanza3)
        # test 2 dns one starting with mds-vo-name but one is not
        self.run_compareDN(self.originalStanza1, self.alterStanza1)
        
        # test 2 dns neither starting with mds-vo-name but are identical
        self.run_compareDN(self.alterStanza1, self.alterStanza1, should_match = True)
        # test 2 dns neither starting with mds-vo-name but are different
        self.run_compareDN(self.alterStanza1, self.alterStanza2)

def main():
    os.environ['GIP_TESTING'] = '1'
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestGipLdap, stream, per_site=False)

if __name__ == '__main__':
    main()

