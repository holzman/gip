#!/usr/bin/env python

import os
import sys
import cStringIO
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_testing import runTest, streamHandler
from gip_common import config
from gip_ldap import read_ldap, compareDN 

class TestGipLdap(unittest.TestCase):
    def setupTestStrings(self):
        originalStanza = """dn: Mds-Vo-name=resource,o=grid
objectClass: GlueTop
objectClass: Mds
"""
        alterStanza = """dn: GlueSiteUniqueID=USCMS-FNAL-WC1,mds-vo-name=local,o=grid
GlueSiteSecurityContact: mailto: computer-security@fnal.gov
"""
        return originalStanza, alterStanza 

    def test_compareDN(self):
        orig, alter = self.setupTestStrings()
        
        orig_entries = []
        fp = cStringIO.StringIO(orig)
        orig_entries += read_ldap(fp, multi=True)
        
        alter_entries = []
        fp = cStringIO.StringIO(alter)
        alter_entries += read_ldap(fp, multi=True)
        
        result = compareDN(orig_entries[0], alter_entries[0])
        print >> sys.stderr,"result: " + str(result) 
        self.failIf(result, msg="compareDN returned True when it should have returned False")
        return result

def main():
    os.environ['GIP_TESTING'] = '1'
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestGipLdap, stream, per_site=False)

if __name__ == '__main__':
    main()

