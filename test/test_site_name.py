import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get, configContents
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap, prettyDN

class TestSiteName(unittest.TestCase):
    def setUpLDAP(self, filename, multi=False):
        if filename != None:
            filename = filename
        command = "$GIP_LOCATION/providers/site_topology.py --config=%s" % \
            filename

        stdout = os.popen(command)
        entries = read_ldap(stdout, multi=multi)
        return entries

    def siteTest(self, entries):
        has_site = False
        for entry in entries:
            if 'GlueSite' in entry.objectClass:
                self.failUnless(entry.glue['SiteUniqueID'] != 'UNKNOWN')
                self.failUnless(entry.glue['SiteName'] != 'UNKNOWN')
                has_site = True
        self.failUnless(has_site, msg="No site LDAP entry present!")
    
    def subclusterTest(self, entries):
        has_subcluster = False
        for entry in entries:
            if 'GlueSubCluster' in entry.objectClass:
                try:
                    msg = "Subclusters site name is UNKNOWN"
                    subcluster_name = entry.glue['SubClusterName']
                    sc_sitename = subcluster_name.split("-")[0]
                    self.failUnless(sc_sitename != "UNKNOWN", msg)
                except:
                    msg = "Subcluster stanza is malformed.  dn: %s"
                    self.failUnless(False, msg % prettyDN(entry.dn))
                has_subcluster = True
        self.failUnless(has_subcluster, msg="No subcluster LDAP entry present!")

    def cpTest(self, filename):
        cp = config(filename)

        siteName = cp_get(cp, "site", "name", "UNKNOWN")
        self.failUnless(siteName != "UNKNOWN", msg="Site name is UNKNOWN")
        siteUniqueName = cp_get(cp, "site", "unique_name", "UNKNOWN")
        self.failUnless(siteName != "UNKNOWN", msg="Site unique name is UNKNOWN")

        for section in cp.sections():
            my_sect = section.lower()
            if not my_sect.startswith('subcluster'):
                continue
            msg = "Subcluster name is UNKNOWN"
            subcluster_name = cp_get(cp, my_sect, 'unique_name', "UNKNOWN")
            self.failUnless(subcluster_name != "UNKNOWN", msg)

            msg = "Subclusters site name is UNKNOWN"
            try:
                sc_sitename = subcluster_name.split("-")[0]
                self.failUnless(sc_sitename != "UNKNOWN", msg)
            except:
                self.failUnless(False, msg)
        
    def test_cp(self):
        self.cpTest("test_configs/site-name.conf")
        self.cpTest("test_configs/red.conf")
        
    def test_site(self):
        entries = self.setUpLDAP("test_configs/site-name.conf")
        self.siteTest(entries)
        entries = self.setUpLDAP("test_configs/red.conf")
        self.siteTest(entries)

    def test_subcluster(self):
        entries = self.setUpLDAP("test_configs/site-name.conf")
        self.subclusterTest(entries)
        entries = self.setUpLDAP("test_configs/red.conf")
        self.subclusterTest(entries)
    
def main():
    os.environ['GIP_TESTING'] = '1'
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestSiteName, stream, per_site=False)

if __name__ == '__main__':
    main()
