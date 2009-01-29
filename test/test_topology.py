
import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean, voList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap

class TestSiteTopology(unittest.TestCase):

    def setUp(self):
        filename = 'test/test_configs/red.conf'
        if not os.path.exists(filename):
            filename = 'test_configs/red.conf'
        self.filename = filename
        self.cp = config(filename)

    def setUpLDAP(self, multi=False):
        command = "$GIP_LOCATION/providers/site_topology.py --config=%s" % \
            self.filename
        stdout = os.popen(command)
        entries = read_ldap(stdout, multi=multi)
        return entries

    def test_exitcode(self):
        command = "$GIP_LOCATION/providers/site_topology.py --config=%s" % \
            self.filename
        stdout = os.popen(command)
        stdout.read()
        self.failUnless(stdout.close() == None, msg="Site topology provider " \
            "failed with nonzero exit code.")

    def test_site(self):
        has_site = False
        for entry in self.setUpLDAP():
            if 'GlueSite' in entry.objectClass:
                self.failUnless(entry.glue['SiteUniqueID'] == 'Nebraska')
                self.failUnless(entry.glue['SiteName'] == 'Nebraska')
                self.failUnless(entry.glue['SiteDescription'] == 'OSG Site')
                self.failUnless(entry.glue['SiteLatitude'] == '40.82')
                has_site = True
        self.failUnless(has_site, msg="No site LDAP entry present!")

    def test_cluster(self):
        has_cluster = False
        for entry in self.setUpLDAP(multi=True):
            if 'GlueCluster' in entry.objectClass:
                self.failUnless(entry.glue['ClusterName'][0] == 'red.unl.edu')
                self.failUnless(entry.glue['ClusterTmpDir'][0] == \
                    '/opt/osg/data')
                self.failUnless('GlueSiteUniqueID=Nebraska' in \
                    entry.glue['ForeignKey'])
                self.failUnless('GlueCEUniqueID=red.unl.edu:2119/jobmanager-' \
                    'pbs-cmsprod' in entry.glue['ForeignKey'])
                has_cluster = True
        self.failUnless(has_cluster, msg="No cluster LDAP entry present!")

    def test_subcluster(self):
        has_subcluster = False
        for entry in self.setUpLDAP():
            if 'GlueSubcluster' in entry.objectClass:
                has_subcluster = True
        self.failUnless(has_subcluster, msg="No subcluster LDAP entry present!")


def main():
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestSiteTopology, stream, per_site=False)

if __name__ == '__main__':
    main()

