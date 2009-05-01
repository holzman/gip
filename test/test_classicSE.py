
import os
import sys
import unittest
import cStringIO
import ConfigParser

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean, voList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap
from gip.providers.generic_storage import print_classicSE

se_statuses = ['Production', 'Queueing', 'Closed', 'Draining']
service_statuses = ['OK', 'Warning', 'Critical', 'Unknown', 'Other']

class TestClassicSE(unittest.TestCase):

    def setUp(self):
        cp = ConfigParser.ConfigParser()
        self.cp = cp
        cp.add_section("site")
        cp.set("site", "name", "Nebraska")
        cp.set("site", "unique_name", "red.unl.edu")
        cp.add_section("bdii")
        cp.set("bdii", "endpoint", "ldap://is.grid.iu.edu:2170/")
        cp.add_section("classic_se")
        cp.set("classic_se", "advertise_se", "True")
        cp.set("classic_se", "host", "red.unl.edu")
        cp.set("classic_se", "port", "2811")
        cp.set("classic_se", "name", "T2_Nebraska_classicSE")
        cp.set("classic_se", "unique_name", "red.unl.edu_se")
        cp.set("classic_se", "default", "/opt/osg/data/$VO")
        cp.set("classic_se", "space", "%i, %i, %i" % (1000**2, 10*1000**2,
            11*1000**2))
        self.output = cStringIO.StringIO()
        old_stdout = sys.stdout
        sys.stdout = self.output
        try:
            print_classicSE(cp)
        finally:
            sys.stdout = old_stdout
        self.output.seek(0)
        self.entries = read_ldap(self.output, multi=True)
        ses = []
        sas = []
        for entry in self.entries:
            if 'GlueSE' in entry.objectClass:
                ses.append(entry)
            if 'GlueSA' in entry.objectClass:
                sas.append(entry)
        self.ses = ses
        self.sas = sas

    def test_space(self):
        used_gb = 1
        free_gb = 10
        total_gb = 11
        used = 1*1000**2
        free = 10*1000**2
        total = 11*1000**2
        self.failUnless(self.ses)
        self.failUnless(self.sas)
        for se in self.ses:
            self.failUnless(se.glue['SESizeTotal'][0] == str(total_gb))
            self.failUnless(se.glue['SESizeFree'][0] == str(free_gb))
        for sa in self.sas:
            self.failUnless(sa.glue['SATotalOnlineSize'][0] == str(total_gb))
            self.failUnless(sa.glue['SAUsedOnlineSize'][0] == str(used_gb))
            self.failUnless(sa.glue['SAFreeOnlineSize'][0] == str(free_gb))
            self.failUnless(sa.glue['SAStateAvailableSpace'][0] == str(free))
            self.failUnless(sa.glue['SAStateUsedSpace'][0] == str(used))

    def test_valid_se_status(self):
        for se in self.ses:
            self.failUnless(se.glue['SEStatus'][0] in se_statuses)

    def test_one_sa(self):
        vos = voList(self.cp)
        len_vos = len(vos)
        has_special_sa = False
        for sa in self.sas:
            if len(sa.glue['SAAccessControlBaseRule']) == len_vos:
                has_special_sa = True
                for vo in vos:
                    has_vo_entry = False
                    for entry in sa.glue['SAAccessControlBaseRule']:
                        if entry.find(vo) >= 0:
                            has_vo_entry = True
                            break
                    self.failUnless(has_vo_entry, "VO %s isn't in ACBR." % vo)
        self.failUnless(has_special_sa, "Can't find group SA for classicSE")

    def test_many_sa(self):
        vos = voList(self.cp)
        for sa in self.sas:
            print sa
            rules = sa.glue['SAAccessControlBaseRule']
            for rule in rules:
                if rule.startswith("VO:"):
                    rule = rule[3:]
                if rule in vos:
                    vos.remove(rule)
                else:
                    self.fail("Unknown VO supported: %s." % rule)
        self.failIf(vos, msg="VOs with no classicSE support: %s" % \
            ', '.join(vos))

    def test_sa_values(self):
        type_enum = ['permanent', 'durable', 'volatile', 'other']
        expiration_enum = ['neverExpire','warnWhenExpired','releaseWhenExpired']
        policy_enum = ['Permanent', 'Durable', 'Volatile']
        access_enum = ['online', 'nearline', 'offline']
        retention_enum = ['custodial', 'output', 'replica']
        for sa in self.sas:
            self.failUnless(sa.glue['SAType'][0] in type_enum)
            self.failUnless(sa.glue['SARetentionPolicy'][0] in retention_enum)
            self.failUnless(sa.glue['SAAccessLatency'][0] in access_enum)
            self.failUnless(sa.glue['SAExpirationMode'][0] in expiration_enum)
            #self.failUnless(sa.glue['SAPolicyFileLifeTime'] in policy_enum)

def main():
    os.environ['GIP_TESTING'] = '1'
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestClassicSE, stream, per_site=False)

if __name__ == '__main__':
    main()

