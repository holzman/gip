
import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get
from gip_cese_bind import getCEList, getSEList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap

class TestDowntime(unittest.TestCase):

    def resetCache(self):
        cp = config("test_configs/red.conf")
        temp_dir = os.path.expandvars(cp_get(cp, "gip", "temp_dir", \
            "$GIP_LOCATION/var/tmp"))
        save = os.path.join(temp_dir, "known_downtime.pickle")
        if os.path.exists(save):
            os.unlink(save)
        
    def setUpLDAP(self, filename=None):
        if filename != None:
            self.filename = filename
        os.environ['GIP_TESTING'] = "1"
        cp = config(self.filename)
        self.ces = getCEList(cp)
        self.ses = getSEList(cp)
        cese_provider_path = os.path.expandvars("$GIP_LOCATION/plugins/" \
                               "downtime.py --config %s" % self.filename)
        print >> sys.stderr, "Used command", cese_provider_path
        fd = os.popen(cese_provider_path)
        self.entries = read_ldap(fd, multi=True)
        self.exit_status = fd.close()

    def testDowntime(self):
        self.filename = 'test_configs/downtime_success.conf'
        self.setUpLDAP()
        self.verifyOutput()

    def verifyOutput(self):
        for ce in self.ces:
            has_downtime = False
            for entry in self.entries:
                dn = "GlueCEUniqueID=%s" % ce
                if entry.dn[0] != dn:
                    continue
                self.failUnless(entry.glue['CEStateStatus'][0] == 'Closed',
                    msg="CE %s not marked as down." % ce)
                has_downtime = True
                break
            if not has_downtime:
                self.fail(msg="CE %s has no entry in output" % ce)
        ses = ['Nebraska_classicSE']
        for se in ses:
            has_downtime = False
            for entry in self.entries:
                dn = "GlueSEUniqueID=%s" % se
                if entry.dn[0] != dn:
                    continue
                self.failUnless(entry.glue['SEStatus'][0] == 'Closed',
                    msg="SE %s not marked as down." % se)
                has_downtime = True
                break
            if not has_downtime:
                self.fail(msg="SE %s has no entry in output" % se)

    def testBlocked(self):
        self.resetCache()
        self.filename = 'test_configs/downtime_success.conf'
        self.setUpLDAP()
        self.filename = 'test_configs/downtime_blocked.conf'
        self.setUpLDAP()
        self.verifyOutput()

    def testDown(self):
        self.resetCache()
        self.filename = 'test_configs/downtime_success.conf'
        self.setUpLDAP()
        self.filename = 'test_configs/downtime_down_url.conf'
        self.setUpLDAP()
        self.verifyOutput()

    def testWrongURL(self):
        self.resetCache()
        self.filename = 'test_configs/downtime_success.conf'
        self.setUpLDAP()
        self.filename = 'test_configs/downtime_wrong_url.conf'
        self.setUpLDAP()
        self.verifyOutput()

    def testEmpty(self):
        self.resetCache()
        self.filename = 'test_configs/downtime_down_url.conf'
        self.setUpLDAP()
        self.failIf(self.entries, msg="Output even though there should be no" \
            " downtime information available.")

def main():
    cp = config("test_configs/red.conf")
    stream = streamHandler(cp)
    runTest(cp, TestDowntime, stream, per_site=False)

if __name__ == '__main__':
    main()

