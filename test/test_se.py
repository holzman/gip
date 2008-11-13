#!/usr/bin/env python

import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap

class TestSEConfigs(unittest.TestCase):

    def run_test_config(self, testconfig):
        os.environ['GIP_TESTING'] = "1"
        cfg_name = os.path.join("test_configs", testconfig)
        if not os.path.exists(cfg_name):
           self.fail(msg="Test configuration %s does not exist." % cfg_name)
        cp = config(cfg_name)
        providers_path = cp_get(cp, "gip", "provider_dir", "$GIP_LOCATION/" \
            "providers")
        providers_path = os.path.expandvars(providers_path)
        se_provider_path = os.path.join(providers_path,
            'storage_element.py')
        cmd = se_provider_path + " --config %s" % cfg_name
        print cmd
        fd = os.popen(cmd)
        entries = read_ldap(fd, multi=True)
        self.failIf(fd.close(), msg="Run of storage element provider failed!")
        return entries

    def check_se_1_ldif(self, entries):
        found_se = False
        for entry in entries:
            if not ('GlueSE' in entry.objectClass and \
                    entry.glue.get('SEUniqueID', [''])[0] == 'srm.unl.edu'):
                continue
            found_se = True
            self.failUnless(entry.glue['SEName'][0] == 'T2_Nebraska_Storage')
            self.failUnless(entry.glue['SEImplementationName'][0] == 'dcache')
            self.failUnless(entry.glue['SEPort'][0] == '8443')
        self.failUnless(found_se, msg="GlueSE entry for srm.unl.edu missing.")

    def check_se_1(self, entries):
        self.check_se_1_ldif(entries)
        #self.check_sas_1_ldif(entries)
        #self.check_aps_1_ldif(entries)
        #self.check_srm_1_ldif(entries)

    def check_se_2(self, entries):
        pass
        #self.check_se_2_ldif(entries)
        #self.check_sas_2_ldif(entries)
        #self.check_aps_2_ldif(entries)
        #self.check_srm_2_ldif(entries)

    def test_old_se_config(self):
        entries = self.run_test_config("red-se-test.conf")
        self.check_se_1(entries)

    def test_new_se_config(self):
        entries = self.run_test_config("red-se-test2.conf")
        self.check_se_1(entries)
        self.check_se_2(entries)

def main():
    cp = config("test_configs/red-se-test.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSEConfigs, stream, per_site=False)

if __name__ == '__main__':
    main()

