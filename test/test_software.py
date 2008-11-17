#!/usr/bin/env python

import os
import re
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get, voList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap

class TestSoftware(unittest.TestCase):

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
            'software.py')
        cmd = se_provider_path + " --config %s" % cfg_name
        print cmd
        fd = os.popen(cmd)
        entries = read_ldap(fd, multi=True)
        self.failIf(fd.close(), msg="Run of software provider failed!")
        return entries, cp

    def check_software(self, entries, name, version, path):
        found_software = False
        for entry in entries:
            if 'GlueLocation' not in entry.objectClass:
                continue
            if name not in entry.glue['LocationName']:
                continue
            if version not in entry.glue['LocationVersion']:
                continue
            if path not in entry.glue['LocationPath']:
                continue
            found_software = True
            break
        self.failUnless(found_software, msg="Software package %s, version %s, path %s" \
            " not advertised in GLUE." % (name, version, path))

    def check_software_cms_helper(self, entries, version):
        self.check_software(entries, 'VO-cms-%s' % version, version, '/opt/osg/app/' \
            'cmssoft/cms')

    def check_software_gpn(self, entries):
        self.check_software_cms_helper(entries, 'CMSSW_0_9_2')
        self.check_software_cms_helper(entries, 'CMSSW_0_8_4')
        self.check_software_cms_helper(entries, 'CMSSW_1_1_0')
        self.check_software_cms_helper(entries, 'CMSSW_1_2_1')
        self.check_software_cms_helper(entries, 'CMSSW_1_2_2')
        self.check_software_cms_helper(entries, 'CMSSW_1_2_3')

    def check_software_atlas(self, entries):
        self.check_software_cms_helper(entries, 'CMKIN_6_1_0')
        self.check_software(entries, "LCG-2_7_0", '2.7.0', '/opt/osg/app/dteam')

    def check_software_cms(self, entries):
        self.check_software(entries, "SAMPLE_LOCATION", "default", "/SAMPLE-path")
        self.check_software(entries, "SAMPLE_SCRATCH", "devel", "/SAMPLE-path")
        self.check_software(entries, "CMS_PATH", "ORCA_8_7_5", "/opt/osg/app/cmssoft/cms")
        self.check_software(entries, "CMS_PATH", "ORCA_8_9_3", "/opt/osg/app/cmssoft/cms")

    def test_new_software(self):
        entries, cp = self.run_test_config("red-software-test.conf")
        self.check_software_gpn(entries)
        self.check_software_atlas(entries)
        self.check_software_cms(entries)

def main():
    cp = config("test_configs/red-software-test.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSoftware, stream, per_site=False)

if __name__ == '__main__':
    main()

