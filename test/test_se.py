#!/usr/bin/env python

import os
import re
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get, voList
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
        return entries, cp

    def check_se_1_ldif(self, entries):
        found_se = False
        for entry in entries:
            if not ('GlueSE' in entry.objectClass and \
                    entry.glue.get('SEUniqueID', [''])[0] == 'srm.unl.edu'):
                continue
            found_se = True
            self.failUnless(entry.glue['SEName'][0] == 'T2_Nebraska_Storage')
            self.failUnless(entry.glue['SEImplementationName'][0] == 'dcache')
            self.failUnless(entry.glue['SEImplementationVersion'][0] == \
                '1.8.0-15p6')
            self.failUnless(entry.glue['SEPort'][0] == '8443')
        self.failUnless(found_se, msg="GlueSE entry for srm.unl.edu missing.")

    def check_sa_1_ldif(self, entries, cp):
        found_se = False
        for entry in entries:
            if not ('GlueSA' in entry.objectClass and \
                    entry.glue.get('ChunkKey', [''])[0] == \
                    'GlueSEUniqueID=srm.unl.edu' and
                    entry.glue.get('SALocalID', [''])[0] == 'default'):
                continue
            found_se = True
            self.failUnless(entry.glue['SARoot'][0] == '/')
            self.failUnless(entry.glue['SAPath'][0] == '/pnfs/unl.edu/data4/')
            self.failUnless(entry.glue['SAType'][0] == 'permanent')
            self.failUnless(entry.glue['SARetentionPolicy'][0] == 'replica')
            self.failUnless(entry.glue['SAAccessLatency'][0] == 'online')
            self.failUnless(entry.glue['SAExpirationMode'][0] == 'neverExpire')
            self.failUnless(entry.glue['SACapability'][0] == 'file storage')
            self.failUnless(entry.glue['SAPolicyFileLifeTime'][0] == \
                'permanent')
            for vo in voList(cp):
                self.failUnless(vo in entry.glue['SAAccessControlBaseRule'])
                self.failUnless("VO: %s" % vo in \
                    entry.glue['SAAccessControlBaseRule'])
        self.failUnless(found_se, msg="GlueSA entry for srm.unl.edu missing.")

    def check_voview_1_ldif(self, entries, cp):
        name_re = re.compile('([A-Za-z]+):default')
        vos = voList(cp)
        found_vos = dict([(vo, False) for vo in vos])
        for entry in entries:
            if 'GlueVOInfo' not in entry.objectClass:
                continue
            if 'GlueSALocalID=default' not in entry.glue['ChunkKey'] or \
                    'GlueSEUniqueID=srm.unl.edu' not in entry.glue['ChunkKey']:
                continue
            m = name_re.match(entry.glue['VOInfoLocalID'][0])
            self.failUnless(m, msg="Unknown VOInfo entry!")
            vo = m.groups()[0]
            self.failIf(vo not in vos, msg="Unknown VO for view: %s." % vo)
            found_vos[vo] = True
            self.failUnless(entry.glue['VOInfoPath'][0] == \
               '/pnfs/unl.edu/data4/%s' % vo, msg="Incorrect path, %s, for" \
               " vo, %s" % (entry.glue['VOInfoPath'][0], vo))
            self.failUnless(entry.glue['VOInfoLocalID'] == \
                entry.glue['VOInfoName'])
            self.failUnless(vo in entry.glue['VOInfoAccessControlBaseRule'])
        for vo, status in found_vos.items():
            if not status:
                self.fail("VOView for %s missing." % vo)

    def check_srm_1_ldif(self, entries, cp):
        vos = voList(cp)
        found_srm = False
        for entry in entries:
            if 'GlueService' not in entry.objectClass:
                continue
            if 'httpg://srm.unl.edu:8443/srm/managerv2' not in \
                    entry.glue['ServiceName']:
                continue
            found_srm = True
            print entry
            for vo in vos:
                self.failIf(vo not in entry.glue['ServiceAccessControlRule'])
                self.failIf("VO:%s" % vo not in \
                    entry.glue['ServiceAccessControlRule'], msg="String `" \
                    "VO:%s` not in ACBR" % vo)
                self.failUnless("OK" in entry.glue["ServiceStatus"])
            self.failUnless(entry.glue['ServiceEndpoint'] == \
                entry.glue['ServiceName'])
            self.failUnless(entry.glue['ServiceAccessPointURL'] == \
                entry.glue['ServiceName'])
            self.failUnless(entry.glue['ServiceURI'] == \
                entry.glue['ServiceName'])
            self.failUnless("GlueSiteUniqueID=Nebraska" in \
                entry.glue['ForeignKey'])
            self.failUnless(entry.glue["ServiceType"][0] == 'SRM')
            self.failUnless(entry.glue["ServiceVersion"][0] == '2.2.0')
        self.failUnless(found_srm, msg="Could not find the SRM entity.")

    def check_se_2_ldif(self, entries):
        found_se = False
        for entry in entries:
            if not ('GlueSE' in entry.objectClass and \
                    entry.glue.get('SEUniqueID', [''])[0] == \
                    'dcache07.unl.edu'):
                continue
            found_se = True
            self.failUnless(entry.glue['SEName'][0] == 'T2_Nebraska_Storage')
            self.failUnless(entry.glue['SEImplementationName'][0] == 'dcache')
            self.failUnless(entry.glue['SEImplementationVersion'][0] == \
                '1.8.0-15p6')
            self.failUnless(entry.glue['SEPort'][0] == '8443')
        self.failUnless(found_se, msg="GlueSE entry for dcache07.unl.edu " \
            "missing.")

    def check_sa_2_ldif(self, entries, cp):
        found_se = False
        for entry in entries:
            if not ('GlueSA' in entry.objectClass and \
                    entry.glue.get('ChunkKey', [''])[0] == \
                    'GlueSEUniqueID=dcache07.unl.edu' and
                    entry.glue.get('SALocalID', [''])[0] == 'default'):
                continue
            found_se = True
            self.failUnless(entry.glue['SARoot'][0] == '/')
            self.failUnless(entry.glue['SAPath'][0] == '/pnfs/unl.edu/data4/')
            self.failUnless(entry.glue['SAType'][0] == 'permanent')
            self.failUnless(entry.glue['SARetentionPolicy'][0] == 'replica')
            self.failUnless(entry.glue['SAAccessLatency'][0] == 'online')
            self.failUnless(entry.glue['SAExpirationMode'][0] == 'neverExpire')
            self.failUnless(entry.glue['SACapability'][0] == 'file storage')
            self.failUnless(entry.glue['SAPolicyFileLifeTime'][0] == \
                'permanent')
            for vo in voList(cp):
                self.failUnless(vo in entry.glue['SAAccessControlBaseRule'])
                self.failUnless("VO: %s" % vo in \
                    entry.glue['SAAccessControlBaseRule'])
        self.failUnless(found_se, msg="GlueSA entry for dcache07.unl.edu " \
            "missing.")

    def check_voview_2_ldif(self, entries, cp):
        name_re = re.compile('([A-Za-z]+):default')
        vos = voList(cp)
        found_vos = dict([(vo, False) for vo in vos])
        for entry in entries:
            if 'GlueVOInfo' not in entry.objectClass:
                continue
            if 'GlueSALocalID=default' not in entry.glue['ChunkKey'] or \
                    'GlueSEUniqueID=dcache07.unl.edu' not in \
                    entry.glue['ChunkKey']:
                continue
            m = name_re.match(entry.glue['VOInfoLocalID'][0])
            self.failUnless(m, msg="Unknown VOInfo entry!")
            vo = m.groups()[0]
            self.failIf(vo not in vos, msg="Unknown VO for view: %s." % vo)
            found_vos[vo] = True
            self.failUnless(entry.glue['VOInfoPath'][0] == \
               '/user/%s' % vo, msg="Incorrect path, %s, for" \
               " vo, %s" % (entry.glue['VOInfoPath'][0], vo))
            self.failUnless(entry.glue['VOInfoLocalID'] == \
                entry.glue['VOInfoName'])
            self.failUnless(vo in entry.glue['VOInfoAccessControlBaseRule'])
        for vo, status in found_vos.items():
            if not status:
                self.fail("VOView for %s missing." % vo)

    def check_srm_2_ldif(self, entries, cp):
        vos = voList(cp)
        found_srm = False
        for entry in entries:
            if 'GlueService' not in entry.objectClass:
                continue
            if 'httpg://dcache07.unl.edu:8443/srm/v2/server' not in \
                    entry.glue['ServiceName']:
                continue
            found_srm = True
            print entry
            for vo in vos:
                self.failIf(vo not in entry.glue['ServiceAccessControlRule'])
                self.failIf("VO:%s" % vo not in \
                    entry.glue['ServiceAccessControlRule'], msg="String `" \
                    "VO:%s` not in ACBR" % vo)
                self.failUnless("OK" in entry.glue["ServiceStatus"])
            self.failUnless(entry.glue['ServiceEndpoint'] == \
                entry.glue['ServiceName'])
            self.failUnless(entry.glue['ServiceAccessPointURL'] == \
                entry.glue['ServiceName'])
            self.failUnless(entry.glue['ServiceURI'] == \
                entry.glue['ServiceName'])
            self.failUnless("GlueSiteUniqueID=Nebraska" in \
                entry.glue['ForeignKey'])
            self.failUnless(entry.glue["ServiceType"][0] == 'SRM')
            self.failUnless(entry.glue["ServiceVersion"][0] == '2.2.0')
        self.failUnless(found_srm, msg="Could not find the SRM entity.")

    def check_se_1(self, entries, cp):
        self.check_se_1_ldif(entries)
        self.check_sa_1_ldif(entries, cp)
        self.check_voview_1_ldif(entries, cp)
        #self.check_aps_1_ldif(entries)
        self.check_srm_1_ldif(entries, cp)

    def check_se_2(self, entries, cp):
        pass
        self.check_se_2_ldif(entries)
        self.check_sa_2_ldif(entries, cp)
        #self.check_aps_2_ldif(entries)
        self.check_srm_2_ldif(entries)

    def test_old_se_config(self):
        entries, cp = self.run_test_config("red-se-test.conf")
        self.check_se_1(entries, cp)

    def test_new_se_config(self):
        entries, cp = self.run_test_config("red-se-test2.conf")
        self.check_se_1(entries, cp)
        self.check_se_2(entries, cp)

def main():
    cp = config("test_configs/red-se-test.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSEConfigs, stream, per_site=False)

if __name__ == '__main__':
    main()

