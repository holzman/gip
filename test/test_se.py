#!/usr/bin/env python

import os
import re
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get, voList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap
import gip.bestman.srm_ping as srm_ping

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

    def check_srmcp_1_ldif(self, entries):
        found_cp = False
        for entry in entries:
            if 'GlueSEControlProtocol' not in entry.objectClass:
                continue
            if entry.glue['SEControlProtocolLocalID'][0] != 'srm.unl.edu_srmv2':
                continue
            found_cp = True
            self.failUnless('httpg://srm.unl.edu:8443/srm/managerv2' in \
                entry.glue['SEControlProtocolEndpoint'])
            self.failUnless('2.2.0' in entry.glue['SEControlProtocolVersion'])
            self.failUnless('SRM' in entry.glue['SEControlProtocolType'])
            self.failUnless('GlueSEUniqueID=srm.unl.edu' in \
                entry.glue['ChunkKey'])
        self.failUnless(found_cp, msg="Could not find the SRM Control Prot.")

    def check_srmcp_2_ldif(self, entries):
        found_cp = False
        for entry in entries:
            if 'GlueSEControlProtocol' not in entry.objectClass:
                continue
            if entry.glue['SEControlProtocolLocalID'][0] != \
                    'dcache07.unl.edu_srmv2':
                continue
            found_cp = True
            self.failUnless('httpg://dcache07.unl.edu:8443/srm/v2/server' in \
                entry.glue['SEControlProtocolEndpoint'])
            self.failUnless('2.2.0' in entry.glue['SEControlProtocolVersion'])
            self.failUnless('SRM' in entry.glue['SEControlProtocolType'])
            self.failUnless('GlueSEUniqueID=dcache07.unl.edu' in \
                entry.glue['ChunkKey'])
        self.failUnless(found_cp, msg="Could not find the SRM Control Prot.")

    def check_se_2_ldif(self, entries):
        found_se = False
        for entry in entries:
            if not ('GlueSE' in entry.objectClass and \
                    entry.glue.get('SEUniqueID', [''])[0] == \
                    'dcache07.unl.edu'):
                continue
            found_se = True
            self.failUnless(entry.glue['SEName'][0] == 'T2_Nebraska_Hadoop')
            self.failUnless(entry.glue['SEImplementationName'][0] == 'bestman')
            self.failUnless(entry.glue['SEImplementationVersion'][0] == \
                '2.2.1.2.e1')
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
            self.failUnless(entry.glue['SAPath'][0] == '/user/')
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
        self.check_srmcp_1_ldif(entries)

    def check_se_2(self, entries, cp):
        self.check_se_2_ldif(entries)
        self.check_sa_2_ldif(entries, cp)
        #self.check_aps_2_ldif(entries)
        self.check_srm_2_ldif(entries, cp)
        self.check_srmcp_2_ldif(entries)

    def test_old_se_config(self):
        entries, cp = self.run_test_config("red-se-test.conf")
        self.check_se_1(entries, cp)
        self.check_srmcp_1_ldif(entries)

    def test_new_se_config(self):
        entries, cp = self.run_test_config("red-se-test2.conf")
        self.check_se_1(entries, cp)
        self.check_se_2(entries, cp)

    def test_bestman_space(self):
        """
        Make sure that the correct space-calc is used for BestMan in the case
        where there are no static tokens.

        Written to test bug from ticket #38
        """
        entries, cp = self.run_test_config("bestman_space.conf")
        # Check space for SE
        found_se = False
        for entry in entries:
            if 'GlueSE' not in entry.objectClass:
                continue
            if 'cit-se2.ultralight.org' not in entry.glue['SEUniqueID']:
                continue
            self.failUnless(int(entry.glue['SESizeTotal'][0]) == 3)
            self.failUnless(int(entry.glue['SESizeFree'][0]) == 2)
            found_se = True
        self.failUnless(found_se, msg="Could not find the correct target SE.")
        # Check space for SA
        found_sa = False
        for entry in entries:
            if 'GlueSA' not in entry.objectClass:
                continue
            if 'GlueSEUniqueID=cit-se2.ultralight.org' not in \
                    entry.glue['ChunkKey']:
                continue
            if 'default' not in entry.glue['SALocalID']:
                continue
            self.failUnless(int(entry.glue['SATotalOnlineSize'][0]) == 3)
            self.failUnless(int(entry.glue['SAFreeOnlineSize'][0]) == 2)
            found_sa = True
        self.failUnless(found_se, msg="Could not find the correct target SA.")

    def checkReservedRules1(self, entries):
        """
        Make sure that on a SA with multiple supported VOs that there is
        0GB of reserved space, but non-zero GB of total space.
        """
        found_sa = False
        for entry in entries:
            if 'GlueSA' not in entry.objectClass:
                continue
            if 'osg-group:replica:nearline' not in entry.glue['SALocalID']:
                continue
            found_sa = True
            self.failUnless(int(entry.glue['SATotalOnlineSize'][0]) == 26023)
            self.failUnless(int(entry.glue['SAReservedOnlineSize'][0]) == 0)
        self.failUnless(found_sa, msg="Could not find the correct target SA.")

    def checkReservedRules2(self, entries):
        """
        Make sure that on a SA with one supported VO that reserved space is
        equal to the total space.
        """
        found_sa = False
        for entry in entries:
            if 'GlueSA' not in entry.objectClass:
                continue
            if 'atlas-tape-write-group:replica:nearline' not in \
                    entry.glue['SALocalID']:
                continue
            found_sa = True
            self.failUnless(int(entry.glue['SATotalOnlineSize'][0]) == \
                int(entry.glue['SAReservedOnlineSize'][0]))
        self.failUnless(found_sa, msg="Could not find the correct target SA.")

    def checkReducedAmount(self, entries):
        """
        Example 3 is set up so one of the SAs will have less space than dCache
        actually advertises due to a limit on the amount of space a SE can
        advertise.
        """
        found_sa = False
        for entry in entries:
            if 'GlueSA' not in entry.objectClass:
                continue
            if 'atlas-tape-write-group:replica:nearline' not in \
                    entry.glue['SALocalID']:
                continue
            found_sa = True
            self.failUnless(int(entry.glue['SATotalOnlineSize'][0]) == 3000)
        self.failUnless(found_sa, msg="Could not find the correct target SA.")

    def checkPaths(self, entries):
        """
        For given LG and reservations, make sure the paths are correct.
        """
        lkgrp = [('atlasuser-disk-link-group:replica:nearline', 'ATLASUSERDISK:10007', '/pnfs/usatlas.bnl.gov/atlasuserdisk/'),
                 ('atlasgroup-disk-link-group:replica:nearline', 'ATLASGROUPDISK:10006', '/pnfs/usatlas.bnl.gov/atlasgroupdisk/')
                ]
        for grp, res, path in lkgrp:
            found_sa = False
            for entry in entries:
                if 'GlueSA' not in entry.objectClass:
                    continue
                if grp not in entry.glue['SALocalID']:
                    continue
                found_sa = True
                self.failUnless(path in entry.glue['SAPath'])
            self.failUnless(found_sa, msg="Could not find target SA")
            found_vo = False
            for entry in entries:
                if 'GlueVOInfo' not in entry.objectClass:
                    continue
                if res not in entry.glue['VOInfoLocalID']:
                    continue
                found_vo = True
                self.failUnless(path in entry.glue['VOInfoPath'])
            self.failUnless(found_vo, msg="Could not find target VOInfo.")

    def test_ngdf_config(self):
        entries, cp = self.run_test_config('red-se-test3.conf')
        self.checkReservedRules1(entries)
        self.checkReservedRules2(entries)
        self.checkReducedAmount(entries)

    def test_fnal_config(self):
        entries, cp = self.run_test_config('red-se-test4.conf')

    def test_atlas_config(self):
        entries, cp = self.run_test_config('red-se-test6.conf')
        self.checkPaths(entries)

    def test_vo_dirs_config(self):
        """
        This test verifies that the VO dirs overrides work for the new-style
        configs.
        """
        entries, cp = self.run_test_config('red-se-test5.conf')
        found_cms_voinfo = False
        for entry in entries:
            if 'GlueVOInfo' not in entry.objectClass:
                continue
            if entry.glue['VOInfoName'][0] != 'cms:default':
                continue
            found_cms_voinfo = True
            self.failUnless(entry.glue['VOInfoPath'][0] == \
                '/pnfs/unl.edu/data4/cms/store', msg="vo_dirs override failed.")
        self.failUnless(found_cms_voinfo, msg="VOInfo for CMS missing.")

    def verify_bestman_output(self, info):
        """
        Verify that the two example gsiftp servers are picked up.
        Verify that the version number is correct.
        """
        gsiftp = 'gsiftp://cithep160.ultralight.org:5000;' \
            'gsiftp://cithep251.ultralight.org:5000'
        self.failUnless(info['gsiftpTxfServers'] == gsiftp, msg="Incorrect " \
            "gsiftp server line.")
        self.failUnless(info['backend_version'] == '2.2.1.2.i4', msg=\
            "Incorrect backend version number.")

    def test_bestman_output(self):
        """
        Test to make sure we can parse BestMan output for the new and old server
        responses, as well as the new and old client formats
        """
        os.environ['GIP_TESTING'] = "1"
        cfg_name = os.path.join("test_configs", "red-se-test.conf")
        cp = config(cfg_name)
        info = srm_ping.bestman_srm_ping(cp, "1")
        self.verify_bestman_output(info)
        srm_ping.bestman_srm_ping(cp, "2")
        self.verify_bestman_output(info)

    def check_srm_3_ldif(self, entries, cp):
        vos = voList(cp)
        found_srm = False
        for entry in entries:
            if 'GlueService' not in entry.objectClass:
                continue
            if 'httpg://srm.unl.edu:8443/srm/v2/server' not in \
                    entry.glue['ServiceName']:
                continue
            found_srm = True
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

    def check_srmcp_3_ldif(self, entries):
        found_cp = False
        for entry in entries:
            if 'GlueSEControlProtocol' not in entry.objectClass:
                continue
            if entry.glue['SEControlProtocolLocalID'][0] != 'srm.unl.edu_srmv2':
                continue
            found_cp = True
            self.failUnless('httpg://srm.unl.edu:8443/srm/v2/server' in \
                entry.glue['SEControlProtocolEndpoint'])
            self.failUnless('2.2.0' in entry.glue['SEControlProtocolVersion'])
            self.failUnless('SRM' in entry.glue['SEControlProtocolType'])
            self.failUnless('GlueSEUniqueID=dcache07.unl.edu' in \
                entry.glue['ChunkKey'])
        self.failUnless(found_cp, msg="Could not find the SRM Control Prot.")

    def test_multisrm_output(self):
        """
        Verify that the SRM section in the config.ini can accept a comma-sep
        list of SRM endpoints.
        """
        entries, cp = self.run_test_config("red-se-test-multisrm.conf")
        self.check_srm_3_ldif(entries, cp)
        self.check_srm_2_ldif(entries, cp)
        self.check_srmcp_3_ldif(entries)
        self.check_srmcp_2_ldif(entries)
        self.check_se_2_ldif(entries)
        self.check_sa_2_ldif(entries, cp)
        self.check_voview_2_ldif(entries, cp)

    def check_srm_mit_ldif(self, entries):
        found_srm = False
        for entry in entries:
            if 'GlueService' not in entry.objectClass:
                continue
            if 'httpg://se01.cmsaf.mit.edu:8443/srm/managerv2' not in \
                    entry.glue['ServiceName']:
                continue
            found_srm = True
            self.failUnless(entry.glue['ServiceEndpoint'] == \
                entry.glue['ServiceName'])
            self.failUnless(entry.glue['ServiceAccessPointURL'] == \
                entry.glue['ServiceName'])
            self.failUnless(entry.glue['ServiceURI'] == \
                entry.glue['ServiceName'])
            self.failUnless("GlueSiteUniqueID=MIT_CMS" in \
                entry.glue['ForeignKey'])
            self.failUnless(entry.glue["ServiceType"][0] == 'SRM')
            self.failUnless(entry.glue["ServiceVersion"][0] == '2.2.0')
        self.failUnless(found_srm, msg="Could not find the SRM entity for " \
            "se01.cmsaf.mit.edu")
        found_cp = False
        for entry in entries:
            if 'GlueSEControlProtocol' not in entry.objectClass:
                continue
            if entry.glue['SEControlProtocolLocalID'][0] != \
                    'se01.cmsaf.mit.edu_srmv2':
                continue
            found_cp = True
            self.failUnless('httpg://se01.cmsaf.mit.edu:8443/srm/managerv2' in \
                entry.glue['SEControlProtocolEndpoint'])
            self.failUnless('2.2.0' in entry.glue['SEControlProtocolVersion'])
            self.failUnless('SRM' in entry.glue['SEControlProtocolType'])
            self.failUnless('GlueSEUniqueID=se01.cmsaf.mit.edu' in \
                entry.glue['ChunkKey'])
        self.failUnless(found_cp, msg="Could not find the SRM Control Prot for"\
            " se01.cmsaf.mit.edu")

    def check_pools_mit_ldif(self, entries):
        found_pool_sa = False
        for entry in entries:
            if 'GlueSA' not in entry.objectClass:
                continue
            if 'test-pools:custodial:nearline' not in entry.glue['SALocalID']:
                continue
            found_pool_sa = True
            self.failUnless('15849' in entry.glue['SATotalOnlineSize'])
            self.failUnless('4373' in entry.glue['SAUsedOnlineSize'])
            self.failUnless('11476' in entry.glue['SAFreeOnlineSize'])
            self.failUnless('SRMTokenReservedSpace=0' in \
                entry.glue['SACapability'])
            self.failUnless('InstalledOnlineCapacity=15849' in \
                entry.glue['SACapability'])
            self.failUnless('GlueSEUniqueID=se01.cmsaf.mit.edu' in \
                entry.glue['ChunkKey'])
            self.failUnless(entry.glue['SALocalID'][0] == \
                entry.glue['SAName'][0])
        self.failUnless(found_pool_sa, msg="Could not find GlueSA entry for " \
            "the test-pools pool group")

    def check_links_mit_ldif(self, entries, cp):
        found_link_sa = False
        vos = voList(cp)
        for entry in entries:
            if 'GlueSA' not in entry.objectClass:
                continue
            if 'opportunistic-link-group:replica:nearline' not in \
                    entry.glue['SALocalID']:
                continue
            for vo in vos:
                #self.failIf(vo not in entry.glue['ServiceAccessControlRule'])
                self.failIf("VO:%s" % vo not in \
                    entry.glue['SAAccessControlBaseRule'], msg="String `" \
                    "VO:%s` not in ACBR" % vo)
            found_link_sa = True
            self.failUnless('1954' in entry.glue['SATotalOnlineSize'])
            self.failUnless('0' in entry.glue['SAUsedOnlineSize'])
            self.failUnless('1954' in entry.glue['SAFreeOnlineSize'])
            self.failUnless('SRMTokenReservedSpace=0' in \
                entry.glue['SACapability'])
            self.failUnless('InstalledOnlineCapacity=1954' in \
                entry.glue['SACapability'])
            self.failUnless('GlueSEUniqueID=se01.cmsaf.mit.edu' in \
                entry.glue['ChunkKey'])
            self.failUnless(entry.glue['SALocalID'][0] == \
                entry.glue['SAName'][0])
        self.failUnless(found_link_sa, msg="Could not find GlueSA entry for " \
            "the test-pools pool group")

    def check_se_mit_ldif(self, entries):
        found_se = False
        for entry in entries:
            if not ('GlueSE' in entry.objectClass and entry.glue.\
                    get('SEUniqueID', [''])[0] == 'se01.cmsaf.mit.edu'):
                continue
            found_se = True
            self.failUnless(entry.glue['SEName'][0] == 'MIT dCache')
            self.failUnless(entry.glue['SEImplementationName'][0] == 'dcache')
            self.failUnless(entry.glue['SEImplementationVersion'][0] == \
                'cells')
            self.failUnless(entry.glue['SEPort'][0] == '8443')
            self.failUnless('370989' in entry.glue['SESizeTotal'])
            self.failUnless('73280' in entry.glue['SESizeFree'])
            self.failUnless('multi-disk' in entry.glue['SEArchitecture'])
            self.failUnless('GlueSiteUniqueID=MIT_CMS' in entry.glue\
                ['ForeignKey'])
        self.failUnless(found_se, msg="GlueSE entry for srm.unl.edu missing.")

    def test_mit_output(self):
        entries, cp = self.run_test_config("mit-se-test.conf")        
        self.check_srm_mit_ldif(entries)
        self.check_pools_mit_ldif(entries)
        self.check_links_mit_ldif(entries, cp)
        self.check_se_mit_ldif(entries)

def main():
    os.environ['GIP_TESTING'] = '1'
    cp = config("test_configs/red-se-test.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSEConfigs, stream, per_site=False)

if __name__ == '__main__':
    main()

