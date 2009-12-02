#!/usr/bin/env python

import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap

class TestSubclusterConfigs(unittest.TestCase):

    def run_test_config(self, testconfig):
        os.environ['GIP_TESTING'] = "1"
        cfg_name = os.path.join("test_configs", testconfig)
        if not os.path.exists(cfg_name):
           self.fail(msg="Test configuration %s does not exist." % cfg_name)
        cp = config(cfg_name)
        providers_path = cp_get(cp, "gip", "provider_dir", "$GIP_LOCATION/" \
            "providers")
        providers_path = os.path.expandvars(providers_path)
        subcluster_provider_path = os.path.join(providers_path,
            'site_topology.py')
        cmd = subcluster_provider_path + " --config %s" % cfg_name
        print cmd
        fd = os.popen(cmd)
        entries = read_ldap(fd, multi=True)
        self.failIf(fd.close(), msg="Run of subcluster provider failed!")
        return entries

    def check_red_sc_1(self, entries, new=False):
        found_entry = False
        for entry in entries:
            if 'GlueSubCluster' not in entry.objectClass:
                continue
            if entry.glue.get('SubClusterUniqueID', [None])[0] != 'red.unl.edu-Nebraska':
                continue
            found_entry = True
            self.failUnless(entry.glue['SubClusterPhysicalCPUs'][0] == '120')
            self.failUnless(entry.glue['SubClusterWNTmpDir'][0] == '/scratch')
            self.failUnless(entry.glue['HostNetworkAdapterOutboundIP'][0] == \
                'TRUE')
            self.failUnless(entry.glue['HostProcessorModel'][0] == \
                'Opteron 275')
            self.failUnless(entry.glue['SubClusterLogicalCPUs'][0] == '240')
            self.failUnless(entry.glue['HostNetworkAdapterInboundIP'][0] == \
                'FALSE')
            self.failUnless(entry.glue['SubClusterTmpDir'][0] == \
                '/opt/osg/data')
            self.failUnless(entry.glue['HostBenchmarkSI00'][0] == '2000')
            self.failUnless(entry.glue['HostProcessorVendor'][0] == 'AMD')
            self.failUnless(entry.glue['HostMainMemoryRAMSize'][0] == '4000')
            self.failUnless(entry.glue['HostMainMemoryVirtualSize'][0] == \
                '4000')
            self.failUnless(entry.glue['HostBenchmarkSF00'][0] == '2000')
            self.failUnless(entry.glue['HostArchitectureSMPSize'][0] == '2')
            self.failUnless(entry.glue['HostProcessorClockSpeed'][0] == '2200')
            if new:
                self.failUnless(entry.glue['HostProcessorOtherDescription'][0] == \
                    'Cores=%i, Benchmark=%i-HEP-SPEC06' % (240, 8))
        self.failUnless(found_entry, msg="Test subcluster red.unl.edu not" \
            "found!")

    def check_red_sc_2(self, entries):
        found_entry = False
        for entry in entries:
            if 'GlueSubCluster' not in entry.objectClass:
                continue
            if entry.glue.get('SubClusterUniqueID', [None])[0] != 'Dell Nodes-Nebraska':
                continue
            found_entry = True
            self.failUnless(entry.glue['SubClusterPhysicalCPUs'][0] == '106')
            self.failUnless(entry.glue['SubClusterWNTmpDir'][0] == '/scratch')
            self.failUnless(entry.glue['HostNetworkAdapterOutboundIP'][0] == \
                'TRUE')
            self.failUnless(entry.glue['HostProcessorModel'][0] == 'Dual-Core '\
                'AMD Opteron(tm) Processor 2216')
            self.failUnless(entry.glue['SubClusterLogicalCPUs'][0] == '212')
            self.failUnless(entry.glue['HostNetworkAdapterInboundIP'][0] == \
                'FALSE')
            self.failUnless(entry.glue['SubClusterTmpDir'][0] == '/opt/osg/data')
            self.failUnless(entry.glue['HostBenchmarkSI00'][0] == '2000')
            self.failUnless(entry.glue['HostProcessorVendor'][0] == 'AMD')
            self.failUnless(entry.glue['HostMainMemoryRAMSize'][0] == '4110')
            self.failUnless(entry.glue['HostMainMemoryVirtualSize'][0] == '4110')
            self.failUnless(entry.glue['HostBenchmarkSF00'][0] == '2000')
            self.failUnless(entry.glue['HostArchitectureSMPSize'][0] == '2')
            self.failUnless(entry.glue['HostProcessorClockSpeed'][0] == '2400')
            self.failUnless(entry.glue['HostProcessorOtherDescription'][0] == \
                'Cores=%i' % (53*4))
        self.failUnless(found_entry, msg="Test subcluster 'Dell Nodes' not" \
            "found!")

    def test_old_sc_config(self):
        entries = self.run_test_config("red-sc-test.conf")
        self.check_red_sc_1(entries)
        self.check_red_sc_2(entries)

    def test_new_sc_config(self):
        entries = self.run_test_config("red-sc-test2.conf")
        self.check_red_sc_1(entries, new=True)
        self.check_red_sc_2(entries)

def main():
    os.environ['GIP_TESTING'] = '1'
    cp = config("test_configs/red-sc-test.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSubclusterConfigs, stream, per_site=False)

if __name__ == '__main__':
    main()

