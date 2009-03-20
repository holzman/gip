#!/usr/bin/env python

import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from gip_cese_bind import getCEList, getSEList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap


class TestCESEBind(unittest.TestCase):

     def setUp(self):
         self.filename = "test_configs/red.conf"
         self.setUpLDAP()

     def setUpLDAP(self):
         os.environ['GIP_TESTING'] = "1"
         cp = config(self.filename)
         self.ces = getCEList(cp)
         self.ses = getSEList(cp)
         cese_provider_path = os.path.expandvars("$GIP_LOCATION/libexec/" \
             "osg_info_cesebind.py --config %s" % self.filename)
         fd = os.popen(cese_provider_path)
         self.entries = read_ldap(fd, multi=True)
         self.exit_status = fd.close()

     def _test_exists(self, *dns):
         rng = range(len(dns))
         for entry in self.entries:
             if len(entry.dn) < len(dns):
                 continue
             matches = True
             for idx in rng:
                 if entry.dn[idx] != dns[idx]:
                     matches = False
                     break
             if matches:
                 return entry
         self.fail(msg="Missing the entry starting with dn: %s" % ','.join(dns))

     def test_cese_portion(self):
         # Make sure there are two SEs; classic and dCache
         self.failUnless(len(self.ses) == 2, msg="Only one SE present!")
         for ce in self.ces:
             # 1) Make sure there's a CESEBindGroup
             self._test_exists("GlueCESEBindGroupCEUniqueID=%s" % ce)
             for se in self.ses:
                 # 2) Make sure there is a matching SE portion
                 entry = self._test_exists("GlueCESEBindSEUniqueID=%s" % se,
                     "GlueCESEBindGroupCEUniqueID=%s" % ce)
                 # 3) Make sure there's a non-zero-length access point
                 self.failIf(len(entry.glue['CESEBindCEAccesspoint'][0])==0)

     def test_cese_disabledse(self):
         self.filename = 'test_configs/disabled_se.conf'
         self.setUpLDAP()
         has_cese_bind = False
         for entry in self.entries:
             if 'GlueCESEBindGroup' not in entry.objectClass:
                 continue
             has_cese_bind = True
             if 'srm.unl.edu' in entry.glue['CESEBindGroupSEUniqueID']:
                 self.fail(msg="There is a CESE entry for the disabled SE.")
         self.failUnless(has_cese_bind, msg="No CESE bind present.")

def main():
    cp = config("test_configs/red.conf")
    stream = streamHandler(cp)
    runTest(cp, TestCESEBind, stream, per_site=False)

if __name__ == '__main__':
    main()

