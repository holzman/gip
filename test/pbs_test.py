#!/usr/bin/env python

import os
import sys
import unittest

os.environ['GIP_TESTING'] = '1'
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_sets import Set
from gip_common import config, cp_get
from pbs_common import getVoQueues
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler
import gip_testing

example_queues = \
   [('cms', 'lcgadmin'),  ('gridex', 'default'), ('cms', 'cmsprod'),
    ('atlas', 'atlas'),   ('osg', 'workq'),      ('osgedu', 'workq'),
    ('mis', 'workq'),     ('fmri', 'workq'),     ('grase', 'workq'),
    ('gridex', 'workq'),  ('ligo', 'workq'),     ('ivdgl', 'workq'),
    ('gadu', 'workq'),    ('GLOW', 'workq'),     ('cdf', 'workq'),
    ('nanohub', 'workq'), ('sdss', 'workq'),     ('gpn', 'workq'),
    ('engage', 'workq'),  ('cms', 'cms'),        ('dzero', 'dzero'),
   ]
    

example_queues = Set(example_queues)

class TestPbsDynamic(unittest.TestCase):

    def test_dynamic_provider(self):
        """
        Checks to make sure that the PBS dynamic provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg-info-provider-pbs"\
            ".py --config=test_configs/red.conf")
        fd = os.popen(path)
        fd.read()
        self.assertEquals(fd.close(), None)

    def test_vo_queues(self):
        os.environ['GIP_TESTING'] = '1'
        cp = config("test_configs/red.conf")
        vo_queues = Set(getVoQueues(cp))
        print vo_queues
        diff = vo_queues.symmetric_difference(example_queues)
        self.assertEquals(len(diff), 0, msg="The following VO-queues are " \
            "different between the expected and actual: %s" % str(diff))

    def test_lbl_entries(self):
        """
        Checks the LBL information, as they don't have PBSPro
        """
        # Switch commands over to the LBL ones:
        old_commands = dict(gip_testing.commands)
        try:
            os.environ['GIP_TESTING'] = 'suffix=lbl'
            path = os.path.expandvars("$GIP_LOCATION/libexec/" \
                "osg-info-provider-pbs.py --config=test_configs/red.conf")
            fd = os.popen(path)
            entries = read_ldap(fd)
            self.assertEquals(fd.close(), None)
        finally:
            gip_testing.commands = old_commands
        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                self.assertEquals(entry.glue['CEStateTotalJobs'], '6')
                self.assertEquals(entry.glue['CEStateRunningJobs'], '6')
                self.assertEquals(entry.glue['CEStateFreeCPUs'], '51')
                self.assertEquals(entry.glue['CEStateFreeJobSlots'], '51')
                self.assertEquals(entry.glue['CEPolicyAssignedJobSlots'], '60')
                self.assertEquals(entry.glue['CEUniqueID'], \
                    'red.unl.edu:2119/jobmanager-pbs-batch')


    def test_red_entries(self):
        """
        Make sure that VOLocal gets the correct queue information.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/" \
            "osg-info-provider-pbs.py --config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)
        for entry in entries:
            if 'GlueVOView' in entry.objectClass and \
                    entry.glue['VOViewLocalID'] == 'cms' and \
                    entry.glue['ChunkKey']=='GlueCEUniqueID=red.unl.edu:2119/'\
                    'jobmanager-pbs-cms':
                self.failUnless(entry.glue['CEStateRunningJobs'] == '203')
                self.failUnless(entry.glue['CEStateWaitingJobs'] == '1')
                self.failUnless(entry.glue['CEStateTotalJobs'] == '204')

    def test_max_queuable(self):
        """
        Regression test for the max_queuable attribute.  Ticket #10.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/" \
            "osg-info-provider-pbs.py --config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)
        has_default_ce = False
        for entry in entries:
            if 'GlueCE' in entry.objectClass and \
                    entry.glue['CEUniqueID'] == 'red.unl.edu:2119/jobmanager' \
                    '-pbs-default':
                self.failUnless(entry.glue['CEPolicyMaxWaitingJobs'] == '2000')
                has_default_ce = True
        self.failUnless(has_default_ce, msg="Default queue's CE was not found!")

    def test_max_queuable_22(self):
        """
        Regression test for the max_queuable attribute.  Ticket #22.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/" \
            "osg-info-provider-pbs.py --config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)
        has_default_ce = False
        has_lcgadmin_ce = False
        for entry in entries:
            if 'GlueCE' in entry.objectClass and \
                    entry.glue['CEUniqueID'] == 'red.unl.edu:2119/jobmanager' \
                    '-pbs-default':
                self.failUnless(entry.glue['CEPolicyMaxTotalJobs'] == '2000')
                has_default_ce = True
            if 'GlueCE' in entry.objectClass and \
                    entry.glue['CEUniqueID'] == 'red.unl.edu:2119/jobmanager' \
                    '-pbs-lcgadmin':
                print entry
                self.failUnless(entry.glue['CEPolicyMaxWaitingJobs'] == '2000')
                has_lcgadmin_ce = True
        self.failUnless(has_default_ce, msg="Default queue's CE was not found!")
        self.failUnless(has_default_ce, msg="lcgadmin queue's CE was not found!")

    def make_site_tester(site):
        def test_site_entries(self):
            """
            Checks the %s information.
            """ % site
            # Switch commands over to the site ones:
            os.environ['GIP_TESTING'] = 'suffix=%s' % site
            path = os.path.expandvars("$GIP_LOCATION/libexec/" \
                "osg-info-provider-pbs.py --config=test_configs/%s.conf" % site)
            fd = os.popen(path)
            entries = read_ldap(fd)
            self.assertEquals(fd.close(), None)
        return test_site_entries

    test_cigi_entries = make_site_tester("cigi")
    test_uc_entries = make_site_tester("uc")

def main():
    """
    The main entry point for when pbs_test is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestPbsDynamic, stream, per_site=False)

if __name__ == '__main__':
    main()
