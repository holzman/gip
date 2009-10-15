#!/usr/bin/env python

import os
import sys
import unittest

os.environ['GIP_TESTING'] = '1'
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_sets import Set
from gip_common import config, cp_get
#from pbs_common import getVoQueues, getQueueList
from gip.batch_systems.pbs import PbsBatchSystem
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

rvf_example_queues = Set(['lcgadmin', 'atlas', 'workq'])

class TestPbsDynamic(unittest.TestCase):

    def test_dynamic_provider(self):
        """
        Checks to make sure that the PBS dynamic provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
        fd = os.popen(path)
        fd.read()
        self.assertEquals(fd.close(), None)

    def test_vo_queues(self):
        os.environ['GIP_TESTING'] = '1'
        cp = config("test_configs/red.conf")
        old_globus_loc = os.environ.get("GLOBUS_LOCATION", None)
        if old_globus_loc != None:
            del os.environ['GLOBUS_LOCATION']
        try:
            pbs = PbsBatchSystem(cp)
            vo_queues = Set(pbs.getVoQueues())
        finally:
            if old_globus_loc != None:
                os.environ['GLOBUS_LOCATION'] = old_globus_loc
        diff = vo_queues.symmetric_difference(example_queues)
        self.failIf(diff, msg="The following VO-queues are " \
            "different between the expected and actual:\n%s\nExpected:\n%s"\
            "\nActual:\n%s." % (", ".join([str(i) for i in diff]),
             ", ".join([str(i) for i in example_queues]),
            ", ".join([str(i) for i in vo_queues])))

    def test_lbl_entries(self):
        """
        Checks the LBL information, as they don't have PBSPro
        """
        # Switch commands over to the LBL ones:
        old_commands = dict(gip_testing.commands)
        try:
            os.environ['GIP_TESTING'] = 'suffix=lbl'
            path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py"\
                                      " --config=test_configs/red.conf")
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
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
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
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
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
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
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
                self.failUnless(entry.glue['CEPolicyMaxWaitingJobs'] == '183')
                has_lcgadmin_ce = True
        self.failUnless(has_default_ce, msg="Default queue's CE was not found!")
        self.failUnless(has_default_ce, msg="lcgadmin queue's CE was not found!")

    def test_max_running(self):
        """
        Regression test for max_running.  Ensure that the number of free slots
        & total reported is never greater than the # of max running.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)
        has_dzero_ce = False
        for entry in entries:
            if 'GlueCE' in entry.objectClass and \
                    entry.glue['CEUniqueID'] == 'red.unl.edu:2119/jobmanager' \
                    '-pbs-dzero':
                self.failUnless(entry.glue['CEPolicyMaxRunningJobs'] == '158')
                self.failUnless(entry.glue['CEPolicyMaxTotalJobs'] == '158')
                self.failUnless(entry.glue['CEStateFreeJobSlots'] == '158')
                has_dzero_ce = True
        self.failUnless(has_dzero_ce, msg="dzero queue's CE was not found!")

    def test_rvf_queues(self):
        """
        Regression test for the RVF files.
        """
        os.environ['GIP_TESTING'] = '1'
        old_globus_loc = os.environ.get('GLOBUS_LOCATION', None)
        try:
            os.environ['GLOBUS_LOCATION'] = 'test_configs/globus'
            cp = config('test_configs/pbs_rvf.conf')
            pbs = PbsBatchSystem(cp)
            queue_set = Set(pbs.getQueueList())
            vo_queues = pbs.getVoQueues()
        finally:
            if old_globus_loc != None:
                os.environ['GLOBUS_LOCATION'] = old_globus_loc
        queue_set2 = Set([i[1] for i in vo_queues])
        diff = queue_set.symmetric_difference(queue_set2)
        self.failIf(diff, msg="Disagreement in queue list between getVoQueues"\
            " and getQueueList: %s" % ", ".join(diff))
        diff = queue_set.symmetric_difference(rvf_example_queues)
        self.failIf(diff, msg="Disagreement between queue list and reference"\
            " values: %s" % ", ".join(diff))

    def test_max_queuable_26(self):
        """
        Regression test for max_queuable.  Ensure that the number of free slots
        reported is never greater than the # of max queuable
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)
        has_lcgadmin_ce = False
        for entry in entries:
            if 'GlueCE' in entry.objectClass and \
                    entry.glue['CEUniqueID'] == 'red.unl.edu:2119/jobmanager' \
                    '-pbs-lcgadmin':
                self.failUnless(entry.glue['CEPolicyMaxWaitingJobs'] == '183')
                self.failUnless(entry.glue['CEStateFreeCPUs'] == '183')
                self.failUnless(entry.glue['CEStateFreeJobSlots'] == '183')
                has_lcgadmin_ce = True
        self.failUnless(has_lcgadmin_ce, msg="lcgadmin queue's CE was not found!")

    def test_contact_string(self):
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py " \
                                  "--config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)

        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                contact_string = entry.glue['CEInfoContactString']
                self.failIf(contact_string == "", "Contact string is missing")
                self.failIf(contact_string.endswith("jobmanager-pbs"), \
                    "Contact string must include the queue.")
                
    def make_site_tester(site):
        def test_site_entries(self):
            """
            Checks the %s information.
            """ % site
            # Switch commands over to the site ones:
            os.environ['GIP_TESTING'] = 'suffix=%s' % site
            path = os.path.expandvars("$GIP_LOCATION/providers/batch_system.py"\
                                      " --config=test_configs/%s.conf" % site)
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
    cp = config("test_configs/red.conf")
    stream = streamHandler(cp)
    runTest(cp, TestPbsDynamic, stream, per_site=False)

if __name__ == '__main__':
    main()
