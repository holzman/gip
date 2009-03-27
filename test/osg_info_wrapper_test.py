
import os
import sys
import time
import unittest

#Standard testing imports:
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get
from gip_testing import runTest, streamHandler

log = getLogger("GIP.Test.Wrapper")

#Add the path with the osg_info_wrapper script:
sys.path.append(os.path.expandvars("$GIP_LOCATION/libexec"))
import osg_info_wrapper

class TestOsgInfoWrapper(unittest.TestCase):

    def test_simple(self):
        """
        Simple test of the OSG Info Wrapper.  Make sure that both the provider
        and plugin functionality works.
        """
        cp = config("test_modules/simple/config")
        entries = osg_info_wrapper.main(cp, return_entries=True)
        has_timestamp = False
        has_ce = False
        for entry in entries:
            if entry.glue.get('LocationName', (0,))[0] == 'TIMESTAMP':
                has_timestamp = True
            if entry.dn[0] == 'GlueCEUniqueID=red.unl.edu:2119/jobmanager' \
                    '-pbs-workq':
                has_ce = True
                self.assertEquals(entry.glue['CEPolicyAssignedJobSlots'][0], \
                    '1234', msg="Plugin did not get applied properly")
        self.assertEquals(has_timestamp, True, msg="Provider did not run.")
        self.assertEquals(has_ce, True, msg="Static info was not included.")

    def test_nonglue(self):
        """
        Test the ability to handle non-GLUE attributes (such as FermiGrid's
        custom extensions).
        """
        cp = config("test_modules/nonglue/config")
        entries = osg_info_wrapper.main(cp, return_entries=True)
        has_ce = False
        for entry in entries:
            if entry.dn[0] == 'GlueCEUniqueID=red.unl.edu:2119/jobmanager' \
                    '-pbs-cms':
                has_ce = True
                self.assertEquals(entry.nonglue['FermiGridMemoryPerNode'][0], \
                    '5', msg="Add-attributes did not get applied")
                self.assertEquals(entry.nonglue['FermiGridBatchSlotsPerNode'] \
                    [0], '5', msg="Alter-attributes did not get applied")
        self.assertEquals(has_ce, True, msg="Static info was not included.")

    def test_timeout(self):
        """
        Test a plugin which times out.

        The timeout value is when the module kills itself.
        """
        cp = config("test_modules/timeout/config")
        cp.set("gip", "timeout", "5")
        cp.set("gip", "flush_cache", "True")
        t1 = time.time()
        entries = osg_info_wrapper.main(cp, return_entries=True)
        t1 = time.time() - t1
        has_ent_1 = False
        has_ent_2 = False
        for entry in entries:
            if entry.dn[0] == 'GlueVOInfoLocalID=osgedu':
                has_ent_1 = True
            if entry.dn[0] == 'GlueVOInfoLocalID=fmri':
                has_ent_2 = True
        self.assertEquals(has_ent_1, False, msg="Module timeout_15 was not" \
            " killed quickly enough.")
        self.assertEquals(has_ent_2, False, msg="Module timeout_20 was not" \
            " killed quickly enough.")
        self.assertEquals(t1 < 6.0, True, msg="Test did not end quick enough.")

    def test_response(self):
        """
        Test a provider which goes above the response time.

        The response value is when the osg-info-wrapper starts killing its
        children.
        """
        cp = config("test_modules/timeout/config")
        cp.set("gip", "timeout", "50")
        cp.set("gip", "response", "5")
        cp.set("gip", "flush_cache", "True")
        t1 = time.time()
        entries = osg_info_wrapper.main(cp, return_entries=True)
        t1 = time.time() - t1
        has_ent_1 = False
        has_ent_2 = False
        for entry in entries:
            if entry.dn[0] == 'GlueVOInfoLocalID=osgedu':
                has_ent_1 = True
            if entry.dn[0] == 'GlueVOInfoLocalID=fmri':
                has_ent_2 = True
        self.assertEquals(has_ent_1, False, msg="Module timeout_15 was not" \
            " ignored quickly enough.")
        self.assertEquals(has_ent_2, False, msg="Module timeout_20 was not" \
            " ignored quickly enough.")
        self.assertEquals(t1 < 6, True, msg="Response was not handled " \
            "correctly.")

    def test_multi_providers(self):
        """
        Test multiple providers.
        """
        cp = config("test_modules/simple/config")
        entries = osg_info_wrapper.main(cp, return_entries=True)
        has_ent_1 = False
        has_ent_2 = False
        for entry in entries:
            if entry.dn[0] == 'GlueVOInfoLocalID=pragma':
                has_ent_1 = True
            if entry.dn[0] == 'GlueLocationLocalID=TIMESTAMP':
                has_ent_2 = True
        self.assertEquals(has_ent_1, True, msg="Module random_entry was not" \
            " included.")
        self.assertEquals(has_ent_2, True, msg="Module osg-info-timestamp was" \
            " not included.")

    def test_alter_attributes(self):
        """
        Make sure the alter-attributes.conf file works properly
        """
        cp = config("test_modules/simple/config")
        cp.set("gip","alter_attributes", "test_modules/simple/alter_attributes")
        has_ce = False
        entries = osg_info_wrapper.main(cp, return_entries=True)
        for entry in entries:
            if entry.dn[0] == 'GlueCEUniqueID=red.unl.edu:2119/jobmanager' \
                    '-pbs-workq':
                has_ce = True
                self.assertEquals(entry.glue['CEPolicyAssignedJobSlots'][0], \
                    '1235', msg="Plugin did not get applied properly")
        self.assertEquals(has_ce, True, msg="Static info was not included.")


    def test_add_attributes(self):
        """
        Make sure the add-attributes.conf file works properly
        """
        cp = config("test_modules/simple/config")
        cp.set("gip", "add_attributes", "test_modules/simple/add_attributes")
        has_ce = False
        entries = osg_info_wrapper.main(cp, return_entries=True)
        for entry in entries:
            if entry.dn[0] == 'GlueCEUniqueID=red.unl.edu:2119/jobmanager-pbs' \
                    '-cms':
                has_ce = True
        self.assertEquals(has_ce, True, msg="The entry in the sample " \
            "add_attributes file was not found!")

    def test_remove_attributes(self):
        """
        Make sure the remove-attributes.conf file works properly
        """
        cp = config("test_modules/simple/config")
        cp.set("gip", "remove_attributes", \
            "test_modules/simple/remove_attributes")
        has_no_time = True
        entries = osg_info_wrapper.main(cp, return_entries=True)
        for entry in entries:
             if entry.dn[0] == 'GlueLocationLocalID=TIMESTAMP':
                 has_no_time = False
        self.assertEquals(has_no_time, True, msg="The entry from the timestamp"\
            " provider is present, and it should have been removed.")

    def test_static_ldif(self):
        """
        Test the ability to include static LDIF in the GIP.
        """
        cp = config("test_modules/simple/config")
        entries = osg_info_wrapper.main(cp, return_entries=True)
        has_timestamp = False
        has_ce = False
        for entry in entries:
            if entry.glue.get('LocationName', (0,))[0] == 'TIMESTAMP':
                has_timestamp = True
            if entry.dn[0] == 'GlueCEUniqueID=red.unl.edu:2119/jobmanager' \
                    '-pbs-workq':
                has_ce = True
        self.assertEquals(has_ce, True, msg="Static info was not included.")

    def test_cache_flush(self):
        """
        Make sure that osg-info-wrapper flushes the cache properly.
        """
        cp = config("test_modules/cache_flush/config")
        cp.set("gip", "flush_cache", "False")
        entries = osg_info_wrapper.main(cp, return_entries=True)
        timestamp_entry = entries[0]
        t1 = float(timestamp_entry.glue['LocationVersion'][0])
        cp.set("gip", "flush_cache", "True")
        entries = osg_info_wrapper.main(cp, return_entries=True)
        timestamp_entry = entries[0]
        t2 = float(timestamp_entry.glue['LocationVersion'][0])
        self.assertTrue(t1 < t2)

def main():
    """
    The main entry point for testing the osg-info-wrapper implementation.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestOsgInfoWrapper, stream, per_site=False)

if __name__ == '__main__':
    main()


