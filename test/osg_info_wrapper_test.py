
import os
import sys
import unittest

#Standard testing imports:
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from gip_testing import runTest, streamHandler

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
            print entry
            if entry.glue['LocationName'][0] == 'TIMESTAMP':
                has_timestamp = True
            if entry.dn[0] == 'GlueCEUniqueID=red.unl.edu:2119/jobmanager' \
                    '-pbs-workq':
                has_ce = True
                self.assertEquals(entry.glue['CEPolicyAssignedJobSlots'][0], \
                    '1234', msg="Plugin did not get applied properly")
        self.assertEquals(has_timestamp, True, msg="Provider did not run.")
        self.assertEquals(has_ce, True, msg="Static info was not included.")

    def test_timeout(self):
        """
        Test a plugin which times out.
        """
        raise NotImplementedError()

    def test_timeout2(self):
        """
        Test a provider which times out; make sure we reject all of its data.
        """
        raise NotImplementedError()

    def test_alter_attributes(self):
        """
        Make sure the alter-attributes.conf file works properly
        """
        raise NotImplementedError()

    def test_add_attributes(self):
        """
        Make sure the add-attributes.conf file works properly
        """
        raise NotImplementedError()

    def test_remove_attributes(self):
        """
        Make sure the remove-attributes.conf file works properly
        """
        raise NotImplementedError()

    def test_static_ldif(self):
        """
        Test the ability to include static LDIF in the GIP.
        """
        raise NotImplementedError()

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
        print t1, t2
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


