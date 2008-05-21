
import unittest

from gip_common import config

class TestOsgInfoWrapper(unittest.TestCase):

    def test_simple(self):
        """
        Simple test of the OSG Info Wrapper.  Make sure that both the provider
        and plugin functionality works.
        """
        raise NotImplementedError()

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
        cp = config("test_modules/simple/config")
        cp.set("gip", "flush_cache", "False")
        entries = osg_info_wrapper.main(cp, return_entries)
        timestamp_entry = entries[0]
        t1 = float(timestamp_entry.glue['LocationVersion'])
        cp.set("gip", "flush_cache", "True")
        entries = osg_info_wrapper.main(cp, return_entries)
        timestamp_entry = entries[0]
        t2 = float(timestamp_entry.glue['LocationVersion'])
        self.assertTrue(t1 < t2)

