
import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler

class TestSlurmDynamic(unittest.TestCase):

    def test_dynamic_provider(self):
        """
        Checks to make sure that the SLURM dynamic provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg_info_provider_slurm"\
            ".py --config=test_configs/red.conf")
        print path
        fd = os.popen(path)
        fd.read()
        self.assertEquals(fd.close(), None)
    
    def test_contact_string(self):
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg_info_provider_" \
            "slurm.py --config=test_configs/red.conf")
        fd = os.popen(path)
        entries = read_ldap(fd)
        self.failUnless(fd.close() == None)

        for entry in entries:
            if 'GlueCE' in entry.objectClass:
                contact_string = entry.glue['CEInfoContactString']
                self.failIf(contact_string == "", "Contact string is missing")
                self.failIf(contact_string.endswith("jobmanager-slurm"), \
                    "Contact string must include the queue.")

def main():
    """
    The main entry point for when slurm_test is run in standalone mode.
    """
    cp = config("test_configs/red.conf")
    stream = streamHandler(cp)
    runTest(cp, TestSlurmDynamic, stream, per_site=False)

if __name__ == '__main__':
    main()

