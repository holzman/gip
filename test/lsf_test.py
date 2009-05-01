
import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from gip_testing import runTest, streamHandler

class TestLsfDynamic(unittest.TestCase):

    def test_dynamic_provider(self):
        """
        Checks to make sure that the LSF dynamic provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        os.environ['GIP_TESTING'] = '1'
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg_info_provider_lsf"\
            ".py --config=test_configs/red.conf")
        print path
        fd = os.popen(path)
        fd.read()
        self.assertEquals(fd.close(), None)

def main():
    """
    The main entry point for when pbs_test is run in standalone mode.
    """
    cp = config("test_configs/red.conf")
    stream = streamHandler(cp)
    runTest(cp, TestLsfDynamic, stream, per_site=False)

if __name__ == '__main__':
    main()

