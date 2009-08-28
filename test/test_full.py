
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from gip_testing import runTest, streamHandler, GipValidate

#Add the path with the osg_info_wrapper script:
sys.path.append(os.path.expandvars("$GIP_LOCATION/libexec"))
import osg_info_wrapper

class TestGipFull(unittest.TestCase):

    def test_red(self):
        """
        Test all the output for red.unl.edu.
        """
        cp = config("test_modules/red/config")
        os.environ['GIP_TESTING'] = '1'
        entries = osg_info_wrapper.main(cp, return_entries=True)
        gv = GipValidate(entries)
        gv.run()

def main():
    """
    The main entry point for when the test is in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestGipFull, stream, per_site=False)

if __name__ == '__main__':
    main()

