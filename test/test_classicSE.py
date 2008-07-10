
import os
import sys
import unittest
import ConfigParser

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean, voList
from gip_testing import runTest, streamHandler
from gip_ldap import read_ldap

se_statuses = ['Production', 'Queueing', 'Closed', 'Draining']
service_statuses = ['OK', 'Warning', 'Critical', 'Unknown', 'Other']

class TestClassicSE(unittest.TestCase):

    def setUp(self):
        cp = ConfigParser.ConfigParser()
        self.cp = cp
        cp.add_section("classic_se")
        cp.set("classic_se", "advertise_se", "True")
        cp.set("classic_se", "host", "red.unl.edu")
        cp.set("classic_se", "port", "2811")
        cp.set("classic_se", "name", "T2_Nebraska_classicSE")
        cp.set("classic_se", "unique_name", "red.unl.edu_se")
        cp.set("classic_se", "default", "/opt/osg/data/$VO")

    def setUpLDAP():

    def test_valid_se_status():

    def test_valid_service_status():

def main():
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestClassicSE, stream, per_site=False)

if __name__ == '__main__':
    main()

