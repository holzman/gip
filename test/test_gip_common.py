#!/usr/bin/env python

import os
import sys
import unittest
import tempfile
import ConfigParser
from sets import Set

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean
from gip_testing import runTest, streamHandler
import gip_testing



class TestGipCommon(unittest.TestCase):

    def test_config(self):
        """
        Make sure that the ConfigParser object can load without errors
        """
        cp = config()

    def test_gip_conf(self):
        """
        Make sure that the $GIP_LOCATION/etc/gip.conf file is read.
        """
        old_gip_location = os.environ['GIP_LOCATION']
        tmpdir = tempfile.mkdtemp()
        try:
            os.environ['GIP_LOCATION'] = tmpdir
            etc_dir = os.path.join(tmpdir, 'etc')
            try:
                os.mkdir(etc_dir)
                cp_orig = ConfigParser.ConfigParser()
                cp_orig.add_section("gip_test")
                cp_orig.set("gip_test", "gip_conf", "True")
                gip_conf = os.path.join(etc_dir, 'gip.conf')
                fp = open(gip_conf, 'w')
                try:
                    cp_orig.write(fp)
                    fp.close()
                    cp = ConfigParser.ConfigParser()
                    cp.read([gip_conf])
                    result = cp_getBoolean(cp, "gip_test", "gip_conf", False)
                    self.failUnless(result, msg="Failed to load $GIP_LOCATION"\
                        "/etc/gip.conf")
                finally:
                    os.unlink(gip_conf)
            finally:
                os.rmdir(etc_dir)
        finally:
            os.rmdir(tmpdir)
            os.environ['GIP_LOCATION'] = old_gip_location

    def test_voList(self):
        """
        Make sure voList does indeed load up the correct VOs.
        """

def main():
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestGipCommon, stream, per_site=False)

if __name__ == '__main__':
    main()

