#!/usr/bin/env python

import os
import sys
import sets
import unittest
import tempfile
import ConfigParser
from sets import Set

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean, voList
from gip_cluster import getOSGVersion, getApplications
from gip_testing import runTest, streamHandler
import gip_testing

fermigrid_vos = sets.Set(['osg', 'cdms', 'lqcd', 'auger', 'i2u2', 'cdf', 'des',
    'dzero', 'nanohub', 'grase', 'cms', 'fermilab', 'astro', 'accelerator',
    'hypercp', 'ktev', 'miniboone', 'minos', 'nova', 'numi', 'mipp', 'patriot',
    'sdss', 'theory', 'fermilab-test', 'accelerator', 'cdms', 'LIGO', 'glow',
    'dosar', 'star', 'geant4', 'mariachi', 'atlas', 'nwicg', 'ops', 'gugrid',
    'gpn', 'compbiogrid', 'engage', 'pragma', 'nysgrid', 'sbgrid', 'cigi',
    'mis', 'fmri', 'gridex', 'vo1'])

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
        cp = ConfigParser.ConfigParser()
        cp.add_section("vo")
        cp.set("vo", "vo_blacklist", "ilc")
        cp.set("vo", "vo_whitelist", "vo1")
        cp.set("vo","user_vo_map", "test_configs/fermigrid-osg-user-vo-map.txt")
        vos = voList(cp)
        vos = sets.Set(vos)
        diff = vos.symmetric_difference(fermigrid_vos)
        self.failIf(diff, msg="Difference between voList output and desired " \
            "output: %s." % ', '.join(diff))

    def test_osg_version(self):
        """
        Test the osg-version function and test for the contents in the software.
        """
        my_version = 'OSG-magic-version'
        cp = config("test_configs/red.conf")
        version = getOSGVersion(cp)
        self.failUnless(version == my_version, msg="Computed OSG version does"\
            " not match the test case's OSG version.")
        found_osg = False
        locations = getApplications(cp)
        for loc in locations:
            if loc['locationId'] == my_version:
                self.failUnless(loc['locationName'] == my_version)
                self.failUnless(loc['version'] == my_version)
                found_osg = True
        if found_osg == False:
            self.fail(msg="OSG version not in software list!")
        

def main():
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestGipCommon, stream, per_site=False)

if __name__ == '__main__':
    main()

