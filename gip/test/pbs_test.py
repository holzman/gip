#!/usr/bin/env python

import os
import sys
import unittest
from sets import Set

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from pbs_common import getVoQueues
from gip_testing import runTest, streamHandler

example_queues = [('osg', 'default'), ('osgedu', 'default'),
    ('mis', 'default'),      ('fmri', 'default'),    ('grase', 'default'),
    ('gridex', 'default'),   ('ligo', 'default'),    ('ivdgl', 'default'),
    ('gadu', 'default'),     ('glow', 'default'),    ('cdf', 'default'),
    ('nanohub', 'default'),  ('sdss', 'default'),    ('gpn', 'default'),
    ('engage', 'default'),   ('dteam', 'default'),   ('ops', 'default'),
    ('cms', 'lcgadmin'),     ('osg', 'lcgadmin'),    ('osgedu', 'lcgadmin'),
    ('mis', 'lcgadmin'),     ('fmri', 'lcgadmin'),   ('grase', 'lcgadmin'),
    ('gridex', 'lcgadmin'),  ('ligo', 'lcgadmin'),   ('ivdgl', 'lcgadmin'),
    ('gadu', 'lcgadmin'),    ('glow', 'lcgadmin'),   ('cdf', 'lcgadmin'),
    ('nanohub', 'lcgadmin'), ('dzero', 'lcgadmin'),  ('sdss', 'lcgadmin'),
    ('gpn', 'lcgadmin'),     ('engage', 'lcgadmin'), ('atlas', 'lcgadmin'),
    ('dteam', 'lcgadmin'),   ('ops', 'lcgadmin'),    ('cms', 'cmsprod'),
    ('atlas', 'atlas'),      ('osg', 'workq'),       ('osgedu', 'workq'),
    ('mis', 'workq'),        ('fmri', 'workq'),      ('grase', 'workq'),
    ('gridex', 'workq'),     ('ligo', 'workq'),      ('ivdgl', 'workq'),
    ('gadu', 'workq'),       ('glow', 'workq'),      ('cdf', 'workq'),
    ('nanohub', 'workq'),    ('sdss', 'workq'),      ('gpn', 'workq'),
    ('engage', 'workq'),     ('dteam', 'workq'),     ('ops', 'workq'),
    ('cms', 'cms'),          ('dzero', 'dzero')]
example_queues = Set(example_queues)

class TestPbsDynamic(unittest.TestCase):

    def test_dynamic_provider(self):
        """
        Checks to make sure that the PBS dynamic provider script runs and exits
        with code 0.

        Does not check for correctness.
        """
        path = os.path.expandvars("$GIP_LOCATION/libexec/osg-info-dynamic-pbs" \
            ".py")
        fd = os.popen(path)
        fd.read()
        self.assertEquals(fd.close(), None)

    def test_vo_queues(self):
        cp = config()
        vo_queues = Set(getVoQueues(cp))
        #print vo_queues
        diff = vo_queues.symmetric_difference(example_queues)
        self.assertEquals(len(diff), 0, msg="The following VO-queues are " \
            "different between the expected and actual: %s" % str(diff))

def main():
    """
    The main entry point for when ce_print is run in standalone mode.
    """
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestPbsDynamic, stream)

if __name__ == '__main__':
    main()
