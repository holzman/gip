
import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
from gip_ldap import read_ldap
from gip_testing import runTest, streamHandler
from gip_sets import Set
from slurm_common import getVoQueues

example_queues = \
   [
    ('cms', 'part1'),  ('gridex', 'part1'), ('cms', 'part1'),
    ('atlas', 'part1'),   ('osg', 'part1'),      ('osgedu', 'part1'),
    ('mis', 'part1'),     ('fmri', 'part1'),     ('grase', 'part1'),
    ('gridex', 'part1'),  ('ligo', 'part1'),     ('ivdgl', 'part1'),
    ('gadu', 'part1'),    ('GLOW', 'part1'),     ('cdf', 'part1'),
    ('nanohub', 'part1'), ('sdss', 'part1'),     ('gpn', 'part1'),
    ('engage', 'part1'),  ('cms', 'part1'),        ('dzero', 'part1'),
    ('cms', 'part2'),  ('gridex', 'part2'), ('cms', 'part2'),
    ('atlas', 'part2'),   ('osg', 'part2'),      ('osgedu', 'part2'),
    ('mis', 'part2'),     ('fmri', 'part2'),     ('grase', 'part2'),
    ('gridex', 'part2'),  ('ligo', 'part2'),     ('ivdgl', 'part2'),
    ('gadu', 'part2'),    ('GLOW', 'part2'),     ('cdf', 'part2'),
    ('nanohub', 'part2'), ('sdss', 'part2'),     ('gpn', 'part2'),
    ('engage', 'part2'),  ('cms', 'part2'),        ('dzero', 'part2')
   ]

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

    def test_vo_queues(self):
        os.environ['GIP_TESTING'] = '1'
        cp = config("test_configs/red.conf")
        vo_queues = Set(getVoQueues(cp))
        diff = vo_queues.symmetric_difference(example_queues)
        self.assertEquals(len(diff), 0, msg="The following VO-queues are " \
            "different between the expected and actual: %s" % str(diff))
    
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

