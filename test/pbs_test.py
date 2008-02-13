#!/usr/bin/env python

import unittest
import os

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

if __name__ == '__main__':
    unittest.main()
