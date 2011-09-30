#!/usr/bin/python

"""
Provide the information related to the Condor batch system.  The general
outline of how this is computed is given here:

https://twiki.grid.iu.edu/twiki/bin/view/InformationServices/GipCeInfo
"""

import os
import sys
import unittest
# Standard GIP imports
if 'GIP_LOCATION' in os.environ:
    sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.condor import main

if __name__ == '__main__':
    main()

