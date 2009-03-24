#!/usr/bin/python

"""
Provide the information related to the PBS batch system.  The general
outline of how this is computed is given here:

https://twiki.grid.iu.edu/twiki/bin/view/InformationServices/GipCeInfo
"""

import os
import sys
import unittest
# Standard GIP imports
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.pbs import main

if __name__ == '__main__':
    main()


