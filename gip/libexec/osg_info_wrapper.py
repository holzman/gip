#!/usr/bin/env python

"""
osg-info-wrapper: Run the generic information provider
"""

import gip.utils.info_main
import sys
import os
if 'GIP_LOCATION' in os.environ:
    sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))

if __name__ == '__main__':
    gip.utils.info_main.main()

