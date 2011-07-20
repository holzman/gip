#!/usr/bin/python

import re
import sys
import os

if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.sge import main

if __name__ == '__main__':
    main()

