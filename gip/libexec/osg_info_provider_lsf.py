#!/usr/bin/python

import re
import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.lsf import main

if __name__ == '__main__':
    main()

