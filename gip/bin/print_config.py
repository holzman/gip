#!/usr/bin/python

import sys
import os
from pprint import pprint
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config
cp = config()

for section in cp.sections():
    print section
    pprint(cp.items(section))



