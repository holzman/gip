#!/usr/bin/python

import sys
import os
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import configContents, config
cp = config()
configContents(cp)




