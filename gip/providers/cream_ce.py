#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean
from gip.providers.creamCE import main 
from gip_common import getTemplate, getLogger, printTemplate

log = getLogger('GIP.CreamCE')

if __name__ == '__main__':
    cp = config()
    if cp_getBoolean(cp, "cream", "enabled", False) and \
           cp_getBoolean(cp, "gip", "se_only", False):
        main()
