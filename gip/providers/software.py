#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean
from gip.providers.software import main

if __name__ == '__main__':
    cp = config()
    se_only = cp_getBoolean(cp, "gip", "se_only", False)
    if not se_only:
        main()
