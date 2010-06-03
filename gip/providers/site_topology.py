#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_getBoolean
from gip.providers.site import main as site_main
from gip.providers.cluster import main as cluster_main

def main():
    cp = config()
    se_only = cp_getBoolean(cp, "gip", "se_only", False)
    site_main()
    if not se_only:
        cluster_main()

if __name__ == '__main__':
    main()

