#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get, cp_getBoolean
#from gip.providers.dcache import main as dcache_main
from gip.providers.generic_storage import main as generic_main

log = getLogger("GIP.SE")

def main():
    log.info("Using generic storage element.")
    generic_main()

if __name__ == '__main__':
    main()

