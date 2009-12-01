#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.generic_storage import main as generic_main
from gip_logging import getLogger

log = getLogger("GIP.SE")

def main():
    log.info("Using generic storage element.")
    generic_main()

if __name__ == '__main__':
    main()

