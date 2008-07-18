#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.site import main as site_main
from gip.providers.cluster import main as cluster_main

def main():
    site_main()
    cluster_main()

if __name__ == '__main__':
    main()

