#!/usr/bin/env python

"""
The original python software provider.

This is now obsolete.  To see what will really be used beyond version 1.0,
look at gip/lib/python/gip/providers/software.py
"""

import os
import re
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, getTemplate

log = getLogger("GIP.Software")

def print_Locations(cp):
    app_dir = cp.get("osg_dirs", "app")
    ce_name = cp.get('ce', 'name')
    template = getTemplate("GlueCluster", "GlueLocationLocalID")
    path = "%s/etc/grid3-locations.txt" % app_dir
    if not os.path.exists(path):
        path = '%s/etc/osg-locations.txt' % app_dir
    fp = open(path, 'r')
    for line in fp:
        line = line.strip()
        info = line.split()
        if len(info) != 3 or info[0].startswith('#'):
            continue
        if info[1].startswith('#') or info[1].startswith('$'):
            info[1] = 'UNDEFINED'
        info = {'locationId'   : info[0],
                'subClusterId' : ce_name,
                'clusterId'    : ce_name,
                'locationName' : info[0],
                'version'      : info[1],
                'path'         : info[2]
               }
        print template % info

def main():
    try:
        cp = config()
        print_Locations(cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.exception(e)
        raise

if __name__ == '__main__':
    main()

