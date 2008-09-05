#!/usr/bin/python

import os
import sys
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip_common import config, getTemplate, printTemplate
from gip.dcache.admin import connect_admin
from gip.dcache.space_calculator import calculate_spaces

def main():
    cp = config("$GIP_LOCATION/etc/dcache_storage.conf")
    admin = connect_admin(cp)
    sas, vos = calculate_spaces(cp, admin)
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")
    voTemplate = getTemplate("GlueSE", "GlueVOInfoLocalID")
    for sa in sas:
        printTemplate(saTemplate, sa)
    for vo in vos:
        printTemplate(voTemplate, vo)

if __name__ == '__main__':
    main()

