#!/usr/bin/env python

import os
import sys
import urllib2

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip_common import config
from gip_ldap import read_ldap
from gip_testing import GipValidate

def main():
    """
    The main entry point for when url_validator is run in standalone mode.
    """
    fp = urllib2.urlopen(sys.argv[1])
    gv = GipValidate(read_ldap(fp))
    gv.run()

if __name__ == '__main__':
    main()

