#!/usr/bin/env python

import sys
import os


sys.argv = ['epydoc', 'gip_common', 'condor_common', 'dCacheAdmin', 
            'gip_storage', 'gip_testing', 'ldap', 'pbs_common', 'user_input',
            '-v']

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from epydoc.cli import cli
cli()

