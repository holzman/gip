#!/usr/bin/env python

import os
import sys
if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip.providers.token_info_provider import main

if __name__ =='__main__':
    main()

