#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.cese_bind import main

if __name__ == '__main__':
    main()

