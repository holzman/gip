#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip.providers.software import main as software_main

def main():
    software_main()

if __name__ == '__main__':
    main()
