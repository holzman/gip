#!/usr/bin/env python

from pbs_test import main as pbs_main
from condor_test import main as condor_main
from test_gip_common import main as gip_main
from test_topology import main as topology_main
from osg_info_wrapper_test import main as wrapper_main

def test_run(fcn):
    try:
        fcn()
    except SystemExit, se:
        if se.code != 0:
            raise

def main():
    test_run(pbs_main)
    test_run(condor_main)
    test_run(gip_main)
    test_run(topology_main)
    test_run(wrapper_main)

if __name__ == '__main__':
    main()

