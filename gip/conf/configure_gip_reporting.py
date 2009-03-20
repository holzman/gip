#!/usr/bin/env python

"""
Configuration script for the dCache information provider
"""

import os
import sys

if 'GIP_LOCATION' not in os.environ:
    if 'VDT_LOCATION' not in os.environ:
        print >> sys.stderr, "GIP_LOCATION and VDT_LOCATION are not set!"
        sys.exit(1)
    os.environ['GIP_LOCATION'] = os.path.expandvars('$VDT_LOCATION/gip')

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import cp_get
from gip_testing import getTestConfig, runCommand


def main():
    cp = getTestConfig("xml")
    results_dir = os.path.expandvars(cp_get(cp, "gip_tests", "results_dir", "$VDT_LOCATION/apache/htdocs/"))

    # check for the existence of the css, images, and includes directories in the results dir
    # if they do not exist, copy the dirs and their contents to the results dir
    source_dir = os.path.expandvars('$GIP_LOCATION/reporting/http')
    css_dir = "%s/css" % results_dir
    images_dir = "%s/images" % results_dir
    includes_dir = "%s/includes" % results_dir
    
    if not os.path.isdir(css_dir): runCommand("cp -r %s/css %s" % (source_dir, css_dir))
    if not os.path.isdir(images_dir): runCommand("cp -r %s/images %s" % (source_dir, images_dir))
    if not os.path.isdir(includes_dir): runCommand("cp -r %s/includes %s" % (source_dir, includes_dir))

if __name__ == '__main__':
    main()

