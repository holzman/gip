#!/usr/bin/env python

import sys, time, os
import re

# Make sure the gip_common libraries are in our path
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, printTemplate
from gip_testing import runCommand
from gip_cluster import getSubClusterIDs, getClusterID

# Retrieve our logger in case of failure
log = getLogger("GIP.timestamp")

def main():
    try:
        # Load up the site configuration
        cp = config()

        # get the VDT version
        vdt_version_cmd = os.path.expandvars("$VDT_LOCATION/vdt/bin/") + 'vdt-version --no-wget'
        vdt_version_out = runCommand(vdt_version_cmd).readlines()

        gip_re = re.compile('Generic Information Provider\s+(.*?)\s*-.*')
        for line in vdt_version_out:
            m = gip_re.match(line)
            if m:
                gip_version = m.groups()[0]
                break

        # Get the timestamp in the two formats we wanted
        now = time.strftime("%a %b %d %T UTC %Y", time.gmtime())

        # Load up the template for GlueLocationLocalID
        # To view its contents, see $VDT_LOCATION/gip/templates/GlueCluster
        template = getTemplate("GlueCluster", "GlueLocationLocalID")
        cluster_id = getClusterID(cp)
        for subClusterId in getSubClusterIDs(cp):
            # Dictionary of data to fill in for GlueLocationLocalID
            info = {'locationId':   'GIP_VERSION',
                    'subClusterId': subClusterId,
                    'clusterId':    cluster_id,
                    'locationName': 'GIP_VERSION',
                    'version':      gip_version,
                    'path':         now,
                   }
    
            # Spit out our template, fill it with the appropriate info.
            printTemplate(template, info)
            
    except Exception, e:
        # Log error, then report it via stderr.
        log.error(e)
        sys.stdout = sys.stderr
        raise

if __name__ == '__main__':
    main()

