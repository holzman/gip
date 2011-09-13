#!/usr/bin/env python

import sys, time, os
import re

# Make sure the gip_common libraries are in our path
if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, printTemplate, cp_getBoolean
from gip_testing import runCommand
from gip_cluster import getSubClusterIDs, getClusterID

# Retrieve our logger in case of failure
log = getLogger("GIP.timestamp")

def main():
    try:
        # Load up the site configuration
        cp = config()
        se_only = cp_getBoolean(cp, "gip", "se_only", False)
        if not se_only and 'VDT_LOCATION' in os.environ:
    
            # get the VDT version
            vdt_version_cmd = os.path.expandvars("$VDT_LOCATION/vdt/bin/") + 'vdt-version --no-wget'
            vdt_version_out = runCommand(vdt_version_cmd).readlines()
    
            gip_re = re.compile('Generic Information Provider\s+(.*?)\s*-.*')
            gip_version = 'UNKNOWN'
            for line in vdt_version_out:
                m = gip_re.match(line)
                if m:
                    gip_version = m.groups()[0]
                    break
    
            gip_version += '; $Revision$'
    
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
        log.exception(e)
        sys.stdout = sys.stderr
        raise

if __name__ == '__main__':
    main()

