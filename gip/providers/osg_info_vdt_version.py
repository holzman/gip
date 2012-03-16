#!/usr/bin/python

import sys, time, os

# Make sure the gip_common libraries are in our path
if 'GIP_LOCATION' in os.environ:
    sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
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
            vdt_version_cmd = os.path.expandvars("$VDT_LOCATION/vdt/bin/") + 'vdt-version --brief'
            vdt_version = runCommand(vdt_version_cmd).readlines()[0].strip()
            if (vdt_version == ""): vdt_version = "OLD_VDT"
            
            # Get the timestamp in the two formats we wanted
            now = time.strftime("%a %b %d %T UTC %Y", time.gmtime())
    
            # Load up the template for GlueLocationLocalID
            # To view its contents, see $VDT_LOCATION/gip/templates/GlueCluster
            template = getTemplate("GlueCluster", "GlueLocationLocalID")
            cluster_id = getClusterID(cp)
            for subClusterId in getSubClusterIDs(cp):
                # Dictionary of data to fill in for GlueLocationLocalID
                info = {'locationId':   'VDT_VERSION',
                        'subClusterId': subClusterId,
                        'clusterId':    cluster_id,
                        'locationName': 'VDT_VERSION',
                        'version':      vdt_version,
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

