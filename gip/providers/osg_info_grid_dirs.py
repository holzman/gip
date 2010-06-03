#!/usr/bin/env python

import os
import sys

# Make sure the gip_common libraries are in our path
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, printTemplate, cp_get
from gip_cluster import getSubClusterIDs, getClusterID

# Retrieve our logger in case of failure
log = getLogger("GIP.grid_dirs")

def main():
    try:
        # Load up the site configuration
        cp = config()
        se_only = cp_getBoolean(cp, "gip", "se_only", False)
        if not se_only:
            # Load up the template for GlueLocationLocalID
            # To view its contents, see $VDT_LOCATION/gip/templates/GlueCluster
            template = getTemplate("GlueCluster", "GlueLocationLocalID")
            cluster_id = getClusterID(cp)
            osg_grid = cp_get(cp, "osg_dirs", "grid_dir", None)
    
            if not osg_grid:
                raise RuntimeError('grid_dir ($OSG_GRID) not defined!')
                
            for subClusterId in getSubClusterIDs(cp):
                # Dictionary of data to fill in for GlueLocationLocalID
                info = {'locationId':   'OSG_GRID',
                        'subClusterId': subClusterId,
                        'clusterId':    cluster_id,
                        'locationName': 'OSG_GRID',
                        'version':      1.0,
                        'path':         osg_grid,
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

